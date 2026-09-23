import json
import os
import subprocess
import sys
import tempfile
import unittest
from unittest import mock

from raftlite import Cluster, LEADER, Node, NotLeaderError
from raftlite.runner import build_report, run_scenario

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SCENARIO = os.path.join(REPO_ROOT, "sample", "scenario.json")
IDS = ["n1", "n2", "n3"]


def leader_of(cluster):
    leader = cluster.leader()
    assert leader is not None
    return leader


class ElectionTest(unittest.TestCase):
    def test_election_and_heartbeat(self):
        cluster = Cluster(IDS)
        cluster.step(3)
        leaders = [n for n in cluster.nodes.values() if n.state == LEADER]
        self.assertEqual([n.id for n in leaders], ["n1"])
        self.assertEqual(leaders[0].term, 1)
        cluster.step(3)
        # heartbeats keep followers from starting a new election
        self.assertEqual(cluster.nodes["n1"].term, 1)
        self.assertEqual(len([n for n in cluster.nodes.values() if n.state == LEADER]), 1)

    def test_append_only_on_leader(self):
        cluster = Cluster(IDS)
        cluster.step(3)
        with self.assertRaises(NotLeaderError):
            cluster.nodes["n2"].append("x")
        with self.assertRaises(NotLeaderError):
            cluster.nodes["n3"].append("x")


class ReplicationTest(unittest.TestCase):
    def test_log_replication_and_commit(self):
        cluster = Cluster(IDS)
        cluster.step(3)
        leader = leader_of(cluster)
        leader.append("a")
        cluster.step(1)
        self.assertEqual(leader.committed(), ["a"])
        leader.append("b")
        cluster.step(2)  # replicate b, then propagate leader_commit
        for node in cluster.nodes.values():
            self.assertEqual(node.log, leader.log)
            self.assertEqual(node.committed(), ["a", "b"])

    def test_commit_requires_majority(self):
        cluster = Cluster(IDS)
        cluster.step(3)
        leader = leader_of(cluster)
        leader.append("a")
        # only one follower reachable: 2/3 still a majority, commits
        cluster.set_partition([["n1", "n2"], ["n3"]])
        cluster.step(1)
        self.assertEqual(leader.committed(), ["a"])
        # leader alone: minority, must not commit
        cluster.set_partition([["n1"], ["n2", "n3"]])
        leader.append("b")
        cluster.step(2)
        self.assertEqual(leader.committed(), ["a"])

    def test_conflict_truncation_and_convergence(self):
        cluster = Cluster(IDS)
        cluster.step(3)
        old = leader_of(cluster)
        old.append("c1")
        cluster.step(1)
        # isolate the old leader; its next entry stays uncommitted
        cluster.set_partition([["n1"], ["n2", "n3"]])
        old.append("stale")
        cluster.step(4)  # majority partition elects a new leader
        new = leader_of(cluster)
        self.assertNotEqual(new.id, old.id)
        new.append("c2")
        cluster.step(2)
        self.assertEqual(new.committed(), ["c1", "c2"])
        self.assertEqual(old.committed(), ["c1"])  # minority: no new commits
        cluster.heal()
        cluster.step(3)
        logs = [n.log for n in cluster.nodes.values()]
        for other in logs[1:]:
            # entry-by-entry: index and term identical on every node
            self.assertEqual(logs[0], other)
        commands = [e["command"] for e in logs[0]]
        self.assertNotIn("stale", commands)  # overwritten by the new leader
        self.assertEqual(commands, ["c1", "c2"])


class PersistenceTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.path = os.path.join(self.tmp.name, "n1.json")

    def make_node(self):
        return Node("n1", ["n2", "n3"])

    def test_save_load_roundtrip(self):
        node = self.make_node()
        node.term = 2
        node.voted_for = "n2"
        node.log = [{"term": 1, "command": "a"}, {"term": 2, "command": "b"}]
        node.save(self.path)
        restored = self.make_node()
        restored.load(self.path)
        self.assertEqual(restored.term, 2)
        self.assertEqual(restored.voted_for, "n2")
        self.assertEqual(restored.log, node.log)

    def test_no_double_vote_in_same_term_after_restart(self):
        node = self.make_node()
        term, granted = node.request_vote(2, "n2", 0, 0)
        self.assertTrue(granted)
        node.save(self.path)
        # "restart": a fresh node loading the persisted state
        restored = self.make_node()
        restored.load(self.path)
        # same term, different candidate: must refuse
        term, granted = restored.request_vote(2, "n3", 0, 0)
        self.assertFalse(granted)
        # same term, same candidate: idempotent re-grant
        term, granted = restored.request_vote(2, "n2", 0, 0)
        self.assertTrue(granted)
        # higher term: may vote again
        term, granted = restored.request_vote(3, "n3", 0, 0)
        self.assertTrue(granted)

    def test_crash_after_nth_write_leaves_no_partial_file(self):
        node = self.make_node()
        node.term = 1
        node.save(self.path)
        node.term = 2
        node.save(self.path)
        node.term = 3
        # crash on the 3rd save, while replacing the file
        with mock.patch("os.replace", side_effect=RuntimeError("crash on write 3")):
            with self.assertRaises(RuntimeError):
                node.save(self.path)
        # the last durable state is write #2; the file is never half-written
        restored = self.make_node()
        restored.load(self.path)
        self.assertEqual(restored.term, 2)
        with open(self.path, encoding="utf-8") as fh:
            self.assertEqual(json.load(fh)["term"], 2)
        # crash mid-write of the temp file also leaves the main file intact
        node.term = 4
        with mock.patch("os.fsync", side_effect=RuntimeError("crash mid-write")):
            with self.assertRaises(RuntimeError):
                node.save(self.path)
        restored.load(self.path)
        self.assertEqual(restored.term, 2)


class PartitionSafetyTest(unittest.TestCase):
    def test_minority_partition_produces_no_new_commits(self):
        cluster = Cluster(IDS)
        cluster.step(3)
        old = leader_of(cluster)
        old.append("c1")
        cluster.step(1)
        self.assertEqual(old.committed(), ["c1"])
        cluster.set_partition([["n1"], ["n2", "n3"]])
        old.append("c2")  # old leader still thinks it leads
        cluster.step(5)
        # hard assertion: a minority partition must not commit anything new
        self.assertEqual(old.committed(), ["c1"])
        cluster.heal()
        cluster.step(2)
        self.assertEqual(len([n for n in cluster.nodes.values() if n.state == LEADER]), 1)
        # a fresh command on the new leader overwrites the stale entry
        leader_of(cluster).append("c3")
        cluster.step(3)
        logs = [n.log for n in cluster.nodes.values()]
        for other in logs[1:]:
            self.assertEqual(logs[0], other)
        for node in cluster.nodes.values():
            self.assertNotIn("c2", [e["command"] for e in node.log])


class DeterminismTest(unittest.TestCase):
    def test_same_script_replays_byte_identical(self):
        outputs = []
        for _ in range(2):
            cluster, commands = run_scenario(SCENARIO)
            report = build_report(cluster, commands)
            outputs.append(json.dumps(report, ensure_ascii=False, sort_keys=True))
        self.assertEqual(outputs[0], outputs[1])

    def test_summarize_reports_round_nodes_and_leader(self):
        cluster, _ = run_scenario(SCENARIO)
        summary = cluster.summarize()
        self.assertEqual(summary["round"], 14)
        self.assertEqual(summary["leader"], "n2")
        self.assertEqual(summary["leaders"], ["n2"])
        self.assertEqual(set(summary["nodes"]), set(IDS))
        for nid in IDS:
            info = summary["nodes"][nid]
            self.assertEqual(info["log_len"], 3)
            self.assertEqual(info["commit_index"], 3)
            self.assertIn(info["state"], ("follower", "candidate", "leader"))


class ScenarioAcceptanceTest(unittest.TestCase):
    def test_scenario_end_state(self):
        cluster, commands = run_scenario(SCENARIO)
        report = build_report(cluster, commands)
        self.assertEqual(report["nodes"], 3)
        self.assertEqual(report["commands"], 3)
        self.assertEqual(report["rounds"], 14)
        self.assertEqual(report["committed"], 3)
        self.assertEqual(report["leaders"], 1)
        logs = [n.log for n in cluster.nodes.values()]
        for other in logs[1:]:
            self.assertEqual(logs[0], other)

    def test_cli_text_and_json(self):
        text = subprocess.run(
            [sys.executable, "-m", "raftlite", "run", "sample/scenario.json"],
            cwd=REPO_ROOT, capture_output=True, text=True)
        self.assertEqual(text.returncode, 0, text.stderr)
        self.assertIn("消息往返轮次合计 = 14", text.stdout)
        self.assertIn("最终提交条数（应等于命令条数） = 3", text.stdout)
        self.assertIn("最终 leader 数（应为 1） = 1", text.stdout)
        js = subprocess.run(
            [sys.executable, "-m", "raftlite", "run", "sample/scenario.json", "--json"],
            cwd=REPO_ROOT, capture_output=True, text=True)
        self.assertEqual(js.returncode, 0, js.stderr)
        report = json.loads(js.stdout)
        self.assertEqual(report["rounds"], 14)
        self.assertEqual(report["committed"], 3)
        self.assertEqual(report["leaders"], 1)


if __name__ == "__main__":
    unittest.main()
