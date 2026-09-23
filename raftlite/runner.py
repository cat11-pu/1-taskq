"""Scenario runner: replay a JSON script against a deterministic Cluster."""

import json

from .raft import Cluster, LEADER


def run_scenario(path):
    """Run a scenario file; return (cluster, command_count)."""
    with open(path, encoding="utf-8") as fh:
        spec = json.load(fh)
    cluster = Cluster(spec["nodes"])
    commands = 0
    for step in spec.get("script", []):
        op = step["op"]
        if op == "tick":
            cluster.step(step["rounds"])
        elif op == "append":
            leader = cluster.leader()
            if leader is None:
                raise RuntimeError("append with no leader at round %d" % cluster.round)
            leader.append(step["command"])
            commands += 1
        elif op == "partition":
            cluster.set_partition(step["groups"])
        elif op == "heal":
            cluster.heal()
        else:
            raise ValueError("unknown op: %r" % op)
    return cluster, commands


def build_report(cluster, commands):
    leaders = sorted(n.id for n in cluster.nodes.values() if n.state == LEADER)
    leader = cluster.leader()
    committed = len(leader.committed()) if leader else 0
    return {
        "nodes": len(cluster.nodes),
        "commands": commands,
        "rounds": cluster.round,
        "committed": committed,
        "leaders": len(leaders),
        "summary": cluster.summarize(),
    }


def format_text(report):
    return "\n".join([
        "场景节点数 = %d" % report["nodes"],
        "客户端命令条数 = %d" % report["commands"],
        "消息往返轮次合计 = %d" % report["rounds"],
        "最终提交条数（应等于命令条数） = %d" % report["committed"],
        "最终 leader 数（应为 1） = %d" % report["leaders"],
    ])
