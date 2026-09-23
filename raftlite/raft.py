"""raftlite: minimal Raft consensus, pure standard library.

Deterministic message-passing simulation: leader election, heartbeats,
log replication with commit, and crash-safe persistence.
"""

import json
import os

FOLLOWER = "follower"
CANDIDATE = "candidate"
LEADER = "leader"


class NotLeaderError(Exception):
    """Raised when a client command is appended on a non-leader node."""


class Node:
    """A single Raft node. Persistent state: term, voted_for, log."""

    def __init__(self, node_id, peers, election_timeout=3, storage_path=None):
        self.id = node_id
        self.peers = list(peers)
        self.election_timeout = election_timeout
        self.storage_path = storage_path
        # persistent state (saved by save()/load())
        self.term = 0
        self.voted_for = None
        self.log = []  # list of {"term": int, "command": str}
        # volatile state
        self.state = FOLLOWER
        self.commit_index = 0
        self.leader_id = None
        self.votes = set()
        self.since_heartbeat = 0
        self.next_index = {}   # peer -> next log index to send (1-based)
        self.match_index = {}  # peer -> highest replicated log index
        self.outbox = []

    # ---- persistence -------------------------------------------------

    def save(self, path):
        """Persist term/voted_for/log atomically: temp file + os.replace."""
        data = {"term": self.term, "voted_for": self.voted_for, "log": self.log}
        tmp = path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as fh:
            json.dump(data, fh)
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(tmp, path)

    def load(self, path):
        """Restore persistent state previously written by save()."""
        with open(path, encoding="utf-8") as fh:
            data = json.load(fh)
        self.term = data["term"]
        self.voted_for = data["voted_for"]
        self.log = [dict(entry) for entry in data["log"]]

    def _persist(self):
        if self.storage_path:
            self.save(self.storage_path)

    # ---- client API --------------------------------------------------

    def append(self, command):
        """Append a client command. Only valid on the leader."""
        if self.state != LEADER:
            raise NotLeaderError("%s is %s, not leader" % (self.id, self.state))
        self.log.append({"term": self.term, "command": command})
        self._persist()
        return len(self.log)

    def committed(self):
        """Return the list of committed commands, in log order."""
        return [entry["command"] for entry in self.log[: self.commit_index]]

    # ---- RPC handlers (public, also used by the Cluster transport) ---

    def request_vote(self, term, candidate_id, last_log_index, last_log_term):
        """Handle a RequestVote RPC. Returns (current_term, vote_granted)."""
        if term < self.term:
            return self.term, False
        if term > self.term:
            self._become_follower(term, None)
        up_to_date = (last_log_term, last_log_index) >= (
            self._last_log_term(),
            len(self.log),
        )
        if self.voted_for in (None, candidate_id) and up_to_date:
            self.voted_for = candidate_id
            self.since_heartbeat = 0
            self._persist()
            return self.term, True
        return self.term, False

    def append_entries(self, term, leader_id, prev_log_index, prev_log_term,
                       entries, leader_commit):
        """Handle an AppendEntries RPC. Returns (current_term, success, match_index)."""
        if term < self.term:
            return self.term, False, len(self.log)
        self._become_follower(term, leader_id)
        if prev_log_index > 0:
            if len(self.log) < prev_log_index or \
                    self.log[prev_log_index - 1]["term"] != prev_log_term:
                return self.term, False, len(self.log)
        index = prev_log_index
        changed = False
        for entry in entries:
            index += 1
            if len(self.log) >= index and self.log[index - 1]["term"] != entry["term"]:
                self.log = self.log[: index - 1]  # truncate conflicting suffix
                changed = True
            if len(self.log) < index:
                self.log.append(dict(entry))
                changed = True
        if changed:
            self._persist()
        if leader_commit > self.commit_index:
            self.commit_index = min(leader_commit, len(self.log))
        return self.term, True, prev_log_index + len(entries)

    # ---- timers ------------------------------------------------------

    def tick(self):
        """Advance one round: leaders heartbeat, others may start election."""
        if self.state == LEADER:
            for peer in self.peers:
                self._send(self._append_msg(peer))
            return
        self.since_heartbeat += 1
        if self.since_heartbeat >= self.election_timeout:
            self._start_election()

    # ---- message dispatch (used by Cluster) --------------------------

    def handle(self, msg):
        """Handle one inbound message; return a list of outbound messages."""
        kind = msg["type"]
        if kind == "request_vote":
            term, granted = self.request_vote(
                msg["term"], msg["candidate"],
                msg["last_log_index"], msg["last_log_term"])
            return [{"type": "vote_response", "from": self.id, "to": msg["from"],
                     "term": term, "granted": granted}]
        if kind == "vote_response":
            return self._on_vote_response(msg)
        if kind == "append_entries":
            term, ok, match = self.append_entries(
                msg["term"], msg["leader"], msg["prev_log_index"],
                msg["prev_log_term"], msg["entries"], msg["leader_commit"])
            return [{"type": "append_response", "from": self.id, "to": msg["from"],
                     "term": term, "success": ok, "match_index": match}]
        if kind == "append_response":
            return self._on_append_response(msg)
        raise ValueError("unknown message type: %r" % kind)

    # ---- internals ---------------------------------------------------

    def _last_log_term(self):
        return self.log[-1]["term"] if self.log else 0

    def _send(self, msg):
        self.outbox.append(msg)

    def _start_election(self):
        self.state = CANDIDATE
        self.term += 1
        self.voted_for = self.id
        self.votes = {self.id}
        self.leader_id = None
        self.since_heartbeat = 0
        self._persist()
        for peer in self.peers:
            self._send({"type": "request_vote", "from": self.id, "to": peer,
                        "term": self.term, "candidate": self.id,
                        "last_log_index": len(self.log),
                        "last_log_term": self._last_log_term()})

    def _become_follower(self, term, leader_id):
        if term > self.term:
            self.term = term
            self.voted_for = None
            self._persist()
        self.state = FOLLOWER
        self.leader_id = leader_id
        self.since_heartbeat = 0
        self.votes = set()

    def _become_leader(self):
        self.state = LEADER
        self.leader_id = self.id
        self.next_index = {peer: len(self.log) + 1 for peer in self.peers}
        self.match_index = {peer: 0 for peer in self.peers}

    def _on_vote_response(self, msg):
        if msg["term"] > self.term:
            self._become_follower(msg["term"], None)
            return []
        if self.state == CANDIDATE and msg["term"] == self.term and msg["granted"]:
            self.votes.add(msg["from"])
            if len(self.votes) > (len(self.peers) + 1) // 2:
                self._become_leader()
                return [self._append_msg(peer) for peer in self.peers]
        return []

    def _on_append_response(self, msg):
        if msg["term"] > self.term:
            self._become_follower(msg["term"], None)
            return []
        if self.state != LEADER or msg["term"] != self.term:
            return []
        peer = msg["from"]
        if msg["success"]:
            self.match_index[peer] = msg["match_index"]
            self.next_index[peer] = msg["match_index"] + 1
            self._advance_commit()
            return []
        self.next_index[peer] = max(1, self.next_index.get(peer, len(self.log) + 1) - 1)
        return [self._append_msg(peer)]

    def _advance_commit(self):
        for index in range(len(self.log), self.commit_index, -1):
            if self.log[index - 1]["term"] != self.term:
                continue
            replicated = 1 + sum(
                1 for peer in self.peers if self.match_index.get(peer, 0) >= index)
            if replicated > (len(self.peers) + 1) // 2:
                self.commit_index = index
                break

    def _append_msg(self, peer):
        next_i = self.next_index.get(peer, len(self.log) + 1)
        prev = next_i - 1
        prev_term = self.log[prev - 1]["term"] if prev > 0 else 0
        return {"type": "append_entries", "from": self.id, "to": peer,
                "term": self.term, "leader": self.id,
                "prev_log_index": prev, "prev_log_term": prev_term,
                "entries": [dict(e) for e in self.log[next_i - 1:]],
                "leader_commit": self.commit_index}


class Cluster:
    """A deterministic raft cluster: fixed-round message delivery, no randomness."""

    def __init__(self, node_ids, base_timeout=3, storage_dir=None):
        ids = list(node_ids)
        self.nodes = {}
        for i, nid in enumerate(ids):
            path = os.path.join(storage_dir, nid + ".json") if storage_dir else None
            self.nodes[nid] = Node(
                nid, [x for x in ids if x != nid],
                election_timeout=base_timeout + i, storage_path=path)
        self.round = 0
        self.blocked = set()  # (src, dst) pairs dropped by the network

    def step(self, rounds=1):
        """Advance the cluster by the given number of message rounds."""
        for _ in range(rounds):
            self.round += 1
            for nid in sorted(self.nodes):
                self.nodes[nid].tick()
            self._route()

    def set_partition(self, groups):
        """Block all traffic between different groups (list of node-id lists)."""
        self.blocked = set()
        for group in groups:
            for other in groups:
                if group is other:
                    continue
                for src in group:
                    for dst in other:
                        self.blocked.add((src, dst))

    def heal(self):
        self.blocked.clear()

    def leader(self):
        """Return the leader with the highest term, or None."""
        leaders = [n for n in self.nodes.values() if n.state == LEADER]
        if not leaders:
            return None
        return max(leaders, key=lambda n: (n.term, n.id))

    def summarize(self):
        """Structured snapshot: round, per-node term/state/log length, leader."""
        leader = self.leader()
        return {
            "round": self.round,
            "leader": leader.id if leader else None,
            "leaders": sorted(n.id for n in self.nodes.values() if n.state == LEADER),
            "nodes": {
                nid: {
                    "term": node.term,
                    "state": node.state,
                    "log_len": len(node.log),
                    "commit_index": node.commit_index,
                }
                for nid, node in sorted(self.nodes.items())
            },
        }

    def _route(self):
        queue = []
        for nid in sorted(self.nodes):
            node = self.nodes[nid]
            queue.extend(node.outbox)
            node.outbox = []
        while queue:
            msg = queue.pop(0)
            if (msg["from"], msg["to"]) in self.blocked:
                continue
            queue.extend(self.nodes[msg["to"]].handle(msg))
