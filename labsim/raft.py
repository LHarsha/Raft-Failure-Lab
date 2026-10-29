"""Raft node: leader election (§5.2) and log replication (§5.3, §5.4).
No membership changes, no snapshots. Log indexes are 1-based."""

from dataclasses import dataclass

FOLLOWER, CANDIDATE, LEADER = "follower", "candidate", "leader"


@dataclass(frozen=True)
class Entry:
    term: int
    command: object


@dataclass(frozen=True)
class RequestVote:
    term: int
    candidate: int
    last_log_index: int
    last_log_term: int

    def describe(self):
        return f"RequestVote(t{self.term} last={self.last_log_index}/t{self.last_log_term})"


@dataclass(frozen=True)
class RequestVoteReply:
    term: int
    granted: bool

    def describe(self):
        return f"VoteReply(t{self.term} {'yes' if self.granted else 'no'})"


@dataclass(frozen=True)
class AppendEntries:
    term: int
    leader: int
    prev_index: int
    prev_term: int
    entries: tuple
    leader_commit: int

    def describe(self):
        return (f"AppendEntries(t{self.term} prev={self.prev_index}/t{self.prev_term} "
                f"n={len(self.entries)} commit={self.leader_commit})")


@dataclass(frozen=True)
class AppendEntriesReply:
    term: int
    success: bool
    match_index: int   # on success: last index matched; on failure: follower's log length (hint)

    def describe(self):
        return f"AppendReply(t{self.term} {'ok' if self.success else 'fail'} match={self.match_index})"


class RaftNode:
    def __init__(self, nid, peers, sim, net, config):
        self.id = nid
        self.peers = list(peers)
        self.sim = sim
        self.net = net
        self.config = config
        self.alive = True
        # persistent state
        self.current_term = 0
        self.voted_for = None
        self.log = []
        self._reset_volatile()

    def _reset_volatile(self):
        self.role = FOLLOWER
        self.leader_id = None
        self.commit_index = 0
        self.votes = set()
        self.next_index = {}
        self.match_index = {}
        self._election_timer = None
        self._heartbeat_timer = None

    @property
    def majority(self):
        return (len(self.peers) + 1) // 2 + 1

    def last_log_index(self):
        return len(self.log)

    def last_log_term(self):
        return self.log[-1].term if self.log else 0

    def term_at(self, index):
        return self.log[index - 1].term if index > 0 else 0

    # ---- lifecycle -------------------------------------------------------

    def start(self):
        self._reset_election_timer()

    def crash(self):
        self.alive = False
        self.sim.cancel(self._election_timer)
        self.sim.cancel(self._heartbeat_timer)
        self.sim.record("crash", self.id, term=self.current_term)

    def restart(self):
        self._reset_volatile()
        self.alive = True
        self.sim.record("restart", self.id, term=self.current_term, log_len=len(self.log))
        self.start()

    # ---- timers ----------------------------------------------------------

    def _reset_election_timer(self):
        self.sim.cancel(self._election_timer)
        lo, hi = self.config.election_timeout
        self._election_timer = self.sim.schedule(self.sim.rng.randint(lo, hi), self._on_election_timeout)

    def _on_election_timeout(self):
        if not self.alive or self.role == LEADER:
            return
        self.current_term += 1
        self.role = CANDIDATE
        self.voted_for = self.id
        self.leader_id = None
        self.votes = {self.id}
        self.sim.record("became_candidate", self.id, term=self.current_term)
        self._reset_election_timer()
        req = RequestVote(self.current_term, self.id, self.last_log_index(), self.last_log_term())
        for p in self.peers:
            self.net.send(self.id, p, req)
        if len(self.votes) >= self.majority:
            self._become_leader()

    def _on_heartbeat(self):
        if not self.alive or self.role != LEADER:
            return
        self._broadcast_append()
        self._heartbeat_timer = self.sim.schedule(self.config.heartbeat, self._on_heartbeat)

    # ---- role transitions ------------------------------------------------

    def _become_follower(self, term):
        was = self.role
        if term > self.current_term:
            self.current_term = term
            self.voted_for = None
        self.role = FOLLOWER
        if was != FOLLOWER:
            self.sim.cancel(self._heartbeat_timer)
            self.sim.record("became_follower", self.id, term=self.current_term, was=was)
            if was == LEADER:
                self._reset_election_timer()

    def _become_leader(self):
        self.role = LEADER
        self.leader_id = self.id
        self.sim.cancel(self._election_timer)
        self.next_index = {p: self.last_log_index() + 1 for p in self.peers}
        self.match_index = {p: 0 for p in self.peers}
        self.sim.record("became_leader", self.id, term=self.current_term,
                        log=tuple((e.term, e.command) for e in self.log))
        self._on_heartbeat()

    # ---- messages --------------------------------------------------------

    def receive(self, src, msg):
        if not self.alive:
            return
        if msg.term > self.current_term:
            self._become_follower(msg.term)
        if isinstance(msg, RequestVote):
            self._handle_request_vote(src, msg)
        elif isinstance(msg, RequestVoteReply):
            self._handle_vote_reply(src, msg)
        elif isinstance(msg, AppendEntries):
            self._handle_append_entries(src, msg)
        elif isinstance(msg, AppendEntriesReply):
            self._handle_append_reply(src, msg)

    def _log_up_to_date(self, last_index, last_term):
        mine = self.last_log_term()
        return last_term > mine or (last_term == mine and last_index >= self.last_log_index())

    def _handle_request_vote(self, src, m):
        granted = (m.term == self.current_term
                   and self.voted_for in (None, m.candidate)
                   and self._log_up_to_date(m.last_log_index, m.last_log_term))
        if granted:
            self.voted_for = m.candidate
            self.sim.record("vote", self.id, term=m.term, candidate=m.candidate)
            self._reset_election_timer()
        self.net.send(self.id, src, RequestVoteReply(self.current_term, granted))

    def _handle_vote_reply(self, src, m):
        if self.role != CANDIDATE or m.term != self.current_term or not m.granted:
            return
        self.votes.add(src)
        if len(self.votes) >= self.majority:
            self._become_leader()

    # ---- replication -----------------------------------------------------

    def client_submit(self, command):
        """Append a client command if leader. Returns whether it was accepted."""
        if not self.alive or self.role != LEADER:
            return False
        self.log.append(Entry(self.current_term, command))
        index = len(self.log)
        self.sim.record("client_accept", self.id, cmd=command, index=index, term=self.current_term)
        self.sim.record("append", self.id, from_index=index, entries=((self.current_term, command),))
        self._advance_leader_commit()   # single-node cluster commits immediately
        self._broadcast_append()
        return True

    def _broadcast_append(self):
        for p in self.peers:
            self._send_append(p)

    def _send_append(self, peer):
        prev = self.next_index[peer] - 1
        entries = tuple(self.log[prev:prev + self.config.max_batch])
        self.net.send(self.id, peer, AppendEntries(
            self.current_term, self.id, prev, self.term_at(prev), entries, self.commit_index))

    def _handle_append_entries(self, src, m):
        if m.term < self.current_term:
            self.net.send(self.id, src, AppendEntriesReply(self.current_term, False, len(self.log)))
            return
        if self.role != FOLLOWER:
            self._become_follower(m.term)
        self.leader_id = m.leader
        self._reset_election_timer()

        if m.prev_index > len(self.log) or self.term_at(m.prev_index) != m.prev_term:
            self.net.send(self.id, src, AppendEntriesReply(self.current_term, False, len(self.log)))
            return

        appended = []
        for k, entry in enumerate(m.entries):
            index = m.prev_index + 1 + k
            if index <= len(self.log):
                if self.log[index - 1].term == entry.term:
                    continue          # already have it (duplicate or reordered message)
                removed = tuple((e.term, e.command) for e in self.log[index - 1:])
                self.sim.record("truncate", self.id, from_index=index, removed=removed)
                del self.log[index - 1:]
            self.log.append(entry)
            appended.append((index, entry))
        if appended:
            self.sim.record("append", self.id, from_index=appended[0][0],
                            entries=tuple((e.term, e.command) for _, e in appended))

        last_new = m.prev_index + len(m.entries)
        if m.leader_commit > self.commit_index:
            self._commit_up_to(min(m.leader_commit, last_new), m.term)
        self.net.send(self.id, src, AppendEntriesReply(self.current_term, True, last_new))

    def _handle_append_reply(self, src, m):
        if self.role != LEADER or m.term != self.current_term:
            return
        if m.success:
            self.match_index[src] = max(self.match_index[src], m.match_index)
            self.next_index[src] = max(self.next_index[src], m.match_index + 1)
            self._advance_leader_commit()
        else:
            hint = min(self.next_index[src] - 1, m.match_index + 1)
            self.next_index[src] = max(self.match_index[src] + 1, hint, 1)
            self._send_append(src)

    def _advance_leader_commit(self):
        # Only entries from the current term are committed by counting replicas (§5.4.2).
        for n in range(len(self.log), self.commit_index, -1):
            if self.log[n - 1].term != self.current_term:
                break
            if 1 + sum(self.match_index[p] >= n for p in self.peers) >= self.majority:
                self._commit_up_to(n, self.current_term)
                break

    def _commit_up_to(self, index, commit_term):
        for i in range(self.commit_index + 1, index + 1):
            e = self.log[i - 1]
            self.sim.record("commit", self.id, index=i, term=e.term, cmd=e.command, commit_term=commit_term)
        self.commit_index = max(self.commit_index, index)
