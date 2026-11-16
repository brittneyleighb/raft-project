package raft;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Deterministic tests of an in-memory Raft teaching simulator's election rules.
 * Not a proof of full Raft safety: log matching, replay, persistence, and
 * crash recovery remain unimplemented.
 */
final class NodeElectionSafetyTest {

    private static Node quietNode(int id) {
        return new Node(id, false); // no background election timer
    }

    @Test
    void sameTermHeartbeatMustNotEnableASecondVote() {
        Node follower = quietNode(1);

        assertTrue(follower.requestVote(2, 5, -1, 0));
        follower.handleHeartbeat(2, 5, -1);

        assertFalse(follower.requestVote(3, 5, -1, 0),
                "One Raft server may not vote for two candidates in a term");
        assertTrue(follower.requestVote(2, 5, -1, 0),
                "Retrying the same candidate's vote remains permissible");
        assertEquals(NodeState.FOLLOWER, follower.getState());
    }

    @Test
    void newerTermClearsPriorTermVoteButRejectsStaleTermRequests() {
        Node follower = quietNode(1);
        assertTrue(follower.requestVote(2, 2, -1, 0));

        assertFalse(follower.requestVote(3, 1, -1, 0),
                "Old terms must not receive a new vote");
        follower.handleHeartbeat(10, 3, -1); // observes newer term
        assertTrue(follower.requestVote(3, 3, -1, 0),
                "A newer term is allowed to select another candidate");
        assertFalse(follower.requestVote(4, 3, -1, 0));
    }

    @Test
    void candidateLogMustBeUpToDateByLastTermBeforeLastIndex() {
        Node follower = quietNode(1);
        assertTrue(follower.appendEntry(new LogEntry(2, "PUT a=1"), 2, -1));
        // More entries cannot compensate for an older *last term*.
        assertFalse(follower.requestVote(2, 3, 999, 1));
        // A candidate with an equally new term and at least the same index wins.
        assertTrue(follower.requestVote(3, 3, 0, 2));
    }

    @Test
    void candidateLogMustBeAtLeastAsLongWhenLastTermsMatch() {
        Node follower = quietNode(1);
        assertTrue(follower.appendEntry(new LogEntry(1, "PUT a=1"), 1, -1));
        assertTrue(follower.appendEntry(new LogEntry(1, "PUT b=2"), 1, -1));

        assertFalse(follower.requestVote(2, 2, 0, 1),
                "Same last term and shorter log must not receive a vote");
        assertTrue(follower.requestVote(3, 2, 1, 1),
                "Same last term and index is sufficiently up-to-date");
    }

    @Test
    void laterLogTermCanOutrankLongerOlderLog() {
        Node follower = quietNode(1);
        assertTrue(follower.appendEntry(new LogEntry(1, "PUT a=1"), 1, -1));
        assertTrue(follower.appendEntry(new LogEntry(1, "PUT b=2"), 1, -1));
        assertTrue(follower.requestVote(2, 3, 0, 2),
                "Later last-log term outranks longer but older log");
    }

    @Test
    void sameTermAppendEntriesStepsDownCandidateWithoutErasingItsVote() {
        Node candidate = quietNode(1);
        candidate.startElection(); // self-vote in term 1
        assertEquals(NodeState.CANDIDATE, candidate.getState());

        candidate.handleHeartbeat(2, 1, -1); // heartbeat is an empty AppendEntries
        assertEquals(NodeState.FOLLOWER, candidate.getState());
        assertFalse(candidate.requestVote(3, 1, -1, 0),
                "Stepping down in the same term cannot undo a cast vote");

        Node other = quietNode(4);
        other.startElection(); // same setup in another node
        assertTrue(other.appendEntry(new LogEntry(1, "PUT a=1"), 1, -1));
        assertEquals(NodeState.FOLLOWER, other.getState());
        assertFalse(other.requestVote(5, 1, 0, 1),
                "Same-term AppendEntries cannot erase the candidate's self-vote");
    }

    @Test
    void staleLeaderMessagesMustNotDemoteCandidate() {
        Node candidate = quietNode(1);
        candidate.startElection(); // term 1
        candidate.handleHeartbeat(2, 0, -1);
        assertEquals(NodeState.CANDIDATE, candidate.getState());
        assertFalse(candidate.appendEntry(new LogEntry(0, "PUT a=1"), 0, -1));
        assertEquals(NodeState.CANDIDATE, candidate.getState());
    }
}
