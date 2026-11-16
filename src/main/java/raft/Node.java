package raft;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

public class Node {

    // Node Identity
    private final int id;

    // Raft State
    private NodeState state;
    private int currentTerm;
    private Integer votedFor;
    private List<Node> peers = new ArrayList<>();

    // Concurrency & Networking
    private final ExecutorService rpcPool = Executors.newCachedThreadPool();

    // Simulated network partition flag
    private volatile boolean networkMuted = false;

    // Election Timer
    private volatile long lastHeartbeatTime;
    private int electionTimeout;

    // Log & State Machine
    private final List<LogEntry> log = new ArrayList<>();
    private final ConcurrentHashMap<String, String> stateMachine = new ConcurrentHashMap<>();

    // Raft Commit Tracking
    private int commitIndex = -1;
    private int lastApplied = -1;

    public Node(int id) {
        this(id, true);
    }

    /**
     * Disable background election timers for deterministic protocol unit tests.
     * Production demos use the public constructor with timers enabled.
     */
    Node(int id, boolean enableElectionTimer) {
        this.id = id;
        this.state = NodeState.FOLLOWER;

        resetElectionTimeout();

        if (enableElectionTimer) {
            startElectionTimer();
        }
    }

    // Randomize the election timeout and restart the timer.
    private void resetElectionTimeout() {

        // Randomize timeout to reduce split votes
        electionTimeout = ThreadLocalRandom.current()
                .nextInt(150, 301);

        lastHeartbeatTime = System.currentTimeMillis();
    }

    public synchronized void setPeers(List<Node> peers) {
        this.peers = new ArrayList<>(peers);
    }

    public synchronized void setNetworkMuted(boolean muted) {
        this.networkMuted = muted;

        if (muted) {
            System.out.println("!!! Node " + id + " has been DISCONNECTED from the network !!!");
        } else {
            resetElectionTimeout();

            System.out.println("+++ Node " + id + " has RECONNECTED to the network +++");
        }
    }

    public synchronized boolean isNetworkMuted() {
        return this.networkMuted;
    }

    public synchronized NodeState getState() { return this.state; }
    public synchronized int getId() { return this.id; }

    private void startElectionTimer() {

        Thread timerThread = new Thread(() -> {
            while (!Thread.currentThread().isInterrupted()) {
                try {
                    Thread.sleep(20); // Small interval sleep to minimize CPU usage

                    long now = System.currentTimeMillis();

                    synchronized (this) {
                        // Muted nodes cannot process time; Leaders do not track election timeouts
                        if (!networkMuted &&
                                state != NodeState.LEADER &&
                                (now - lastHeartbeatTime) >= electionTimeout) {

                            System.out.println("Node " + id +
                                    " timer expired after " + electionTimeout + "ms.");

                            startElection();
                        }
                    }

                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }, "Timer-Node-" + id);

        timerThread.setDaemon(true);
        timerThread.start();
    }

    public synchronized void startElection() {
        if (networkMuted) return; // Disconnected node cannot initiate an election

        currentTerm++;
        state = NodeState.CANDIDATE;
        votedFor = id;

        resetElectionTimeout();

        System.out.println("Node " + id + " started an election for Term " + currentTerm);

        // Atomic counter to track concurrent votes safely across threads
        AtomicInteger voteCount = new AtomicInteger(1);
        int totalNodes = peers.size() + 1;
        int majority = (totalNodes / 2) + 1;

        // Broadcast requests concurrently without blocking the node
        final int termAtRequest = currentTerm;
        // Raft §5.4.1: voters compare the candidate's last log term, then index.
        final int lastLogIndex = log.size() - 1;
        final int lastLogTerm = lastLogIndex >= 0 ? log.get(lastLogIndex).getTerm() : 0;

        for (Node peer : peers) {
            rpcPool.submit(() -> {
                if (peer.requestVote(id, termAtRequest, lastLogIndex, lastLogTerm)) {
                    int votes = voteCount.incrementAndGet();
                    handleVoteGranted(votes, majority, termAtRequest);
                }
            });
        }
    }

    private synchronized void handleVoteGranted(int votes, int majority, int termSent) {
        // Ensure the node hasn't stepped down, changed terms, or been muted since sending the request
        if (!networkMuted && state == NodeState.CANDIDATE && currentTerm == termSent && votes >= majority) {
            state = NodeState.LEADER;
            System.out.println(">>> Node " + id + " wins election and becomes LEADER for Term " + currentTerm + " <<<");
            startHeartbeatBroadcast();
        }
    }

    /**
     * Simplified RequestVote receiver with the two core election-safety guards:
     * one candidate per term, and candidate-log up-to-date ordering (term, index).
     *
     * A complete Raft implementation would persist currentTerm, votedFor and log
     * before replying; this in-memory classroom simulator does not.
     */
    public synchronized boolean requestVote(
            int candidateId,
            int candidateTerm,
            int candidateLastLogIndex,
            int candidateLastLogTerm) {

        if (networkMuted) return false;

        if (candidateTerm < currentTerm) {
            return false;
        }

        if (candidateTerm > currentTerm) {
            // A new term is the ONLY reason to forget the previous term's vote.
            currentTerm = candidateTerm;
            state = NodeState.FOLLOWER;
            votedFor = null;
        }

        int localLastLogIndex = log.size() - 1;
        int localLastLogTerm =
                localLastLogIndex >= 0 ? log.get(localLastLogIndex).getTerm() : 0;
        boolean candidateLogUpToDate =
                candidateLastLogTerm > localLastLogTerm
                || (candidateLastLogTerm == localLastLogTerm
                    && candidateLastLogIndex >= localLastLogIndex);

        if ((votedFor == null || votedFor == candidateId) && candidateLogUpToDate) {
            votedFor = candidateId;
            resetElectionTimeout();
            System.out.println("Node " + id + " voted YES for Node "
                    + candidateId + " in Term " + candidateTerm);
            return true;
        }

        System.out.println("Node " + id + " voted NO for Node "
                + candidateId + " in Term " + candidateTerm
                + (candidateLogUpToDate ? " (already voted)" : " (stale log)"));
        return false;
    }

    /**
     * Simplified AppendEntries RPC.
     * Followers append the leader's log entry.
     */
    public synchronized boolean appendEntry(
            LogEntry entry,
            int leaderTerm,
            int leaderCommit) {

        if (networkMuted) {
            return false;
        }

        // Update term if leader is newer
        if (leaderTerm > currentTerm) {
            currentTerm = leaderTerm;
            state = NodeState.FOLLOWER;
            votedFor = null;
        }

        // Reject stale leaders.
        if (leaderTerm < currentTerm) {
            return false;
        }

        // Accepting a same-term AppendEntries also makes a candidate a follower.
        // Do not clear votedFor except when observing a strictly newer term.
        state = NodeState.FOLLOWER;
        resetElectionTimeout();

        log.add(entry);

        if (leaderCommit > commitIndex) {

            commitIndex = Math.min(leaderCommit, log.size() - 1);

            applyLogToStateMachine();

        }

        System.out.println("Node " + id +
                " replicated log entry: " + entry.getCommand());

        return true;
    }

    private void startHeartbeatBroadcast() {

        Thread heartbeatThread = new Thread(() -> {
            while (!Thread.currentThread().isInterrupted()) {
                synchronized (this) {
                    if (state != NodeState.LEADER) {
                        break;
                    }

                    if (!networkMuted) {
                        resetElectionTimeout();

                        System.out.println("Leader " + id +
                                " sending heartbeats for Term " + currentTerm);

                        for (Node peer : peers) {
                            final int leaderTerm = this.currentTerm;
                            final int leaderCommit = this.commitIndex;

                            rpcPool.submit(() ->
                                    peer.handleHeartbeat(id, leaderTerm, leaderCommit));
                        }
                    }
                }

                try {
                    Thread.sleep(50);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }, "Heartbeat-Node-" + id);

        heartbeatThread.setDaemon(true);
        heartbeatThread.start();
    }

    public synchronized void handleHeartbeat(
            int leaderId,
            int leaderTerm,
            int leaderCommit) {

        System.out.println(
                "Node " + id +
                        " received heartbeat. leaderCommit = " + leaderCommit +
                        ", localCommit = " + commitIndex);

        if (networkMuted) {
            return; // Drop incoming heartbeat silently if isolated
        }

        if (leaderTerm >= this.currentTerm) {

            // A heartbeat is an empty AppendEntries. It must NOT erase a vote
            // in the same term, otherwise this node can vote twice (Raft §5.2).
            if (leaderTerm > this.currentTerm) {
                this.currentTerm = leaderTerm;
                this.votedFor = null;
            }
            this.state = NodeState.FOLLOWER;
            resetElectionTimeout();

            // If the leader has committed further than we have,
            // update our commit index and apply the new entries.
            if (leaderCommit > commitIndex) {

                commitIndex = Math.min(leaderCommit, log.size() - 1);

                applyLogToStateMachine();
            }
        }
    }

    public synchronized boolean executeClientRequest(String command) {

        if (state != NodeState.LEADER || networkMuted) {
            return false;
        }

        LogEntry entry = new LogEntry(currentTerm, command);
        log.add(entry);

        AtomicInteger acknowledgements = new AtomicInteger(1);

        for (Node peer : peers) {

            rpcPool.submit(() -> {

                if (peer.appendEntry(entry, currentTerm, commitIndex)) {

                    acknowledgements.incrementAndGet();

                }

            });

        }

        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        int majority = (peers.size() + 1) / 2 + 1;

        if (acknowledgements.get() >= majority) {

            commitIndex = log.size() - 1;
            applyLogToStateMachine();

            System.out.println("Leader " + id +
                    " committed entry after majority acknowledgement.");

        } else {

            System.out.println("Leader " + id +
                    " failed to reach majority. Entry not committed.");

            return false;
        }

        return true;
    }

    private synchronized void applyLogToStateMachine() {
        while (lastApplied < commitIndex) {
            lastApplied++;
            LogEntry entry = log.get(lastApplied);
            String cmd = entry.getCommand();

            try {
                if (cmd.startsWith("PUT ")) {
                    String kv = cmd.substring(4);
                    String[] parts = kv.split("=", 2);
                    if (parts.length == 2) {
                        stateMachine.put(parts[0].trim(), parts[1].trim());
                        System.out.println("Node " + id + " applied to state machine: " + parts[0].trim() + " -> " + parts[1].trim());
                    }
                }
            } catch (Exception e) {
                System.out.println("Node " + id + " failed parsing log command: " + cmd);
            }
        }
    }

    public synchronized void printStatus() {
        System.out.println("------------");
        System.out.println("Node " + id + " (" + state + ") | Term: " + currentTerm + " | Net: " + (networkMuted ? "DOWN" : "OK"));
        System.out.println("Log Size: " + log.size() + " | CommitIndex: " + commitIndex + " | LastApplied: " + lastApplied);
        System.out.println("State Machine: " + stateMachine);
    }
}
