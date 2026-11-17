# Raft-Inspired Distributed Key-Value Store (Java)

An educational **three-node, in-memory consensus simulator** written in Java. It demonstrates leader elections, heartbeats, asynchronous simulated RPCs, and replicated `PUT` commands.

> **Important:** This project is **not a complete or production-safe implementation of Raft**. Its current replication and commit paths omit crucial protocol rules and may behave incorrectly during failures or concurrent leader changes. The project is a learning lab, not a service for real data.

## Requirements

- **JDK 21+**
- **Apache Maven 3.9+**, for tests and packaged builds
- No external network service, database, or Docker required to run the demo

## Build and run

From the repository root:

```bash
mvn --batch-mode --no-transfer-progress verify
java -cp target/classes raft.Main
```

The earlier instructions (`javac *.java` and `java Main`) did not match the actual `src/main/java/raft/` package layout.

To compile without Maven (demo only; does not run JUnit tests):

```bash
mkdir -p out
javac -d out src/main/java/raft/*.java
java -cp out raft.Main
```

The demo launches three nodes, waits briefly for an election, sends a sample `PUT user=alice` to whichever leader it observes, then prints the local log and state-machine snapshots. Election timing is randomized, so results may vary.

## Scope and accuracy

| Concept | Current implementation | Limits |
| --- | --- | --- |
| Leader election | Candidates increment terms and request votes from peers | In-memory terms; not persisted on stable storage |
| **Vote safety** | Tracks one vote per term, including across same-term heartbeats | Tests cover this local rule; not a complete election-safety proof |
| **Up-to-date candidate check** | Compares last-log **term first**, then **index** when requesting and granting votes | Logs themselves lack complete Raft consistency guarantees |
| Heartbeat | Sends a simulated leader message periodically | Not a full `AppendEntries` RPC with previous-index/term validation |
| Replication | Append one command to followers | Does **not** maintain `nextIndex`/`matchIndex`, validate predecessor entries, deduplicate retries, or repair conflicting logs |
| Commit | Uses a fixed 100 ms wait, then counts acknowledgements | A race-prone approximation, **not Raft's safe commit rule**; no authoritative replication-index or term validation |
| State machine | Parses `PUT key=value` commands and applies committed local entries | No robust command validation, client deduplication, snapshots, or crash recovery |
| Isolation | Manually blocks simulated requests for one node | Does not model real network partitions, retransmission, or transport failure |
| Storage | Memory only | A restart forgets terms, votes, log entries, and data |

A green unit-test build demonstrates that the listed **local election rules** hold for tested scenarios; it does **not** demonstrate linearizability, durable consensus, or safe recovery.

## Tests

Run:

```bash
mvn test
```

`src/test/java/raft/NodeElectionSafetyTest.java` uses nodes with election timers disabled to avoid timing-dependent outcomes. It checks:

- A heartbeat in the **same term** does not clear a vote or permit voting for a second candidate.
- A new term can permit a new vote; requests from older terms are rejected.
- Candidate-log freshness uses **last term before last index**, and index breaks ties.
- Same-term leader messages step down candidates without retroactively changing their self-votes.
- Stale leader messages do not step down a candidate.

GitHub Actions checks the Maven build and tests on JDK 21 and 25.

## What needs to happen before calling this Raft

1. Model true `AppendEntries(term, prevLogIndex, prevLogTerm, entries[], leaderCommit)` semantics, log matching and conflict repair; track per-peer `nextIndex` and `matchIndex`.
2. Replace the 100 ms sleep-based quorum approximation with correct replication acknowledgements, leader-term validation, safe commitment (including the current-term rule), and explicit responses to clients.
3. Persist `currentTerm`, `votedFor`, and log entries *before* acknowledging safety-relevant RPCs; implement restart recovery.
4. Introduce deterministic simulated network failures, delays and partitions, plus property and integration tests for leadership, committed-entry preservation and log convergence.
5. Add graceful thread/executor shutdown and network transport only after the protocol state machine is testable.

## Read and compare

- Ongaro and Ousterhout, *In Search of an Understandable Consensus Algorithm* (USENIX ATC 2014), especially **Figure 2**, `5.2, and `5.4.1: [official paper](https://raft.github.io/raft.pdf).
- [The Raft project](https://raft.github.io/) collects the original papers, visualizations, and further formal material.
- Coulouris et al., *Distributed Systems: Concepts and Design* (course textbook).

This is retained as a learning project: the intentional simplifications are now documented so they are not confused with protocol correctness.
