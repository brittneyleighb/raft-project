# Simplified Raft-Based Distributed Key-Value Store

## Overview

This project implements a simplified version of the Raft consensus algorithm in Java. The implementation demonstrates the core principles of distributed consensus through leader election, heartbeat communication, log replication, quorum-based commits, and replicated state machine synchronization.

## Features

- Leader election using RequestVote RPCs
- Randomized election timeouts
- Heartbeat communication (AppendEntries)
- Log replication
- Majority (quorum) commits
- Replicated in-memory key-value store
- Simulated node isolation
- Multithreaded execution using ExecutorService

## Project Structure

```
src/
├── Main.java
├── Node.java
├── LogEntry.java
└── NodeState.java
```

## Requirements

- Java 21 or later

## Compile

```bash
javac *.java
```

## Run

```bash
java Main
```

## Example Output

The program demonstrates:

- Leader election
- Client request processing
- Log replication
- Majority commit
- State machine synchronization across nodes

## Future Improvements

- Persistent storage
- TCP socket communication
- Complete Raft log consistency
- Real-time visualization of protocol activity
- Network latency and partition simulation

## References

- Ongaro, D., & Ousterhout, J. *In Search of an Understandable Consensus Algorithm (Raft)*, 2014.
- Coulouris, G., Dollimore, J., Kindberg, T., & Blair, G. *Distributed Systems: Concepts and Design*, 6th Edition.
=======
# raft-project
A simplified Java implementation of the Raft consensus algorithm featuring leader election, log replication, and a distributed in-memory key-value store.
>>>>>>> 68cf841293000c3e4e6fadba8af62eb935dbfd30
