# MiniRedis

A Redis-inspired **distributed in-memory key-value database** built in **C++** with support for persistence, eviction, replication, and basic fault tolerance.

## Features

- **TCP Client-Server Architecture** for handling client requests over sockets
- **Multithreaded request handling** using `std::thread`
- **Mutex-based synchronization** for safe concurrent access to shared state
- **TTL Expiration** for automatically removing expired keys
- **O(1) LRU Cache Eviction** using a hash map + doubly linked list
- **Snapshot Persistence** to store database state on disk
- **Append-Only File (AOF) Logging** for durable command logging
- **AOF Recovery** to rebuild state after restart
- **Automatic AOF Rewrite** to compact log size
- **Primary-Replica Replication** for high availability
- **Replica Resynchronization** when a node reconnects after failure
- **Heartbeat-based failure detection** for detecting primary failure
- **Automatic failover** where replica can take over as leader
- **Automatic failback** where primary can resynchronize and reclaim leadership

## Supported Commands

```bash
SET key value
GET key
DEL key
```

You can also support TTL in `SET` if your implementation allows:

```bash
SET key value expiry_seconds
```

## System Design Overview

MiniRedis consists of three main components:

### 1. Client
The client connects to the active server and sends commands like `SET`, `GET`, and `DEL`.

### 2. Primary Server
The primary handles client writes, updates the in-memory store, persists changes to disk, and replicates updates to the replica.

### 3. Replica Server
The replica maintains a copy of the primary’s state. If the primary fails, the replica can detect the failure and promote itself to leader.

## Architecture

Normal mode:

```text
Client -> Primary -> Replica
```

Failover mode:

```text
Client -> Replica
```

After recovery:

```text
Primary syncs from Replica -> Replica steps down -> Client reconnects to Primary
```

## Internal Components

### In-Memory Storage
- Main key-value data stored in an `unordered_map<string, string>`
- TTL metadata stored separately
- LRU order maintained using a doubly linked list + hash map for O(1) updates

### Persistence Layer
MiniRedis uses two persistence mechanisms:

#### Snapshot Persistence
Stores the current database state to disk so that the database can be reloaded after restart.

#### Append-Only File (AOF)
Every write operation is appended to a log file. On restart, MiniRedis can replay the log to recover state.

#### AOF Rewrite
To prevent the AOF from growing indefinitely, it is periodically rewritten into a compact form containing only the latest valid state.

## Replication and Fault Tolerance

### Replication
The primary forwards write operations to the replica so the replica maintains an up-to-date copy of the database.

### Failure Detection
The replica periodically expects heartbeats from the primary. If heartbeats stop for a timeout period, the replica assumes the primary has failed.

### Automatic Failover
When the primary fails:
- replica detects failure
- replica promotes itself to leader
- client reconnects to the replica

### Recovery and Resynchronization
When the primary comes back:
- it requests the latest state from the replica
- updates its local database
- restores leadership
- replica steps down
- client reconnects back to primary

## Tech Stack

- **Language:** C++
- **Networking:** POSIX TCP sockets
- **Concurrency:** `std::thread`, `std::mutex`
- **Storage Structures:** `unordered_map`, `list`, `vector`
- **Persistence:** file I/O for snapshots and AOF logs
- **Platform:** Linux / Ubuntu

## Example Workflow

### Start replica
```bash
g++ server/replica_server.cpp -o replica_server -pthread
./replica_server
```

### Start primary
```bash
g++ server/server.cpp -o primary_server -pthread
./primary_server
```

### Start client
```bash
g++ client/client.cpp -o redis_client
./redis_client
```

### Example commands
```bash
SET a 10
SET b 20
GET a
DEL b
```

## Example Fault-Tolerance Flow

1. Start primary and replica
2. Client connects to primary
3. Perform some writes
4. Kill primary
5. Replica detects failure and becomes leader
6. Client reconnects to replica
7. Perform more writes
8. Restart primary
9. Primary synchronizes missed updates from replica
10. Replica steps down and client reconnects to primary

