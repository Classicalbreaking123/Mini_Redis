# MiniRedis

Redis-inspired distributed in-memory key-value database built in C++.

## Features

- TCP Client-Server Architecture
- Multithreading
- Mutex Synchronization
- TTL Expiration
- O(1) LRU Cache Eviction
- Snapshot Persistence
- AOF Logging
- AOF Recovery
- Automatic AOF Rewrite
- Primary-Replica Replication
- Replica Resynchronization

## Commands

SET key value
GET key
DEL key

## Architecture

Client -> Primary -> Replica
