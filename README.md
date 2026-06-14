# redis-server

![Java](https://img.shields.io/badge/Java-17-007396?logo=openjdk&logoColor=white)
![Build](https://img.shields.io/badge/build-Maven-C71A36?logo=apachemaven&logoColor=white)

An in-memory data store built from scratch in Java that implements the Redis wire protocol — no Redis libraries, just the JDK and raw sockets. It speaks RESP, supports a wide range of data types and commands, and handles pub/sub, transactions, persistence, and replication. Sustains **8k+ requests/sec under 100 concurrent clients**.

## Features

- **RESP protocol** — full parsing and serialization of the Redis wire protocol over raw TCP sockets
- **Data types** — strings, lists, streams, sorted sets, and geospatial commands
- **Pub/Sub** — `SUBSCRIBE` / `PUBLISH` messaging
- **Transactions** — `MULTI`, `EXEC`, `DISCARD`, and `WATCH` (optimistic locking)
- **Persistence** — append-only file (AOF) logging with replay on startup
- **Replication** — master–replica command propagation
- **Access control** — ACL command support
- **Concurrency** — handles 100+ concurrent clients via a multithreaded server

## Tech stack

Java · TCP Sockets · Multithreading · Maven

## Getting started

### Prerequisites
- JDK 17 or later
- Maven

### Build and run
```sh
mvn -B package
./your_program.sh
```

The server listens on port `6379` (the Redis default), so you can connect with the standard `redis-cli`:

```sh
$ redis-cli PING
PONG

$ redis-cli SET name brinda
OK

$ redis-cli GET name
"brinda"
```

Transactions, pub/sub, and sorted sets work through the same client:

```sh
$ redis-cli
> MULTI
OK
> SET counter 1
QUEUED
> INCR counter
QUEUED
> EXEC
1) OK
2) (integer) 2
```

## How it works

```
TCP socket → RESP parser → command dispatch → keyspace + AOF → replication
```

1. A **multithreaded TCP server** accepts client connections and serves each concurrently.
2. The **RESP parser** decodes incoming commands from the Redis wire protocol.
3. The **command dispatcher** routes each command to its handler.
4. Handlers read and mutate the **in-memory keyspace**, which holds the supported data structures.
5. Write commands are appended to the **AOF log** and replayed on startup to restore state.
6. When configured as a master, commands are **propagated to connected replicas**.

## Project structure

```
src/main/java/    # server implementation (entry point: Main.java)
pom.xml           # Maven build
```
