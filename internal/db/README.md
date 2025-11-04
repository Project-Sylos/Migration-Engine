# Database Package Notes

## Overview

The local database instance in Sylos acts as the central ledger for the migration process. Its exact schema will evolve as the project matures, but it currently serves several key purposes:

* **Configuration Data** – Stores migration settings, service connection info, and other environment-level configuration details.
* **Node Table** – Tracks every file and folder discovered during traversal, including path, identifier, parent identifier, traversal status, and related metadata.
* **Audit Logs** – Contains structured logs of all application events, including configuration changes and mid-migration debug output. These logs are SQL-queryable, making them useful for debugging, scripting, and automation.
* **Performance Metrics** – Records performance snapshots and health statistics for analytical and reporting purposes.

## Why SQLite?

Sylos uses **SQLite** with WAL (Write-Ahead Logging) mode as its local database engine. There are several reasons for this choice:

1. **Open Source and Ubiquitous** – SQLite is free, open-source, and widely supported. It's battle-tested and reliable.
2. **Local-Only Operation** – SQLite runs entirely on the user's machine. It never phones home, which is essential for protecting sensitive migration data.
3. **WAL Mode for Concurrency** – SQLite's WAL mode allows concurrent reads and writes safely, which is essential for the queue system's parallel workers.
4. **Lightweight and Zero-Configuration** – Unlike MongoDB or PostgreSQL, SQLite requires no background daemons, Docker containers, or servers. It's a zero-dependency, file-based database that fits seamlessly into Sylos' self-contained design.
5. **Immediate Durability** – With WAL mode and `PRAGMA synchronous = NORMAL`, SQLite provides a good balance between performance and durability, ensuring data is safely persisted without significant performance overhead.
6. **Simple and Reliable** – SQLite is elegant, fast enough for our use case, and developer-friendly. It provides all the features we need without unnecessary complexity.

---

## Database Operations

The database package provides a simple, direct interface for database operations:

### Core Operations

* **`Write(table, args...)`** – Performs immediate INSERT operations into the specified table. No buffering for node data.
* **`Query(query, args...)`** – Executes SQL queries directly against the database. No special flushing needed.
* **`RegisterTable(def)`** – Registers a table schema and creates the table if it doesn't exist.
* **`Close()`** – Cleanly closes the database connection.

### Log Buffering

While most database operations are immediate, **logs are an exception**. The package includes a lightweight `LogBuffer` that:

* Batches log entries for efficient bulk inserts
* Flushes automatically every N entries or every time interval
* Runs asynchronously in a background goroutine
* Gracefully stops and flushes remaining entries on shutdown

This specialized buffering improves log write performance without adding complexity to the general database layer. The log buffer is managed by the log service and is transparent to other components.
