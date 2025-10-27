# Database Package Notes

## Overview

The local database instance in Sylos acts as the central ledger for the migration process. Its exact schema will evolve as the project matures, but it currently serves several key purposes:

* **Configuration Data** – Stores migration settings, service connection info, and other environment-level configuration details.
* **Node Table** – Tracks every file and folder discovered during traversal, including path, identifier, parent identifier, traversal status, and related metadata.
* **Audit Logs** – Contains structured logs of all application events, including configuration changes and mid-migration debug output. These logs are SQL-queryable, making them useful for debugging, scripting, and automation.
* **Performance Metrics** – Records performance snapshots and health statistics for analytical and reporting purposes.

## Why DuckDB?

Sylos uses **DuckDB** as its local database engine. There are several reasons for this choice:

1. **Open Source and Actively Maintained** – DuckDB is free, open, and developed by a strong community. Supporting open-source software aligns with Sylos' values.
2. **Local-Only Operation** – DuckDB runs entirely on the user's machine. It never phones home, which is essential for protecting sensitive migration data.
3. **High Performance** – Compared to SQLite, DuckDB consistently offers better performance in analytical workloads and supports much larger `IN` lists (SQLite limits these to 999 items). This allows Sylos to perform bulk operations efficiently.
4. **Lightweight and Native** – Unlike MongoDB or PostgreSQL, DuckDB requires no background daemons, Docker containers, or servers. It's a zero-dependency, native database that fits seamlessly into Sylos' self-contained design.
5. **Columnar Storage Model** – DuckDB's columnar architecture is ideal for Sylos' structured data and performance analytics.
6. **We Genuinely Like It** – It's elegant, fast, and developer-friendly. DuckDB just *feels right* for Sylos.

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
