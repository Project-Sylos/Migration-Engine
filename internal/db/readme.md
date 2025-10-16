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

## Buffer System

The database package includes a flexible **unified buffer system** that batches operations for maximum efficiency.

### Architecture

* **`Buffer`** – The main manager that coordinates multiple sub-buffers with shared timer and threshold behavior.
* **`SubBuffer`** – Individual buffered channels, each with a custom flush function for specific operation types.
* **`FlushFunc`** – User-defined function that processes accumulated items: `func(items []interface{}) error`.

### Features

* **Multi-channel support** – Register multiple sub-buffers for different operation types (e.g., "completed", "updates", "inserts").
* **Threshold-based flushing** – Automatically flushes when total items across all sub-buffers reach the configured threshold.
* **Timer-based flushing** – Periodic automatic flushing based on configurable intervals.
* **Independent design** – The buffer is not owned by the DB or any specific component; each consumer creates and manages its own buffer instance.
* **Thread-safe** – All operations are properly synchronized with mutexes.

### Usage Pattern

Components like queues and loggers create their own `Buffer` instance, register sub-buffers for different operations, and simply call `buffer.Add(subBufferName, item)`. The buffer handles all batching, timing, and flushing automatically.

This design keeps batch operations efficient while maintaining clean separation of concerns.
