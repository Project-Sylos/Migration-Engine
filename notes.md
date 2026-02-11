**DuckDB-only architecture recap (execution plan)**

* Remove Bolt entirely:

  * Delete all Bolt DB code and dependencies
  * Engine, API, and tests use DuckDB only

* DuckDB rules:

  * DuckDB is **never** used to decide “what’s next”
  * No status predicates in pagination
  * All hot-path writes are append-only
  * Exactly **one UPDATE per level / round**

* Writes:

  * Use DuckDB appender for:

    * traversal inserts
    * dst inserts
    * workset tables
    * status staging tables
  * Single writer goroutine for DuckDB

* Status updates:

  * Append `(node_id, new_status)` into a **staging table**
  * Do **not** update live tables during a level
  * At level seal:

    * `UPDATE live_table FROM staging_table`
    * commit
    * drop staging table

* Levels:

  * Levels are explicit execution boundaries
  * All DB updates happen only at level seal
  * Crash recovery = redo current level

* Src / Dst gating:

  * Src traversal cannot exceed `dst_level + N`
  * Src pauses when ahead of dst

* Dst task loading:

  * Keyset paginate **dst parents** by immutable ID
  * Join to src by path (or path hash)
  * No `$IN` lists, no status filters
  * For each dst parent:

    * fetch src children by `parent_id` only
    * process and drop immediately (no global cache)

* Memory usage:

  * No global node cache
  * Only per-parent child loading
  * Optional bounded LRU later (non-correctness)

* ETL:

  * Remove all ETL logic entirely
  * DuckDB is the single source of persistence

* Tests:

  * Prefer invariant tests over flow tests
  * Update end-to-end tests **after** scale viability is proven

* Guardrails:

  * Pagination only on immutable identity
  * Status never drives scheduling
  * Fewer commits > faster commits

--

## Do not get caught up in trying to refactor the whole repo at once. Don't worry about this one off DB file and trying to copy its functionality over. Just focus on creating the core invariants and structure we need. Then we can refactor the queue package and other packages to fit the new DB structure. 