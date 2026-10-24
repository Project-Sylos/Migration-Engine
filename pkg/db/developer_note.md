## Note

Offset/limit pagination that walks from the start of a result set on every page is O(n) in many stores (classic SQL `OFFSET`, naive Mongo skips, etc.). Example: page size 1000, page 10 forces the engine to skip ~9000 already-seen rows before returning the next page. That cost grows with dataset size and is a poor fit for huge inventories with tight page-turn latency.

Prefer **keyset / cursor** pagination (continue after the last seen id, path, or composite key) so each page is O(page size) rather than O(offset). The Migration Engine’s Badger pulls and review walks follow that pattern (`pend:*` keysets, `after` cursors on status/index scans). Keep new list APIs cursor-based unless there is a strong reason not to.

In practice, most engine queries are already bounded. The main place a naive empty-search “show everything, page forever” UI could still hurt is path review over tens of millions of nodes; that is uncommon real usage, but worth remembering when adding browse/search endpoints.
