# Filter rules (v1)

Composable exclusion predicates over the **discovered** SRC inventory. Rules do not steer BFS discovery. A bound ruleset is optional; applying exclusions is an explicit path-review action (search / preview / exclude matching).

See also: [item_statuses.md](./item_statuses.md), [algorithms.md](./algorithms.md), [search_indexes.md](./search_indexes.md), [pkg/db/README.md](../pkg/db/README.md). Search path rules are not boolean path contains; they are lifted into ordered path segments.

Prep-time root include uses `src_nodes.include_only` allowlists and omits unchecked siblings (not counted in excluded stats). Local FS adapters also omit `.DS_Store` and `Thumbs.db` at `ListChildren` (never enter DuckDB).

## Phase and wiring

1. Traversal discovers the full SRC tree (display paths stamped at seal time).
2. In awaiting-traversal-review, the user searches with optional flat conditions and/or a ruleset predicate.
3. Preview uses the same SQL as apply. Apply is `POST .../exclude` with `{ search: <SearchRequest>, except: [] }` (flat conditions and/or ruleset). Unexclude-by-search uses the same body on `POST .../unexclude`.
4. Nothing auto-applies at `SetRoot` or traversal start.

Implementation: `pkg/filter/` (`Compile`, `EvaluateDetermining`), `pkg/db/filterapply/` (bulk exclude mutation), Sylos-API exclude-by-search.

## Ruleset schema (v1)

Unchanged structurally: `Ruleset` → `root_group` (`AND`/`OR`, optional `negate`, children as conditions or nested groups). Conditions: `id`, `field`, `operator`, `value`, `negate`, `applies_to`, optional `case_sensitive` (name/path string ops; default false = fold case).

### Supported fields

| Field | Notes |
|-------|--------|
| `size` | Bytes (files) |
| `mtime` | `TRY_CAST` in SQL; unparseable node mtime does not match |
| `name` / `path` | Basename / `display_path` |
| `extension` / `mimetype_category` | Derived from name; category CASE generated from `extensionCategory` |
| `depth` | Under migration root |
| `is_empty` / `child_count` | Via child aggregate / join over sealed children |
| `review_status` | Path-review **search only** (copy / traversal status). Rejected for exclusion apply / evaluate |
| `path_issue_status` / `path_issue_category` | Path-review **search only** (compatibility). Same rejection |

### Operators

Comparison: `gt`, `lt`, `gte`, `lte`, `eq`, `neq`, `in`.

Time: `older_than`, `newer_than`, `before`, `after`.

Strings: `glob`, `regex`, `contains`.

## Evaluation model

Rulesets are **exclusion** predicates: when the root group **Passes**, the node matches (would be / is excluded on apply). Compile with `filter.Compile` and evaluate in Go (`EvaluateDetermining`). Three-valued leaves (pass / fail / skip) combine with AND/OR/negate the same way the former Go evaluator did.

`POST /api/rulesets/evaluate` seeds an ephemeral DuckDB and runs the annotation SELECT so browse / examples / preview share one compiler with apply.

## display_path

`path` conditions use write-once **display_path** (root-relative names). Set by `queue.StampDisplayPath` during discovery; not updated on rename.

## Status effects on apply

Apply writes sealed `copy_status` and drops or restores `pend:copy` immediately (like manual subtree exclude):

| Match kind | `copy_status` | Provenance |
|------------|---------------|------------|
| Explicit match | `excluded_explicit` | `exclusion_source` = filter application id; `determining_rule_id` = first matching leaf |
| Descendant of matched folder | `excluded_inherited` | Same application id |

Traversal status is unchanged. Folders stay discovered.

## Provenance

- `filter_applications`: criteria JSON, applied_at, matched/excluded counts
- `src_status_events` / `src_current`: `exclusion_source`, `determining_rule_id`
- Manual excludes use `exclusion_source = manual`

Path review enriches nodes with `ruleExclusion` from these columns (not `rule_evaluation_events`).

## Manual exclude vs filter exclude vs root pick

| Source | How to tell |
|--------|-------------|
| Root pick skip | Omitted row / `include_only`; or counted `traversal_status = excluded` without filter source |
| Filter apply | `excluded_*` + non-manual `exclusion_source` |
| Manual review | `excluded_*` + `exclusion_source = manual` (or empty) |

## Local junk omit

Sylos-FS local `ListChildren` never returns exact basenames `.DS_Store` and `Thumbs.db` (case-insensitive). Broader junk (`*.tmp`, Office locks, `desktop.ini`) remains optional user-authored starter searches.
