# Review search: path segments and trigrams

Two different "find this text" problems. They share a write path and a planner, then diverge.

Path search is ordered folder parts, the same idea as the search bar. It is not an open "this substring appears anywhere in the path" scan. Name contains is a substring problem. That is what the trigram index is for.

Exclusion rulesets are a third thing. `pkg/filter` still evaluates path `contains` / `glob` / `regex` as boolean leaves. Search does not. Do not "fix" one to match the other without meaning to.

## Pieces involved

| Piece | Role |
|-------|------|
| `opsdb.NodeRecord` | Catalog row that gets indexed (path, display path, name, id, parent). |
| `opsdb.Store` | Badger ops store. Owns the posting keys and the scans. |
| `review.ReviewFilter` | Query shape. `PathSegments` is the ordered path search. `Query` / `QueryField` and `CompiledFilter` carry name (and other) rules. |
| `review.plannerPlan` | The candidate set the planner chose (`path_seg`, `tri`, `seg`, `name`, `size`, `all`, …) plus a rough size. |
| `filter.Ruleset` | Advanced-filter tree. Path leaves are lifted out of it before compile. Name leaves stay and can become trigram needles. |

Write path: `applyNodeIndexes` on SRC and DST node insert/update/delete. Phase seal only flushes the seal buffer. It does not rebuild these indexes.

Older stores that predate trigrams are repaired once on open (`meta:tri_index_v1`, `EnsureTriIndexV1`). New inserts already write both indexes inline.

SRC and DST are separate posting spaces. The side is part of the key.

## Shared idea: posting lists

Both indexes are inverted lists in Badger, not a scanned catalog column.

A key is a token, then a NUL, then the node id. The value is a placeholder. A query seeks the token prefix and reads ids. There is no per-node lookup on that seek.

```
idx:seg:{side}:{token}\x00{id}
idx:tri:{side}:{gram}\x00{id}
```

`idx:path`, `idx:name`, `idx:size`, and `idx:mtime` sit beside these. The planner tries the cheap precise one first and only falls through to trigrams or a capped full scan when those miss.

## Path segments (`idx:seg`)

**What is stored.** Each whole path part and the node name, lowercased, once. A node at `/Reports/2024/Q1/summary.txt` posts `summary.txt`, `Reports`, `2024`, and `Q1` (the name and the last path part often collapse to one token). Tokens do not cross `/`. A query for `rep` can prefix-seek a token `reports`. A query that only sits in the middle of a name (`eport`) does not hit this index.

**What a search asks for.** An ordered list of parts on `ReviewFilter.PathSegments`. The search bar fills that list directly. Advanced filters do not keep path rules inside the boolean tree: `splitSearchPathSegments` walks the ruleset, appends every non-negated path leaf's values in document order, and strips those leaves before compile. A negated path leaf is dropped, not inverted. AND versus OR does not apply to path leaves after that lift. They are always required, in that order.

**How a hit is found.**

1. The planner takes the first part and seeks `idx:seg` for it (`ScanSegToken`). A miss falls back to the name-prefix index.
2. Each hit is an anchor. The planner expands the anchor's subtree so descendants are candidates too, not only the folder whose name matched.
3. Later parts are not another index intersect. `pathSegmentsMatchAncestry` walks parent links from the root down to the node. Each part must be a case-insensitive substring of a later node's name. Unmatched nodes in between are allowed (gaps). One node consumes at most one part, so `Reports` and `Q1` need two names along the chain, not one folder called `ReportsQ1`.
4. Order is required. `Reports` then `Q1` matches `/Reports/2024/Q1/file.txt`. It does not match `/Q1/Reports/...`.

If the first part cannot use the segment index, the planner may still use a trigram plan (when a name-contains needle exists) or a capped full scan, then the same ancestry check throws out non-matches.

## Trigrams (`idx:tri`)

**What is stored.** Overlapping 3-character slices of the node name and of each path part, lowercased, unique. Slices do not cross `/`, so a gram never straddles a folder boundary. Strings shorter than 3 characters are padded with spaces on the edges so they still have one gram. Display path is used when it is set, otherwise the storage path. For **files** only, the stored `Extension` is stripped from each token before grams are taken, and only when that token actually ends with `.`+Extension (so `ILOVEYOU.txt.vbs` with Extension `vbs` indexes `ILOVEYOU.txt`, not a bare last-dot guess). Folders never strip. Name / segment / display path indexes keep the full strings; this strip is trigram-only. Query needles still drop a trailing extension heuristically so a search for `file.txt` can hit grams indexed as `file`.

**What a search asks for.** Search-bar name/path text, path-segment parts, and ruleset name `contains` / `eq` leaves. Every such needle is planned; candidate ids are the **union of ranked** trigram postings (score = how many needle grams hit). Wildcards are skipped. A needle with no grams is skipped. Verify keeps substring hits, small edit-distance typos, or any shared trigram; results are ordered by relevance score then the UI sort column. Compiled ruleset `contains` still requires a real substring after that.

**How a hit is found.**

1. Split the needle into the same kind of grams the index stores.
2. Load each gram's posting list and **score** ids by overlap count (partial match). Higher scores first; lists are capped then truncated to the planner limit.
3. Soft-verify each survivor (contains, edit distance, or shared grams). Exact `contains` in an advanced filter still runs as a boolean after candidates are chosen.

Trigrams are a weighted partial-match index for the search bar, not only an exact-substring accelerator. A needle of length 1 or 2 still works because of the padding, but it is a wide posting list and a weak plan compared with a longer needle.

## Planner order

`planReviewCandidateIDs` picks one candidate source, then the rest of the filter (status, size, extension, the ancestry check, the trigram verify, and so on) runs in memory on those ids.

Roughly, first match wins:

1. An explicit under-path subtree.
2. A size range, when one was given.
3. A whole-token or prefix name via `idx:seg` or `idx:name`.
4. Path-segment anchors (first part, then subtree expand).
5. Trigram ranked partial match, if a text needle exists.
6. A capped full path scan.

So a query that is both "name contains file" and "path parts Reports / Q1" prefers the path-segment anchor when the first part hits `idx:seg`. The name check still runs on those candidates. Trigrams are the fallback when the precise indexes miss, not the default for every text query.

## What this is not

- Not a scan of file contents.
- Not an open path-contains search. That shape is not how the catalog search indexes work.
- Not rebuilt at phase seal. If a node is inserted, its postings are written then. If a node is renamed or deleted, those postings are rewritten or removed with it. GPL rename updates `Name` and `DisplayPath` (and descendant display paths under a renamed folder) so `idx:tri` / name tokens stay consistent; `Path` / `idx:path` stay on stable id_path.
- Not two-way sync. SRC and DST are indexed separately because both trees are browsable, not because search copies both ways.
