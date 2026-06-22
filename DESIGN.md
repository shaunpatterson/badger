# DeleteRange — Non-blocking Range Tombstones

## Motivation

Badger today offers only `DropPrefix` / `DropAll` (db.go ~1768). Both **block all
writes**, flush every memtable, stop compactions, and eagerly rewrite tables.
They have historically had data-loss bugs and are unusable for a live workload
that needs to delete a key range cheaply.

`DeleteRange(begin, end []byte)` records a *range tombstone* that is resolved
**lazily** at read time (and, in a follow-up, at compaction time) — semantics
like RocksDB / Pebble range deletes. It does **not** stall writes.

`DeleteRange` deletes the half-open interval `[begin, end)` (bytewise) of every
key written *before* the tombstone's commit timestamp.

## Representation

A range tombstone is a **normal LSM entry** stored under a **reserved internal
prefix**, so it rides the existing write path with zero new write-stall surface:

```
prefix = !badger!rangedel               // reserved, mirrors !badger!banned
key    = KeyWithTs(prefix || begin, commitTs)
meta   = bitRangeDelete  (1 << 4)       // previously-free meta bit
value  = end                            // the exclusive upper bound
```

Because it is an ordinary entry it:

- flows through `txn.modifyInternal` → commit → `KeyWithTs` → memtable `Put` →
  WAL → L0 → compaction, with **no** `blockWrites`, memtable flush, or
  compaction halt;
- gets a normal MVCC `commitTs` from the oracle;
- is durable in the WAL/SST and **survives restart**.

Storing it under the `!badger!` prefix means it is **already an internal key**:
the existing `parseItem` / `Txn.Get` internal-key handling hides it from user
reads for free (no bespoke per-entry skipping), and — crucially — the index can
be rebuilt at `Open()` by scanning **only that prefix** rather than the whole DB.

`txn.modify` rejects `!badger!`-prefixed keys; `DeleteRange` uses a sibling
`txn.modifyInternal` that omits only that guard, keeping all other bookkeeping.

### In-memory interval index

Reads cannot afford to scan the LSM for covering tombstones, so the DB keeps a
small in-memory index `rangeTombstones` (db.go field). It is:

- an **immutable slice** of `{begin, end, ts}` behind an `atomic.Pointer`, so
  readers load-and-scan with **no lock**; a `DeleteRange` commit builds a new
  slice (copy-on-write) and atomically stores it (writes are rare relative to
  reads);
- **populated at `Open()`** by a low-level merge iterator scoped to
  `rangeDelPrefix` over the memtables + LSM levels — **not** `txn.NewIterator`,
  so it touches **no user-facing read metrics** and (via prefix-scoped
  `pickTables`) opens only the relevant tables. Cost is O(tombstones), not
  O(DB). Mirrors `initBannedNamespaces`'s prefix-only approach.
- **updated on each `DeleteRange` commit** (after the write is acked).

When `DeleteRange` is never used the slice is empty (nil); every read does one
cheap `len()==0` check and is otherwise unchanged → **no behavior change, no
measurable overhead, old DBs unaffected.**

## Visibility rule (MVCC)

A candidate point version `V` of a user key `k`, read at `readTs`, is **hidden**
iff there exists a range tombstone covering `k` with timestamp `T` such that:

```
V < T <= readTs        (and begin <= k < end)
```

We use the **max** such `T` among all covering tombstones. Worked example
(`k` written @100, `DeleteRange` @200, `k` re-written @300):

| readTs | candidate V | covering T | V < T <= readTs? | result      |
|--------|-------------|------------|------------------|-------------|
| 250    | 100         | 200        | yes              | hidden      |
| 350    | 300         | 200        | no (300 ≥ 200)   | visible     |
| 150    | 100         | —          | no T ≤ 150       | visible     |

- Equal ts (`V == T`) → **visible** (we can't order writes within a ts).
- Multiple / overlapping tombstones → take max covering `T`.
- A later `DeleteRange` naturally supersedes an earlier one via the max rule.

## Read path

1. **Point `Get`** (txn.go `Txn.Get` → `db.get`): after the candidate
   `ValueStruct` (version `V`) is fetched, call
   `db.coveredByRangeTombstone(userKey, V, readTs)`. If covered, treat as
   `ErrKeyNotFound`.
2. **Iterator** (iterator.go `parseItem`): for each candidate item
   (`userKey`@`V`), the same covering check; if covered, `Next()` and skip.
   `AllVersions` iterators still apply the check (a covered version is logically
   deleted), unless `InternalAccess` is set.
3. **The tombstone entries themselves are hidden for free.** Because they live
   under the `!badger!rangedel` prefix they are internal keys; the pre-existing
   `parseItem` / `Txn.Get` internal-key filtering already excludes them from
   user reads. No `bitRangeDelete`-specific skipping is needed in the read path.

### No collision with a real key == begin

The tombstone is keyed at `!badger!rangedel || begin`, a different keyspace from
the user key `begin`. They cannot shadow each other in `db.get`/`parseItem`, so
a real key equal to `begin` is returned normally (and then hidden iff the
interval index covers it). The interval index — not entry presence — enforces
range coverage.

## Compaction (DEFERRED — documented, not in MVP)

The MVP keeps tombstones and covered keys on disk forever; correctness is
enforced purely on the read side. Reclamation is a follow-up:

1. **Drop covered point keys.** Feed a range-tombstone iterator into the
   compaction merge. While emitting key `k`@`V`, if a covering `T` exists with
   `V < T` and `T <= discardTs` (the existing version-elision gate), elide `V`
   from the output table.
2. **GC the tombstone.** Keep propagating tombstones downward. Once a tombstone
   has reached the bottom level and `T <= discardTs` (so no reader can still need
   it and every covered key beneath has been rewritten), drop the tombstone entry
   itself. Optionally fragment/merge tombstones at table boundaries for earlier
   GC. This needs a per-table range-tombstone min/max bound (or a dedicated
   meta-block) to avoid scanning, plus index pruning when tombstones are GC'd.

## Backward compatibility

- New meta bit `1<<4` was previously unused; old SSTs never set it. The startup
  scan reads only the `!badger!rangedel` prefix, which old DBs never wrote, so
  it finds nothing and the index stays empty.
- No on-disk format change (the tombstone is a normal entry under a reserved
  prefix, exactly like banned namespaces).
- If `DeleteRange` is never called: the index is empty and every read does a
  single empty-slice check — semantics and performance are unchanged.

## API

- `func (db *DB) DeleteRange(begin, end []byte) error` — convenience wrapper that
  commits a single tombstone entry in its own transaction (non-managed mode).
- `func (txn *Txn) DeleteRange(begin, end []byte) error` — adds the tombstone to
  a txn's pending writes (composes with other ops in the same commit).
- Requires `begin < end`; otherwise `ErrEmptyKey`/`ErrInvalidRange` (no-op-safe).

## Testing (TDD)

- Write keys, `DeleteRange` a sub-range, assert `Get` + forward iterator hide
  exactly the covered keys at the right versions.
- Keys written *after* the tombstone (higher ts) reappear.
- No write stall: writes during/after `DeleteRange` succeed.
- Default behavior unchanged when `DeleteRange` is never called.
- Restart: index rebuilt from disk, coverage still enforced.
