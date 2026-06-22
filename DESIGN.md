# Feature #3 — Compaction-time associative merge operator

## Problem

Badger today provides only a *read-time* `MergeOperator` (`merge.go`): a background
goroutine periodically does a read-modify-write of a **single fixed key**, folding all
its versions with a user `MergeFunc(existing, new) []byte`. Systems like dgraph
(posting-list deltas) want a *general* associative merge that folds the many values
written for a key into one **as part of the compaction Badger already runs**, removing
a separate read-fold-rewrite layer.

## Goal

A registered associative operator `merge(existing, operand) -> value`, applied during
LSM compaction in the per-key version loop, so that operand versions of a key
(`e.g.` posting deltas, marked with a meta bit) are folded into a single complete
value. Default behavior is unchanged when no operator is registered.

## Operator interface & registration (DB option)

```go
// In options.go
type Options struct {
    ...
    // CompactionMerge, if non-nil, folds operand versions of a key during
    // compaction. It must be ASSOCIATIVE: applying operands left-to-right in
    // version order (oldest first) must equal folding them pairwise.
    //   value = CompactionMerge(existing, operand)
    // where `existing` is the older accumulated value and `operand` the newer
    // operand being folded in.
    CompactionMerge MergeFunc
}

func (opt Options) WithCompactionMerge(f MergeFunc) Options
```

`MergeFunc` is the existing `func(existingVal, newVal []byte) []byte` type from
`merge.go`, reused for consistency.

### Marking operands: the meta bit

We mirror dgraph's `BitDeltaPosting`. Badger already has `bitMergeEntry` (`1<<3`,
`value.go`) which marks "merge operand; do not discard via compaction" — exactly the
delta marker we need. We **reuse `bitMergeEntry`** as the operand marker:

- An entry with `bitMergeEntry` set is an **operand/delta**, not a complete value.
- Written today via `MergeOperator.Add()` → `Entry.withMergeBit()`. For the
  compaction-merge feature we also expose `Entry.WithMergeOperand()` (public) so
  callers can write operands directly into normal transactions.

No new on-disk bit is introduced → forward/backward compatible. When
`CompactionMerge == nil`, `bitMergeEntry` keeps its current meaning (exempt from
discard, folded only at read time) and **compaction is byte-for-byte unchanged**.

## Where the fold happens

In `levels.go`, `subcompact` → `addKeys`, the per-key version loop (~lines 700-819).
The merging iterator yields versions **newest-first**, and Badger guarantees **all
versions of a key live in one output SSTable** (the builder refuses to split a key —
`levels.go:730`). So a single forward pass over one key sees its whole version chain.

### The MVCC wall: only fold versions `<= discardTs`

`discardTs = orc.discardAtOrBelow()` is the snapshot wall: no running transaction can
observe a version below it as an intermediate. We must **never fold across a version
`> discardTs`**, or we'd destroy a version a live reader might need. Versions
`> discardTs` are always passed through unchanged. Folding only ever touches the tail
of the chain at or below `discardTs`. Since iteration is newest-first, by the time we
reach `<= discardTs` entries we are at the tail.

### Fold direction

Read-time operator does `newVal = f(oldVal, newVal)` while iterating newest-first.
We do the same: maintain one accumulator `acc` (the merged-so-far *newer* side). For
each older operand encountered, `acc = CompactionMerge(older, acc)`. When we reach a
complete base value `<= discardTs`, `out = CompactionMerge(base, acc)` and emit one
complete entry at the base's version. If no base is found, emit a single combined
**operand** (keeps `bitMergeEntry`) at the newest folded version.

### Per-key accumulator FSM (added when `CompactionMerge != nil`)

State reset on key change: `acc []byte`, `haveAcc bool`, `accTs uint64`,
`accExp uint64` (min non-zero ExpiresAt of folded operands).

For each version (newest-first), before the existing discard logic:

- `version > discardTs` → pass through unchanged (no fold). (Operands above the wall
  keep surviving exactly as today.)
- operand (`bitMergeEntry`) and `version <= discardTs`:
  - materialize bytes (inline, or read from vlog if `bitValuePointer`);
  - `acc = haveAcc ? CompactionMerge(op, acc) : op`; track `accTs`, `accExp`;
  - **consume** — do not emit, do not count toward `NumVersionsToKeep`.
- `bitDelete` tombstone (`<= discardTs`): **barrier**. Flush pending `acc` as one
  combined operand above the tombstone, then let the existing delete/discard logic run.
- complete base value (`<= discardTs`):
  - if `haveAcc`: `out = CompactionMerge(base, acc)`; emit one complete entry (clear
    `bitMergeEntry`, keep base Version & ExpiresAt & UserMeta); clear `acc`;
  - else: emit base unchanged (existing path).
- `bitDiscardEarlierVersions`: treated as a barrier — fold into/emit, then `skipKey`
  drops everything older (existing logic already sets `skipKey` on this).

On **key change / end**: if `haveAcc`, emit one combined operand
(`bitMergeEntry` set — plus `bitDiscardEarlierVersions` if any folded operand carried
it — Version=`accTs`, no TTL since only TTL-free operands are folded).

### Interactions

- **bitDelete**: barrier; never fold across it. Pending operands above it are emitted
  as a single combined operand; the delete is preserved/handled by existing logic.
- **bitDiscardEarlierVersions**: barrier; fold up to and including it, emit, then
  older versions are dropped (`skipKey`) — matches the bit's meaning. **The emitted
  combined operand MUST carry this bit forward** (tracked via `mergeAccDiscard`).
  Dropping it would let a later compaction/read fall through to an older base in a
  lower level that the barrier is supposed to mask, diverging from the un-compacted
  DB. (Regression-tested.)
- **expiry (ExpiresAt)**: only TTL-free operands (`ExpiresAt == 0`) are folded. An
  operand with a TTL is a **barrier** — TTL-free operands above it are emitted as one
  combined operand, and the TTL operand passes through unchanged. This matches
  read-time `iterateAndMerge`, which breaks at the first expired older version (so a
  read at time `t` after an older operand's TTL but before a newer one's returns only
  the operands above the expiry). Synthesizing a single TTL for the combined operand
  (min/max of constituents) would silently diverge, so we refuse to fold across any
  TTL. Already-expired operands and expired bases are likewise barriers. A merged base
  keeps the base's own TTL.
- **NumVersionsToKeep**: consumed operands do **not** count; only emitted entries
  (merged base, combined operand, deletes, pass-throughs) count.

### Value-pointer gotcha

Operand/base bytes may be in the value log (`bitValuePointer`). We materialize lazily,
only for entries we are going to fold, via `vlog.Read(vp)`. Entries passed through
unchanged are never materialized (their `vs.Value` is already the vptr the builder
expects). When emitting a merged value we clear `bitValuePointer` and write inline
(merged deltas are small; MVP keeps it inline rather than re-spilling to vlog).

## MVP scope (this iteration)

1. Option + `WithCompactionMerge`, `Entry.WithMergeOperand()` (public operand writer).
2. The per-key fold in `addKeys` gated entirely on `opts.CompactionMerge != nil`.
3. Inline operands fully supported; vlog-pointer operands materialized via `vlog.Read`.
4. Barriers for `bitDelete` and `bitDiscardEarlierVersions`; `<= discardTs` wall.

## Out of scope / TODO (listed honestly in the report)

- Re-spilling a large merged value back to the value log (MVP writes merged inline).
- Exhaustive expiry/NumVersionsToKeep>1 corner cases beyond the tested ones.
- Sub-compaction key-range boundary proofs (relies on existing "all versions in one
  table" guarantee; operands of a key are colocated like any other versions).

## Backward compatibility

`CompactionMerge == nil` → the new branch is never entered; compaction output is
byte-identical to upstream. No new meta bit, no format change.
