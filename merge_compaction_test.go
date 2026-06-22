/*
 * SPDX-FileCopyrightText: © 2017-2025 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package badger

import (
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/dgraph-io/badger/v4/options"
	"github.com/dgraph-io/badger/v4/pb"
	"github.com/dgraph-io/badger/v4/table"
	"github.com/dgraph-io/badger/v4/y"
)

// concatMerge is an associative merge: it appends operand to existing.
// f(existing, operand) = existing || operand.
func concatMerge(existing, operand []byte) []byte {
	out := make([]byte, 0, len(existing)+len(operand))
	out = append(out, existing...)
	out = append(out, operand...)
	return out
}

// compactLevel runs a single deterministic from->from+1 compaction over the whole
// key space and returns once done.
func compactLevel(t *testing.T, db *DB, from int) {
	t.Helper()
	cdef := compactDef{
		thisLevel: db.lc.levels[from],
		nextLevel: db.lc.levels[from+1],
		top:       db.lc.levels[from].tables,
		bot:       db.lc.levels[from+1].tables,
		t:         db.lc.levelTargets(),
	}
	cdef.t.baseLevel = from + 1
	require.NoError(t, db.lc.runCompactDef(-1, from, cdef))
}

// compactL1toL2 runs a single deterministic L1->L2 compaction over the whole key
// space and returns once done.
func compactL1toL2(t *testing.T, db *DB) {
	t.Helper()
	compactLevel(t, db, 1)
}

// TestCompactionMergeFold registers an associative operator and verifies that
// operand versions of a key below discardTs are folded into a single value.
func TestCompactionMergeFold(t *testing.T) {
	opt := DefaultOptions("").
		WithNumCompactors(0).
		WithNumVersionsToKeep(1).
		WithCompactionMerge(concatMerge)
	opt.managedTxns = true

	runBadgerTest(t, &opt, func(t *testing.T, db *DB) {
		// Three operands for the same key at versions 1,2,3, all on L1.
		l1 := []keyValVersion{
			{"foo", "c", 3, bitMergeEntry},
			{"foo", "b", 2, bitMergeEntry},
			{"foo", "a", 1, bitMergeEntry},
		}
		createAndOpen(db, l1, 1)

		// All versions are below discardTs => fully foldable.
		db.SetDiscardTs(10)
		compactL1toL2(t, db)

		// Operands are folded oldest->newest into one combined operand at the newest
		// folded version (3). No base value existed, so the result keeps the merge
		// bit (a combined operand).
		getAllAndCheck(t, db, []keyValVersion{
			{"foo", "abc", 3, bitMergeEntry},
		})
	})
}

// TestCompactionMergeFoldOntoBase verifies operands fold onto a complete base value
// and produce a single complete (non-operand) value.
func TestCompactionMergeFoldOntoBase(t *testing.T) {
	opt := DefaultOptions("").
		WithNumCompactors(0).
		WithNumVersionsToKeep(1).
		WithCompactionMerge(concatMerge)
	opt.managedTxns = true

	runBadgerTest(t, &opt, func(t *testing.T, db *DB) {
		l1 := []keyValVersion{
			{"foo", "y", 3, bitMergeEntry}, // operand
			{"foo", "x", 2, bitMergeEntry}, // operand
			{"foo", "base", 1, 0},          // complete base value
		}
		createAndOpen(db, l1, 1)

		db.SetDiscardTs(10)
		compactL1toL2(t, db)

		// base + x + y => "basexy" as a single complete value at the base version (1).
		getAllAndCheck(t, db, []keyValVersion{
			{"foo", "basexy", 1, 0},
		})
	})
}

// TestCompactionMergeRespectsDiscardTs verifies that versions above discardTs are
// NOT folded (snapshot guarantee), while versions at/below are.
func TestCompactionMergeRespectsDiscardTs(t *testing.T) {
	opt := DefaultOptions("").
		WithNumCompactors(0).
		WithNumVersionsToKeep(1).
		WithCompactionMerge(concatMerge)
	opt.managedTxns = true

	runBadgerTest(t, &opt, func(t *testing.T, db *DB) {
		l1 := []keyValVersion{
			{"foo", "d", 4, bitMergeEntry}, // above discardTs => pass through
			{"foo", "c", 3, bitMergeEntry}, // above discardTs => pass through
			{"foo", "b", 2, bitMergeEntry}, // <= discardTs => fold
			{"foo", "a", 1, bitMergeEntry}, // <= discardTs => fold
		}
		createAndOpen(db, l1, 1)

		// Only versions <= 2 may be folded.
		db.SetDiscardTs(2)
		compactL1toL2(t, db)

		getAllAndCheck(t, db, []keyValVersion{
			{"foo", "d", 4, bitMergeEntry},
			{"foo", "c", 3, bitMergeEntry},
			{"foo", "ab", 2, bitMergeEntry}, // a+b folded
		})
	})
}

// TestCompactionMergeDeleteBarrier verifies a delete tombstone stops the fold:
// operands above the delete fold together; nothing folds across the delete.
func TestCompactionMergeDeleteBarrier(t *testing.T) {
	opt := DefaultOptions("").
		WithNumCompactors(0).
		WithNumVersionsToKeep(1).
		WithCompactionMerge(concatMerge)
	opt.managedTxns = true

	runBadgerTest(t, &opt, func(t *testing.T, db *DB) {
		l1 := []keyValVersion{
			{"foo", "y", 4, bitMergeEntry}, // operand above delete
			{"foo", "x", 3, bitMergeEntry}, // operand above delete
			{"foo", "", 2, bitDelete},      // delete barrier
			{"foo", "old", 1, 0},           // base below delete (dropped by delete)
		}
		createAndOpen(db, l1, 1)

		// Keep the delete in place by giving it an overlap with a lower level. Without
		// overlap the delete + everything below is dropped at the bottom-most-ish
		// compaction; with overlap we retain the delete marker. Use a low discardTs so
		// the delete is retained.
		db.SetDiscardTs(10)
		compactL1toL2(t, db)

		// Operands x,y fold into "xy" above the delete; the delete and the base below
		// it are collapsed (delete is the last valid version, base dropped).
		getAllAndCheck(t, db, []keyValVersion{
			{"foo", "xy", 4, bitMergeEntry},
		})
	})
}

// TestCompactionMergeDiscardEarlierBarrier verifies bitDiscardEarlierVersions on an
// operand stops accumulation and drops older versions.
func TestCompactionMergeDiscardEarlierBarrier(t *testing.T) {
	opt := DefaultOptions("").
		WithNumCompactors(0).
		WithNumVersionsToKeep(1).
		WithCompactionMerge(concatMerge)
	opt.managedTxns = true

	runBadgerTest(t, &opt, func(t *testing.T, db *DB) {
		l1 := []keyValVersion{
			{"foo", "c", 3, bitMergeEntry},
			{"foo", "b", 2, bitMergeEntry | bitDiscardEarlierVersions}, // barrier
			{"foo", "a", 1, bitMergeEntry},                             // dropped (older than barrier)
		}
		createAndOpen(db, l1, 1)

		db.SetDiscardTs(10)
		compactL1toL2(t, db)

		// c+b fold; a is discarded by the bitDiscardEarlierVersions barrier on v2.
		// The combined operand MUST retain the discard barrier bit (regression: it
		// was previously dropped, letting a later read/compaction fall through to an
		// older base in a lower level).
		getAllAndCheck(t, db, []keyValVersion{
			{"foo", "bc", 3, bitMergeEntry | bitDiscardEarlierVersions},
		})
	})
}

// TestCompactionMergeAcrossLevels verifies operands that start out in DIFFERENT
// SSTables (here L1 and L2) are correctly folded when those tables are compacted
// together, including operands whose values live in the value log. This drives the
// real runCompactDef -> compactBuildTables -> subcompact path that Flatten also uses.
func TestCompactionMergeAcrossLevels(t *testing.T) {
	opt := DefaultOptions("").
		WithNumCompactors(0).
		WithNumVersionsToKeep(1).
		WithCompactionMerge(concatMerge)
	opt.managedTxns = true

	runBadgerTest(t, &opt, func(t *testing.T, db *DB) {
		// Newer operands on L1, older operands plus the base on L2. After an L1->L2
		// compaction all versions are colocated and fully foldable.
		l1 := []keyValVersion{
			{"foo", "d", 5, bitMergeEntry},
			{"foo", "c", 4, bitMergeEntry},
		}
		l2 := []keyValVersion{
			{"foo", "b", 3, bitMergeEntry},
			{"foo", "a", 2, bitMergeEntry},
			{"foo", "BASE", 1, 0},
		}
		createAndOpen(db, l1, 1)
		createAndOpen(db, l2, 2)

		db.SetDiscardTs(100)
		compactL1toL2(t, db)

		// BASE + a + b + c + d folded into a single complete value at the base ts (1).
		getAllAndCheck(t, db, []keyValVersion{
			{"foo", "BASEabcd", 1, 0},
		})
	})
}

// TestCompactionMergeDiscardBarrierPreservedAcrossLevels is a regression test for a
// bug where the bitDiscardEarlierVersions barrier was dropped when operands folded
// into a combined operand. The barrier masks ALL older versions, including a base in
// a lower level NOT part of the current compaction. If the bit is lost, a later
// compaction (or read) can fall through to that base and fold it in, changing the
// result vs. the un-compacted DB.
func TestCompactionMergeDiscardBarrierPreservedAcrossLevels(t *testing.T) {
	opt := DefaultOptions("").
		WithNumCompactors(0).
		WithNumVersionsToKeep(math.MaxInt32). // keep versions so the base survives until compaction reaches it
		WithCompactionMerge(concatMerge)
	opt.managedTxns = true

	runBadgerTest(t, &opt, func(t *testing.T, db *DB) {
		// Operands on L1; a complete base far below on L3, NOT part of an L1->L2
		// compaction. The v6 operand carries the discard-earlier barrier.
		l1 := []keyValVersion{
			{"foo", "y", 8, bitMergeEntry},                             // > discardTs, passed through
			{"foo", "x", 7, bitMergeEntry},                             // <= discardTs
			{"foo", "d", 6, bitMergeEntry | bitDiscardEarlierVersions}, // <= discardTs, BARRIER
		}
		l3 := []keyValVersion{
			{"foo", "BASE", 3, 0}, // base hidden by the barrier; must never fold in
		}
		createAndOpen(db, l1, 1)
		createAndOpen(db, l3, 3)

		// discardTs = 7: v8 passes through, v7 and v6 fold; barrier at v6 drops all
		// older (the L3 base must be masked).
		db.SetDiscardTs(7)

		// Pre-compaction snapshot of the foldable result a downstream reader would
		// see: fold versions newest-first down to (and including) the v6 barrier,
		// stopping there (never reaching the L3 base).
		preFold := readFoldAllVersions(t, db, []byte("foo"))

		// First compaction: L1 -> L2. Folds x (v7) and d (v6) into one combined
		// operand at v7. The combined operand MUST retain bitDiscardEarlierVersions.
		compactLevel(t, db, 1)

		// Second compaction: L2 -> L3, which now brings the L3 base into the same
		// per-key chain. If the barrier was preserved, the base is masked/dropped; if
		// it was lost, the base would survive and a fold would include it.
		compactLevel(t, db, 2)

		postFold := readFoldAllVersions(t, db, []byte("foo"))
		require.Equal(t, preFold, postFold,
			"folded read result must be identical before and after compaction")

		// Concretely: v8 ("y") passed through, plus the combined operand of x (v7) and
		// d (v6) folded newest-first into "dx" at v7, which still carries the discard
		// barrier; the L3 base is masked and gone.
		getAllAndCheck(t, db, []keyValVersion{
			{"foo", "y", 8, bitMergeEntry},
			{"foo", "dx", 7, bitMergeEntry | bitDiscardEarlierVersions},
		})
	})
}

// TestCompactionMergeTTLOperandBarrier verifies that an operand carrying a TTL acts
// as a barrier (is not folded), so the compacted result matches read-time semantics
// where an older operand reaching its TTL stops the fold. TTL-free operands above it
// still fold together.
func TestCompactionMergeTTLOperandBarrier(t *testing.T) {
	opt := DefaultOptions("").
		WithNumCompactors(0).
		WithNumVersionsToKeep(math.MaxInt32).
		WithCompactionMerge(concatMerge)
	opt.managedTxns = true

	runBadgerTest(t, &opt, func(t *testing.T, db *DB) {
		// v3,v2 are TTL-free operands; v1 carries a (far-future) TTL so it is not yet
		// expired but must NOT be folded across.
		future := uint64(time.Now().Add(time.Hour).Unix())
		// Build a single L1 table holding all three versions, the oldest with a TTL.
		createTableTTL(db, []keyValVersionTTL{
			{"foo", "c", 3, bitMergeEntry, 0},
			{"foo", "b", 2, bitMergeEntry, 0},
			{"foo", "a", 1, bitMergeEntry, future},
		}, 1)

		db.SetDiscardTs(10)
		compactL1toL2(t, db)

		// c+b fold into one combined operand at v3 (no TTL); the TTL operand at v1
		// survives unchanged as a barrier (still folded at read time).
		getAllAndCheckTTL(t, db, []keyValVersionTTL{
			{"foo", "bc", 3, bitMergeEntry, 0},
			{"foo", "a", 1, bitMergeEntry, future},
		})
	})
}

// keyValVersionTTL is keyValVersion plus an ExpiresAt, used to assert TTL handling.
type keyValVersionTTL struct {
	key       string
	val       string
	version   int
	meta      byte
	expiresAt uint64
}

// createTableTTL is like createAndOpen but lets each entry carry an ExpiresAt.
func createTableTTL(db *DB, td []keyValVersionTTL, level int) {
	opts := table.Options{
		BlockSize:          db.opt.BlockSize,
		BloomFalsePositive: db.opt.BloomFalsePositive,
		ChkMode:            options.NoVerification,
	}
	b := table.NewTableBuilder(opts)
	defer b.Close()
	for _, item := range td {
		key := y.KeyWithTs([]byte(item.key), uint64(item.version))
		val := y.ValueStruct{Value: []byte(item.val), Meta: item.meta, ExpiresAt: item.expiresAt}
		b.Add(key, val, 0)
	}
	fname := table.NewFilename(db.lc.reserveFileID(), db.opt.Dir)
	tab, err := table.CreateTable(fname, b)
	if err != nil {
		panic(err)
	}
	if err := db.manifest.addChanges([]*pb.ManifestChange{
		newCreateChange(tab.ID(), level, 0, tab.CompressionType()),
	}, db.opt); err != nil {
		panic(err)
	}
	db.lc.levels[level].Lock()
	db.lc.levels[level].tables = append(db.lc.levels[level].tables, tab)
	db.lc.levels[level].Unlock()
}

// getAllAndCheckTTL is getAllAndCheck that also asserts ExpiresAt.
func getAllAndCheckTTL(t *testing.T, db *DB, expected []keyValVersionTTL) {
	t.Helper()
	require.NoError(t, db.View(func(txn *Txn) error {
		opt := DefaultIteratorOptions
		opt.AllVersions = true
		opt.InternalAccess = true
		it := txn.NewIterator(opt)
		defer it.Close()
		i := 0
		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			v, err := item.ValueCopy(nil)
			require.NoError(t, err)
			require.Less(t, i, len(expected), "DB has more keys than expected")
			e := expected[i]
			require.Equal(t, e.key, string(item.Key()))
			require.Equal(t, e.val, string(v), "key %s", e.key)
			require.Equal(t, e.version, int(item.Version()), "key %s", e.key)
			require.Equal(t, e.meta, item.meta, "key %s", e.key)
			require.Equal(t, e.expiresAt, item.ExpiresAt(), "key %s expiresAt", e.key)
			i++
		}
		require.Equal(t, len(expected), i, "keys examined should equal keys expected")
		return nil
	}))
}

// readFoldAllVersions simulates the read-time associative fold for a single key:
// iterate all versions newest-first, fold with concatMerge, breaking at the first
// deleted/expired version and stopping after a version with bitDiscardEarlierVersions
// (mirrors merge.go iterateAndMerge). It is used to assert that compaction does not
// change the value a downstream reader would compute.
func readFoldAllVersions(t *testing.T, db *DB, key []byte) string {
	t.Helper()
	var folded []byte
	have := false
	require.NoError(t, db.View(func(txn *Txn) error {
		opt := DefaultIteratorOptions
		opt.AllVersions = true
		opt.InternalAccess = true
		it := txn.NewKeyIterator(key, opt)
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			if item.IsDeletedOrExpired() {
				break
			}
			v, err := item.ValueCopy(nil)
			require.NoError(t, err)
			if !have {
				folded = v
				have = true
			} else {
				// item is older than what we have; fold older onto newer.
				folded = concatMerge(v, folded)
			}
			if item.DiscardEarlierVersions() {
				break
			}
		}
		return nil
	}))
	return string(folded)
}

// TestCompactionMergeDisabledNoChange verifies that without an operator registered,
// merge operands are preserved exactly (no behavior change vs. upstream).
func TestCompactionMergeDisabledNoChange(t *testing.T) {
	opt := DefaultOptions("").
		WithNumCompactors(0).
		WithNumVersionsToKeep(1)
	// No WithCompactionMerge.
	opt.managedTxns = true

	runBadgerTest(t, &opt, func(t *testing.T, db *DB) {
		l1 := []keyValVersion{
			{"foo", "c", 3, bitMergeEntry},
			{"foo", "b", 2, bitMergeEntry},
			{"foo", "a", 1, bitMergeEntry},
		}
		createAndOpen(db, l1, 1)

		db.SetDiscardTs(10)
		compactL1toL2(t, db)

		// All operands survive: merge entries are exempt from discard and not folded
		// during compaction when no operator is registered.
		getAllAndCheck(t, db, []keyValVersion{
			{"foo", "c", 3, bitMergeEntry},
			{"foo", "b", 2, bitMergeEntry},
			{"foo", "a", 1, bitMergeEntry},
		})
	})
}
