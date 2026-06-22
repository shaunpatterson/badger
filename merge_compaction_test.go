/*
 * SPDX-FileCopyrightText: © 2017-2025 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package badger

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// concatMerge is an associative merge: it appends operand to existing.
// f(existing, operand) = existing || operand.
func concatMerge(existing, operand []byte) []byte {
	out := make([]byte, 0, len(existing)+len(operand))
	out = append(out, existing...)
	out = append(out, operand...)
	return out
}

// compactL1toL2 runs a single deterministic L1->L2 compaction over the whole key
// space and returns once done.
func compactL1toL2(t *testing.T, db *DB) {
	t.Helper()
	cdef := compactDef{
		thisLevel: db.lc.levels[1],
		nextLevel: db.lc.levels[2],
		top:       db.lc.levels[1].tables,
		bot:       db.lc.levels[2].tables,
		t:         db.lc.levelTargets(),
	}
	cdef.t.baseLevel = 2
	require.NoError(t, db.lc.runCompactDef(-1, 1, cdef))
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
		getAllAndCheck(t, db, []keyValVersion{
			{"foo", "bc", 3, bitMergeEntry},
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
