/*
 * SPDX-FileCopyrightText: © 2017-2025 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package badger

import (
	"bytes"
	"fmt"
	"os"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"
)

// collectKeys returns all user keys visible via a forward iterator (keys only).
func collectKeys(t *testing.T, db *DB) []string {
	t.Helper()
	var keys []string
	err := db.View(func(txn *Txn) error {
		opt := DefaultIteratorOptions
		opt.PrefetchValues = false
		it := txn.NewIterator(opt)
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			keys = append(keys, string(it.Item().KeyCopy(nil)))
		}
		return nil
	})
	require.NoError(t, err)
	sort.Strings(keys)
	return keys
}

func mustGet(t *testing.T, db *DB, key string) ([]byte, error) {
	t.Helper()
	var val []byte
	err := db.View(func(txn *Txn) error {
		item, err := txn.Get([]byte(key))
		if err != nil {
			return err
		}
		val, err = item.ValueCopy(nil)
		return err
	})
	return val, err
}

// TestDeleteRangeBasic: writing keys, deleting a sub-range, asserting Get and
// the iterator hide exactly the covered keys.
func TestDeleteRangeBasic(t *testing.T) {
	runBadgerTest(t, nil, func(t *testing.T, db *DB) {
		for i := 0; i < 10; i++ {
			txnSet(t, db, []byte(fmt.Sprintf("key%02d", i)), []byte("v"), 0x00)
		}

		// Delete [key03, key07): key03..key06 inclusive.
		require.NoError(t, db.DeleteRange([]byte("key03"), []byte("key07")))

		// Get: covered keys are gone, boundary key07 survives, begin key03 gone.
		for i := 0; i < 10; i++ {
			k := fmt.Sprintf("key%02d", i)
			_, err := mustGet(t, db, k)
			if i >= 3 && i < 7 {
				require.ErrorIs(t, err, ErrKeyNotFound, "expected %s deleted", k)
			} else {
				require.NoError(t, err, "expected %s present", k)
			}
		}

		// Iterator: exactly the uncovered keys remain.
		want := []string{"key00", "key01", "key02", "key07", "key08", "key09"}
		require.Equal(t, want, collectKeys(t, db))
	})
}

// TestDeleteRangeHidesTombstoneEntry: the tombstone entry keyed at `begin` must
// not surface as a user key, even when no real key exists at begin.
func TestDeleteRangeHidesTombstoneEntry(t *testing.T) {
	runBadgerTest(t, nil, func(t *testing.T, db *DB) {
		txnSet(t, db, []byte("z"), []byte("v"), 0x00)
		require.NoError(t, db.DeleteRange([]byte("a"), []byte("c")))

		// "a" (the tombstone's begin) must not appear and must not be gettable.
		_, err := mustGet(t, db, "a")
		require.ErrorIs(t, err, ErrKeyNotFound)
		require.Equal(t, []string{"z"}, collectKeys(t, db))
	})
}

// TestDeleteRangeRealKeyAtBegin: a real user key equal to `begin` written before
// the tombstone is hidden; rewritten after, it reappears.
func TestDeleteRangeRealKeyAtBegin(t *testing.T) {
	runBadgerTest(t, nil, func(t *testing.T, db *DB) {
		txnSet(t, db, []byte("b"), []byte("old"), 0x00)
		require.NoError(t, db.DeleteRange([]byte("b"), []byte("d")))

		_, err := mustGet(t, db, "b")
		require.ErrorIs(t, err, ErrKeyNotFound)

		// Rewrite "b" at a higher ts -> must reappear with the new value.
		txnSet(t, db, []byte("b"), []byte("new"), 0x00)
		val, err := mustGet(t, db, "b")
		require.NoError(t, err)
		require.Equal(t, []byte("new"), val)
	})
}

// TestDeleteRangeWritesAfterReappear: keys written after the range tombstone
// (higher ts) within the deleted range reappear.
func TestDeleteRangeWritesAfterReappear(t *testing.T) {
	runBadgerTest(t, nil, func(t *testing.T, db *DB) {
		txnSet(t, db, []byte("m1"), []byte("old"), 0x00)
		txnSet(t, db, []byte("m2"), []byte("old"), 0x00)

		require.NoError(t, db.DeleteRange([]byte("m"), []byte("n")))

		// Both gone now.
		_, err := mustGet(t, db, "m1")
		require.ErrorIs(t, err, ErrKeyNotFound)

		// Rewrite m1 after the tombstone.
		txnSet(t, db, []byte("m1"), []byte("new"), 0x00)

		val, err := mustGet(t, db, "m1")
		require.NoError(t, err)
		require.Equal(t, []byte("new"), val)

		// m2 was not rewritten -> still hidden.
		_, err = mustGet(t, db, "m2")
		require.ErrorIs(t, err, ErrKeyNotFound)

		require.Equal(t, []string{"m1"}, collectKeys(t, db))
	})
}

// TestDeleteRangeNoWriteStall: writes during/after DeleteRange succeed and are
// not blocked.
func TestDeleteRangeNoWriteStall(t *testing.T) {
	runBadgerTest(t, nil, func(t *testing.T, db *DB) {
		require.Equal(t, int32(0), db.blockWrites.Load())
		require.NoError(t, db.DeleteRange([]byte("a"), []byte("z")))
		// blockWrites must not have been raised by DeleteRange.
		require.Equal(t, int32(0), db.blockWrites.Load())

		// Writes still work right after.
		txnSet(t, db, []byte("zz"), []byte("v"), 0x00)
		val, err := mustGet(t, db, "zz")
		require.NoError(t, err)
		require.Equal(t, []byte("v"), val)
	})
}

// TestDeleteRangeDefaultUnchanged: when DeleteRange is never called, behavior is
// identical (index empty, all keys visible).
func TestDeleteRangeDefaultUnchanged(t *testing.T) {
	runBadgerTest(t, nil, func(t *testing.T, db *DB) {
		for i := 0; i < 5; i++ {
			txnSet(t, db, []byte(fmt.Sprintf("k%d", i)), []byte("v"), 0x00)
		}
		want := []string{"k0", "k1", "k2", "k3", "k4"}
		require.Equal(t, want, collectKeys(t, db))
		for _, k := range want {
			_, err := mustGet(t, db, k)
			require.NoError(t, err)
		}
	})
}

// TestDeleteRangeMVCC: at a readTs between the write and the tombstone the key
// is visible; after the tombstone it is hidden (managed mode for explicit ts).
func TestDeleteRangeMVCC(t *testing.T) {
	opt := getTestOptions("")
	opt.managedTxns = true
	runBadgerTest(t, &opt, func(t *testing.T, db *DB) {
		// Write key@100.
		wb := db.NewWriteBatchAt(100)
		require.NoError(t, wb.Set([]byte("k"), []byte("v")))
		require.NoError(t, wb.Flush())

		// DeleteRange [k, l)@200.
		require.NoError(t, db.DeleteRangeAt([]byte("k"), []byte("l"), 200))

		// Read at ts=150 (before tombstone) -> visible.
		txn150 := db.NewTransactionAt(150, false)
		item, err := txn150.Get([]byte("k"))
		require.NoError(t, err)
		val, err := item.ValueCopy(nil)
		require.NoError(t, err)
		require.Equal(t, []byte("v"), val)
		txn150.Discard()

		// Read at ts=250 (after tombstone) -> hidden.
		txn250 := db.NewTransactionAt(250, false)
		_, err = txn250.Get([]byte("k"))
		require.ErrorIs(t, err, ErrKeyNotFound)
		txn250.Discard()

		// Re-write key@300 -> reappears at ts=350.
		wb2 := db.NewWriteBatchAt(300)
		require.NoError(t, wb2.Set([]byte("k"), []byte("v2")))
		require.NoError(t, wb2.Flush())

		txn350 := db.NewTransactionAt(350, false)
		item, err = txn350.Get([]byte("k"))
		require.NoError(t, err)
		val, err = item.ValueCopy(nil)
		require.NoError(t, err)
		require.Equal(t, []byte("v2"), val)
		txn350.Discard()
	})
}

// TestDeleteRangeRestart: the in-memory index is rebuilt from disk at Open(),
// so coverage is still enforced after a restart.
func TestDeleteRangeRestart(t *testing.T) {
	dir, err := os.MkdirTemp("", "badger-test")
	require.NoError(t, err)
	defer removeDir(dir)

	opt := getTestOptions(dir)
	db, err := Open(opt)
	require.NoError(t, err)

	txnSet(t, db, []byte("r1"), []byte("v"), 0x00)
	txnSet(t, db, []byte("r2"), []byte("v"), 0x00)
	txnSet(t, db, []byte("r3"), []byte("v"), 0x00)
	require.NoError(t, db.DeleteRange([]byte("r1"), []byte("r3")))
	require.NoError(t, db.Close())

	// Reopen: index must be rebuilt from the persisted tombstone entry.
	db2, err := Open(opt)
	require.NoError(t, err)
	defer func() { require.NoError(t, db2.Close()) }()

	_, err = mustGet(t, db2, "r1")
	require.ErrorIs(t, err, ErrKeyNotFound)
	_, err = mustGet(t, db2, "r2")
	require.ErrorIs(t, err, ErrKeyNotFound)
	val, err := mustGet(t, db2, "r3")
	require.NoError(t, err)
	require.Equal(t, []byte("v"), val)
}

// TestDeleteRangeReverseIterator: reverse iteration also hides covered keys.
func TestDeleteRangeReverseIterator(t *testing.T) {
	runBadgerTest(t, nil, func(t *testing.T, db *DB) {
		for i := 0; i < 10; i++ {
			txnSet(t, db, []byte(fmt.Sprintf("key%02d", i)), []byte("v"), 0x00)
		}
		require.NoError(t, db.DeleteRange([]byte("key03"), []byte("key07")))

		var keys []string
		err := db.View(func(txn *Txn) error {
			opt := DefaultIteratorOptions
			opt.Reverse = true
			opt.PrefetchValues = false
			it := txn.NewIterator(opt)
			defer it.Close()
			for it.Rewind(); it.Valid(); it.Next() {
				keys = append(keys, string(it.Item().KeyCopy(nil)))
			}
			return nil
		})
		require.NoError(t, err)
		// Reverse order, covered keys removed.
		require.Equal(t, []string{"key09", "key08", "key07", "key02", "key01", "key00"}, keys)
	})
}

// TestDeleteRangeImmediateVisibility: once DeleteRange returns, a brand-new
// transaction (readTs >= the tombstone's commitTs) must already see the covered
// keys as hidden — with NO sleep. This guards the ordering of index publication
// vs. commit visibility (the index must be published before doneCommit advances
// txnMark). Run under -race to catch the window.
func TestDeleteRangeImmediateVisibility(t *testing.T) {
	runBadgerTest(t, nil, func(t *testing.T, db *DB) {
		for i := 0; i < 20; i++ {
			txnSet(t, db, []byte(fmt.Sprintf("v%02d", i)), []byte("x"), 0x00)
		}

		// Commit the DeleteRange; the moment it returns the index must be live.
		require.NoError(t, db.DeleteRange([]byte("v05"), []byte("v15")))

		// New txn (readTs > tombstone commitTs) — no sleep, no retry.
		err := db.View(func(txn *Txn) error {
			for i := 5; i < 15; i++ {
				_, gerr := txn.Get([]byte(fmt.Sprintf("v%02d", i)))
				require.ErrorIs(t, gerr, ErrKeyNotFound,
					"covered key v%02d must already be hidden", i)
			}
			// Uncovered keys still present.
			for _, i := range []int{4, 15} {
				_, gerr := txn.Get([]byte(fmt.Sprintf("v%02d", i)))
				require.NoError(t, gerr)
			}
			return nil
		})
		require.NoError(t, err)
	})
}

// TestDeleteRangeDoesNotHideInternalKeys: a range spanning the reserved prefix
// must not hide internal (!badger!) keys from the InternalAccess read path.
func TestDeleteRangeDoesNotHideInternalKeys(t *testing.T) {
	runBadgerTest(t, nil, func(t *testing.T, db *DB) {
		// A range that lexically spans the reserved !badger! prefix.
		require.NoError(t, db.DeleteRange([]byte("!"), []byte("\xff")))

		// The tombstone itself is persisted as an internal key under
		// !badger!rangedel; an internal-access iterator must still find it,
		// i.e. coverage must not have hidden internal keys.
		var foundInternal bool
		err := db.View(func(txn *Txn) error {
			iopts := DefaultIteratorOptions
			iopts.InternalAccess = true
			iopts.AllVersions = true
			iopts.PrefetchValues = false
			it := txn.NewIterator(iopts)
			defer it.Close()
			for it.Rewind(); it.Valid(); it.Next() {
				if bytes.HasPrefix(it.Item().Key(), rangeDelPrefix) {
					foundInternal = true
				}
			}
			return nil
		})
		require.NoError(t, err)
		require.True(t, foundInternal,
			"internal range-tombstone key must remain visible to InternalAccess")
	})
}

// TestDeleteRangeInvalid: begin >= end is rejected.
func TestDeleteRangeInvalid(t *testing.T) {
	runBadgerTest(t, nil, func(t *testing.T, db *DB) {
		require.Error(t, db.DeleteRange([]byte("b"), []byte("a")))
		require.Error(t, db.DeleteRange([]byte("a"), []byte("a")))
		require.Error(t, db.DeleteRange(nil, []byte("a")))
	})
}
