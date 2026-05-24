/*
 * SPDX-FileCopyrightText: © 2017-2025 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package badger

import (
	"bytes"
	"errors"
	"fmt"
	"testing"

	"github.com/dgraph-io/badger/v4/y"
	"github.com/stretchr/testify/require"
)

// TestKeyOnlyIterator_ValueReturnsErrKeyOnlyMode covers iterator.go:Item.Value
// short-circuit when item.keyOnly is set (iter4).
func TestKeyOnlyIterator_ValueReturnsErrKeyOnlyMode(t *testing.T) {
	runBadgerTest(t, nil, func(t *testing.T, db *DB) {
		txnSet(t, db, []byte("k1"), []byte("v1"), 0)

		require.NoError(t, db.View(func(txn *Txn) error {
			opt := DefaultIteratorOptions
			opt.KeyOnly = true
			it := txn.NewIterator(opt)
			defer it.Close()
			for it.Rewind(); it.Valid(); it.Next() {
				item := it.Item()
				err := item.Value(func(v []byte) error { return nil })
				require.True(t, errors.Is(err, ErrKeyOnlyMode),
					"Value() should return ErrKeyOnlyMode, got %v", err)
			}
			return nil
		}))
	})
}

// TestKeyOnlyIterator_ValueCopyReturnsErrKeyOnlyMode covers iterator.go:Item.ValueCopy.
func TestKeyOnlyIterator_ValueCopyReturnsErrKeyOnlyMode(t *testing.T) {
	runBadgerTest(t, nil, func(t *testing.T, db *DB) {
		txnSet(t, db, []byte("k1"), []byte("v1"), 0)

		require.NoError(t, db.View(func(txn *Txn) error {
			opt := DefaultIteratorOptions
			opt.KeyOnly = true
			it := txn.NewIterator(opt)
			defer it.Close()
			for it.Rewind(); it.Valid(); it.Next() {
				item := it.Item()
				buf, err := item.ValueCopy(nil)
				require.True(t, errors.Is(err, ErrKeyOnlyMode))
				require.Nil(t, buf)
			}
			return nil
		}))
	})
}

// TestKeyOnlyIterator_EstimatedSizeReturnsKeyLen covers iterator.go:Item.EstimatedSize
// short-circuit (returns int64(len(item.key)) when keyOnly).
func TestKeyOnlyIterator_EstimatedSizeReturnsKeyLen(t *testing.T) {
	runBadgerTest(t, nil, func(t *testing.T, db *DB) {
		txnSet(t, db, []byte("abc"), []byte("lots-of-value-bytes-here"), 0)

		require.NoError(t, db.View(func(txn *Txn) error {
			opt := DefaultIteratorOptions
			opt.KeyOnly = true
			it := txn.NewIterator(opt)
			defer it.Close()
			it.Rewind()
			require.True(t, it.Valid())
			item := it.Item()
			require.Equal(t, int64(len("abc")), item.EstimatedSize())
			return nil
		}))
	})
}

// TestKeyOnlyIterator_ValueSizeIsZero covers iterator.go:Item.ValueSize short-circuit.
func TestKeyOnlyIterator_ValueSizeIsZero(t *testing.T) {
	runBadgerTest(t, nil, func(t *testing.T, db *DB) {
		txnSet(t, db, []byte("k1"), []byte("hello-world"), 0)

		require.NoError(t, db.View(func(txn *Txn) error {
			opt := DefaultIteratorOptions
			opt.KeyOnly = true
			it := txn.NewIterator(opt)
			defer it.Close()
			it.Rewind()
			require.True(t, it.Valid())
			require.Equal(t, int64(0), it.Item().ValueSize())
			return nil
		}))
	})
}

// TestKeyOnlyIterator_ForcesPrefetchOff covers iterator.go:NewIterator forcing
// PrefetchValues=false when KeyOnly=true.
func TestKeyOnlyIterator_ForcesPrefetchOff(t *testing.T) {
	runBadgerTest(t, nil, func(t *testing.T, db *DB) {
		txnSet(t, db, []byte("k1"), []byte("v1"), 0)

		require.NoError(t, db.View(func(txn *Txn) error {
			opt := DefaultIteratorOptions
			opt.KeyOnly = true
			opt.PrefetchValues = true
			opt.PrefetchSize = 100
			it := txn.NewIterator(opt)
			defer it.Close()
			require.False(t, it.opt.PrefetchValues,
				"NewIterator must force PrefetchValues=false when KeyOnly=true")
			it.Rewind()
			require.True(t, it.Valid())
			return nil
		}))
	})
}

// TestKeyOnlyIterator_KeyMetaVersionWork verifies that the metadata methods on Item
// still work correctly in KeyOnly mode (per the contract documented on KeyOnly).
// Indirectly covers fill() setting item.keyOnly while still populating meta/version.
func TestKeyOnlyIterator_KeyMetaVersionWork(t *testing.T) {
	runBadgerTest(t, nil, func(t *testing.T, db *DB) {
		key := []byte("hello")
		val := []byte("world")
		txn := db.NewTransaction(true)
		require.NoError(t, txn.SetEntry(NewEntry(key, val).WithMeta(0x42)))
		require.NoError(t, txn.Commit())

		require.NoError(t, db.View(func(txn *Txn) error {
			opt := DefaultIteratorOptions
			opt.KeyOnly = true
			it := txn.NewIterator(opt)
			defer it.Close()
			it.Rewind()
			require.True(t, it.Valid())
			item := it.Item()
			require.Equal(t, key, item.Key())
			require.Equal(t, byte(0x42), item.UserMeta())
			require.NotZero(t, item.Version())
			require.False(t, item.IsDeletedOrExpired())
			require.Equal(t, int64(len(key)), item.KeySize())
			require.False(t, item.DiscardEarlierVersions())
			return nil
		}))
	})
}

// TestKeyOnlyIterator_PrefetchValuesFalseStillWorks covers fill() KeyOnly branch
// without prefetch (the actual hot path; PrefetchValues=false bypasses the
// prefetch goroutine entirely).
func TestKeyOnlyIterator_PrefetchValuesFalseStillWorks(t *testing.T) {
	runBadgerTest(t, nil, func(t *testing.T, db *DB) {
		for i := 0; i < 20; i++ {
			txnSet(t, db, []byte(fmt.Sprintf("k%02d", i)), []byte("vvv"), 0)
		}

		require.NoError(t, db.View(func(txn *Txn) error {
			opt := DefaultIteratorOptions
			opt.PrefetchValues = false
			opt.KeyOnly = true
			it := txn.NewIterator(opt)
			defer it.Close()
			count := 0
			for it.Rewind(); it.Valid(); it.Next() {
				item := it.Item()
				require.NotEmpty(t, item.Key())
				err := item.Value(func(v []byte) error { return nil })
				require.True(t, errors.Is(err, ErrKeyOnlyMode))
				count++
			}
			require.Equal(t, 20, count)
			return nil
		}))
	})
}

// TestCanSeeInternalKeys is a table-driven unit test for the iter3 helper.
// It is the single source of truth for which prefix shapes allow internal keys.
func TestCanSeeInternalKeys(t *testing.T) {
	cases := []struct {
		name   string
		prefix []byte
		want   bool
	}{
		{"empty prefix sees everything", nil, true},
		{"empty slice sees everything", []byte{}, true},
		{"prefix starting with '!' (badgerPrefix[0]) may overlap", []byte("!badger"), true},
		{"prefix starting with 0x21 (== '!') overlap", []byte{0x21, 0xff}, true},
		{"prefix starting with 0x00 cannot overlap", []byte{0x00, 0x01, 0x02}, false},
		{"prefix starting with 'a' cannot overlap", []byte("abc"), false},
		{"prefix starting with high byte cannot overlap", []byte{0xff}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := canSeeInternalKeys(tc.prefix)
			require.Equal(t, tc.want, got)
		})
	}
}

// TestRegressionSameKeyDedup locks in the iter5 lastKey behavior: when multiple
// versions of the same user-key exist and AllVersions=false, the iterator must
// surface exactly one item per user-key (the freshest non-expired one). This
// test exercises the user-key-only comparison path.
func TestRegressionSameKeyDedup(t *testing.T) {
	runBadgerTest(t, nil, func(t *testing.T, db *DB) {
		// Write three versions of "k1" and three versions of "k2".
		for _, k := range []string{"k1", "k2"} {
			for v := 1; v <= 3; v++ {
				txnSet(t, db, []byte(k), []byte(fmt.Sprintf("v%d", v)), 0)
			}
		}

		require.NoError(t, db.View(func(txn *Txn) error {
			opt := DefaultIteratorOptions
			opt.PrefetchValues = false
			it := txn.NewIterator(opt)
			defer it.Close()
			var seen []string
			for it.Rewind(); it.Valid(); it.Next() {
				seen = append(seen, string(it.Item().Key()))
			}
			require.Equal(t, []string{"k1", "k2"}, seen,
				"non-AllVersions iterator must dedup to one item per user-key")
			return nil
		}))

		// AllVersions=true should see all 3 versions per key.
		require.NoError(t, db.View(func(txn *Txn) error {
			opt := DefaultIteratorOptions
			opt.AllVersions = true
			opt.PrefetchValues = false
			it := txn.NewIterator(opt)
			defer it.Close()
			counts := map[string]int{}
			for it.Rewind(); it.Valid(); it.Next() {
				counts[string(it.Item().Key())]++
			}
			require.Equal(t, 3, counts["k1"])
			require.Equal(t, 3, counts["k2"])
			return nil
		}))
	})
}

// TestRegressionInternalKeysHidden locks in iter3 behavior: a default user
// iterator (no InternalAccess) must not surface badger-internal keys even when
// the optimized canSeeInternalKeys path is taken (prefix nil or '!').
func TestRegressionInternalKeysHidden(t *testing.T) {
	runBadgerTest(t, nil, func(t *testing.T, db *DB) {
		txnSet(t, db, []byte("user-key"), []byte("v"), 0)

		require.NoError(t, db.View(func(txn *Txn) error {
			it := txn.NewIterator(DefaultIteratorOptions)
			defer it.Close()
			for it.Rewind(); it.Valid(); it.Next() {
				k := it.Item().Key()
				require.False(t, isBadgerInternalKey(k),
					"default iterator surfaced internal key %q", k)
			}
			return nil
		}))
	})
}

func isBadgerInternalKey(k []byte) bool {
	if len(k) < len(badgerPrefix) {
		return false
	}
	for i := range badgerPrefix {
		if k[i] != badgerPrefix[i] {
			return false
		}
	}
	return true
}

// TestRegressionIsBannedNoBans locks in iter1's fast-path correctness:
// when no namespaces have been banned, isBanned must return nil for any key
// (regardless of NamespaceOffset). Exercises the hasAny.Load()==false branch.
func TestRegressionIsBannedNoBans(t *testing.T) {
	opt := getTestOptions("")
	opt.NamespaceOffset = 0
	runBadgerTest(t, &opt, func(t *testing.T, db *DB) {
		// hasAny is false at this point - never any bans on a fresh DB.
		require.False(t, db.bannedNamespaces.hasAny.Load())

		// isBanned must return nil for any key on a fresh DB.
		key := y.KeyWithTs([]byte("hello-world-12345"), 1)
		require.NoError(t, db.isBanned(key))

		// Empty key, short key - all fine, fast path doesn't even reach the
		// length check.
		require.NoError(t, db.isBanned(nil))
		require.NoError(t, db.isBanned([]byte("x")))
	})
}

// TestRegressionIsBannedWithBan covers the slow-path: hasAny=true, key matches
// a banned namespace.
func TestRegressionIsBannedWithBan(t *testing.T) {
	opt := getTestOptions("")
	opt.NamespaceOffset = 0
	runBadgerTest(t, &opt, func(t *testing.T, db *DB) {
		const ns = uint64(0x4242)
		require.NoError(t, db.BanNamespace(ns))
		require.True(t, db.bannedNamespaces.hasAny.Load(),
			"hasAny must flip to true after add()")

		// Key in banned namespace: ns bytes (big-endian) at offset 0 + suffix
		// + 8B ts.
		banned := y.KeyWithTs(append(y.U64ToBytes(ns), []byte("suffix")...), 1)
		require.ErrorIs(t, db.isBanned(banned), ErrBannedKey)

		// Key in a different namespace: not banned.
		other := y.KeyWithTs(append(y.U64ToBytes(0x1111), []byte("suffix")...), 1)
		require.NoError(t, db.isBanned(other))
	})
}

// TestRegressionIsBannedShortKeyWithBans covers the slow path early-return when
// hasAny=true but the key is too short to extract a namespace.
func TestRegressionIsBannedShortKeyWithBans(t *testing.T) {
	opt := getTestOptions("")
	opt.NamespaceOffset = 0
	runBadgerTest(t, &opt, func(t *testing.T, db *DB) {
		const ns = uint64(0x4242)
		require.NoError(t, db.BanNamespace(ns))

		// Key shorter than NamespaceOffset+8 should be accepted (nil) — the
		// pre-existing semantics treat un-namespaceable keys as non-banned.
		require.NoError(t, db.isBanned([]byte("short")))
		require.NoError(t, db.isBanned(nil))
	})
}

// TestRegressionIsBannedNegativeOffset covers the early-return: NamespaceOffset<0
// short-circuits before any atomic load.
func TestRegressionIsBannedNegativeOffset(t *testing.T) {
	opt := getTestOptions("")
	opt.NamespaceOffset = -1
	runBadgerTest(t, &opt, func(t *testing.T, db *DB) {
		require.NoError(t, db.isBanned([]byte("anything")))
		require.NoError(t, db.isBanned(nil))
	})
}

// TestRegressionFillNonKeyOnly exercises the non-KeyOnly fill path (iter2's
// hoist), verifying that values are still copied correctly when KeyOnly=false.
// This is the path that all existing badger users hit by default.
func TestRegressionFillNonKeyOnly(t *testing.T) {
	runBadgerTest(t, nil, func(t *testing.T, db *DB) {
		txnSet(t, db, []byte("k1"), []byte("expected-value-bytes"), 0)

		require.NoError(t, db.View(func(txn *Txn) error {
			opt := DefaultIteratorOptions
			opt.PrefetchValues = false // exercise yieldItemValue path
			it := txn.NewIterator(opt)
			defer it.Close()
			it.Rewind()
			require.True(t, it.Valid())
			got, err := it.Item().ValueCopy(nil)
			require.NoError(t, err)
			require.Equal(t, []byte("expected-value-bytes"), got)
			return nil
		}))
	})
}

// TestRegressionPrefetchValuesTrue exercises the prefetch goroutine path,
// confirming KeyOnly=false + PrefetchValues=true still works (iter2 + iter4
// didn't break the default).
func TestRegressionPrefetchValuesTrue(t *testing.T) {
	runBadgerTest(t, nil, func(t *testing.T, db *DB) {
		for i := 0; i < 5; i++ {
			txnSet(t, db, []byte(fmt.Sprintf("k%d", i)), []byte(fmt.Sprintf("v%d", i)), 0)
		}

		require.NoError(t, db.View(func(txn *Txn) error {
			opt := DefaultIteratorOptions
			opt.PrefetchValues = true
			opt.PrefetchSize = 10
			it := txn.NewIterator(opt)
			defer it.Close()
			seen := 0
			for it.Rewind(); it.Valid(); it.Next() {
				item := it.Item()
				v, err := item.ValueCopy(nil)
				require.NoError(t, err)
				require.NotEmpty(t, v)
				seen++
			}
			require.Equal(t, 5, seen)
			return nil
		}))
	})
}

// TestRegressionHasPrefixShortPrefixFallback covers the hasPrefix fallback
// branch where the user-supplied prefix is longer than userKey (len(p) >
// len(key)-8). The short-circuit "len(key) >= len(p)+8" must be false so we
// take the ParseKey path and correctly return false (no spurious match
// against ts bytes).
func TestRegressionHasPrefixShortPrefixFallback(t *testing.T) {
	runBadgerTest(t, nil, func(t *testing.T, db *DB) {
		// Single short key — userKey is 1 byte.
		txnSet(t, db, []byte("a"), []byte("v"), 0)

		require.NoError(t, db.View(func(txn *Txn) error {
			opt := DefaultIteratorOptions
			// Prefix is longer than the single existing userKey "a". The
			// optimized hasPrefix must take the ParseKey fallback (because
			// len(key) < len(prefix)+8) and return false — no key matches.
			opt.Prefix = []byte("abcdefghij") // 10 bytes, longer than "a"
			it := txn.NewIterator(opt)
			defer it.Close()
			count := 0
			for it.Rewind(); it.Valid(); it.Next() {
				count++
			}
			require.Equal(t, 0, count, "no key should match an over-long prefix")
			return nil
		}))
	})
}

// TestRegressionHasPrefixFastPath covers the optimized hasPrefix path where
// len(prefix) fits within userKey: it must still correctly identify matching
// and non-matching keys, equivalent to the old ParseKey-based check.
func TestRegressionHasPrefixFastPath(t *testing.T) {
	runBadgerTest(t, nil, func(t *testing.T, db *DB) {
		txnSet(t, db, []byte("alpha/1"), []byte("v"), 0)
		txnSet(t, db, []byte("alpha/2"), []byte("v"), 0)
		txnSet(t, db, []byte("beta/1"), []byte("v"), 0)

		require.NoError(t, db.View(func(txn *Txn) error {
			opt := DefaultIteratorOptions
			opt.Prefix = []byte("alpha/")
			it := txn.NewIterator(opt)
			defer it.Close()
			count := 0
			for it.Rewind(); it.Valid(); it.Next() {
				require.True(t, bytes.HasPrefix(it.Item().Key(), opt.Prefix))
				count++
			}
			require.Equal(t, 2, count, "alpha/ should match both alpha/1 and alpha/2")
			return nil
		}))
	})
}

// TestRegressionTrackReadsReadOnly verifies that iterators built from
// read-only transactions skip the addReadKey path entirely (trackReads
// remains false at construction). Functionally, the iterator still works:
// keys/values are visible, and no conflict-detection state is mutated.
func TestRegressionTrackReadsReadOnly(t *testing.T) {
	runBadgerTest(t, nil, func(t *testing.T, db *DB) {
		txnSet(t, db, []byte("k1"), []byte("v1"), 0)
		txnSet(t, db, []byte("k2"), []byte("v2"), 0)

		require.NoError(t, db.View(func(txn *Txn) error {
			// db.View creates a read-only txn (update=false).
			require.False(t, txn.update, "View must give a read-only txn")
			it := txn.NewIterator(DefaultIteratorOptions)
			defer it.Close()
			require.False(t, it.trackReads, "read-only iterator must not track reads")

			seen := 0
			for it.Rewind(); it.Valid(); it.Next() {
				_ = it.Item().Key() // would call addReadKey on a write txn
				seen++
			}
			require.Equal(t, 2, seen)

			// Confirm no reads were recorded on the txn (would matter for
			// conflict detection on a write txn).
			require.Empty(t, txn.reads, "no reads should be recorded on read-only txn")
			return nil
		}))
	})
}

// TestRegressionTrackReadsWriteTxn verifies that iterators built from
// read-write transactions still call addReadKey, populating the conflict
// detection set so Update transactions can detect concurrent writes.
func TestRegressionTrackReadsWriteTxn(t *testing.T) {
	runBadgerTest(t, nil, func(t *testing.T, db *DB) {
		txnSet(t, db, []byte("k1"), []byte("v1"), 0)
		txnSet(t, db, []byte("k2"), []byte("v2"), 0)

		require.NoError(t, db.Update(func(txn *Txn) error {
			// db.Update creates a read-write txn (update=true).
			require.True(t, txn.update, "Update must give a read-write txn")
			it := txn.NewIterator(DefaultIteratorOptions)
			defer it.Close()
			require.True(t, it.trackReads, "read-write iterator must track reads")

			for it.Rewind(); it.Valid(); it.Next() {
				_ = it.Item() // calls addReadKey
			}
			require.NotEmpty(t, txn.reads, "reads should be recorded for conflict detection")
			return nil
		}))
	})
}

// TestRegressionLazyItemSliceValueRead exercises Item.Value/ValueCopy on
// items produced by an iterator that uses the lazy-slice allocation path
// (newItem no longer eagerly allocates y.Slice). yieldItemValue and
// prefetchValue must lazy-init item.slice on first use; if they don't,
// item.slice.Resize() will nil-deref.
//
// Covers three configurations that previously relied on the eager
// allocation: PrefetchValues=true (prefetch goroutine), PrefetchValues=false
// + Item.Value() (synchronous read), and ValueCopy on a lazy item.
func TestRegressionLazyItemSliceValueRead(t *testing.T) {
	runBadgerTest(t, nil, func(t *testing.T, db *DB) {
		// Two keys with distinct, non-empty values so the value path is
		// actually traversed (deletes/empty values exit yieldItemValue
		// before touching slice).
		txnSet(t, db, []byte("k1"), []byte("value-one"), 0)
		txnSet(t, db, []byte("k2"), []byte("value-two-larger"), 0)

		// Variant A: PrefetchValues=true triggers the prefetch goroutine
		// path, which calls yieldItemValue → item.slice.Resize.
		require.NoError(t, db.View(func(txn *Txn) error {
			opt := DefaultIteratorOptions
			opt.PrefetchValues = true
			it := txn.NewIterator(opt)
			defer it.Close()
			count := 0
			for it.Rewind(); it.Valid(); it.Next() {
				item := it.Item()
				require.NoError(t, item.Value(func(val []byte) error {
					require.NotEmpty(t, val, "prefetched value must be non-empty")
					return nil
				}))
				count++
			}
			require.Equal(t, 2, count)
			return nil
		}))

		// Variant B: PrefetchValues=false + synchronous Item.Value(). Item
		// is produced with slice=nil; yieldItemValue must initialize it.
		require.NoError(t, db.View(func(txn *Txn) error {
			opt := DefaultIteratorOptions
			opt.PrefetchValues = false
			it := txn.NewIterator(opt)
			defer it.Close()
			count := 0
			for it.Rewind(); it.Valid(); it.Next() {
				item := it.Item()
				require.NoError(t, item.Value(func(val []byte) error {
					require.NotEmpty(t, val)
					return nil
				}))
				count++
			}
			require.Equal(t, 2, count)
			return nil
		}))

		// Variant C: ValueCopy on a lazy item — separate code path through
		// yieldItemValue → SafeCopy.
		require.NoError(t, db.View(func(txn *Txn) error {
			opt := DefaultIteratorOptions
			opt.PrefetchValues = false
			it := txn.NewIterator(opt)
			defer it.Close()
			it.Rewind()
			require.True(t, it.Valid())
			buf, err := it.Item().ValueCopy(nil)
			require.NoError(t, err)
			require.NotEmpty(t, buf)
			return nil
		}))
	})
}
