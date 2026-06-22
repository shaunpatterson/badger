/*
 * SPDX-FileCopyrightText: © 2017-2025 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package badger

import (
	"bytes"
	"sync/atomic"

	"github.com/dgraph-io/badger/v4/y"
)

// rangeTombstone records that the half-open user-key interval [Begin, End) was
// deleted at version Ts. Any version V of a covered key with V < Ts (and
// Ts <= readTs) is hidden from reads.
type rangeTombstone struct {
	Begin []byte
	End   []byte
	Ts    uint64
}

// covers reports whether this tombstone deletes user key `key` at the given
// readTs and candidate version V, per the MVCC visibility rule V < Ts <= readTs.
func (rt rangeTombstone) covers(key []byte, version, readTs uint64) bool {
	if rt.Ts > readTs || version >= rt.Ts {
		return false
	}
	// Half-open [Begin, End): Begin <= key < End.
	return bytes.Compare(key, rt.Begin) >= 0 && bytes.Compare(key, rt.End) < 0
}

// rangeTombstoneIndex is an in-memory, copy-on-write set of range tombstones.
// Readers load the immutable slice via an atomic pointer and scan it with no
// lock; writers (DeleteRange commits) install a new slice. When DeleteRange is
// never used the slice is nil and every read does a single cheap nil/len check.
type rangeTombstoneIndex struct {
	tombstones atomic.Pointer[[]rangeTombstone]
}

// add installs a new tombstone via copy-on-write.
func (ix *rangeTombstoneIndex) add(rt rangeTombstone) {
	for {
		oldp := ix.tombstones.Load()
		var old []rangeTombstone
		if oldp != nil {
			old = *oldp
		}
		next := make([]rangeTombstone, len(old), len(old)+1)
		copy(next, old)
		next = append(next, rt)
		if ix.tombstones.CompareAndSwap(oldp, &next) {
			return
		}
	}
}

// empty reports whether no tombstones are registered (the common case).
func (ix *rangeTombstoneIndex) empty() bool {
	p := ix.tombstones.Load()
	return p == nil || len(*p) == 0
}

// covered reports whether user key `key` at candidate version `version`, read at
// `readTs`, is hidden by some range tombstone (the max-covering-Ts rule reduces
// to "any covering tombstone exists": if one covers, the key is hidden).
func (ix *rangeTombstoneIndex) covered(key []byte, version, readTs uint64) bool {
	p := ix.tombstones.Load()
	if p == nil {
		return false
	}
	for _, rt := range *p {
		if rt.covers(key, version, readTs) {
			return true
		}
	}
	return false
}

// coveredByRangeTombstone is the DB-level read hook used by point Get and the
// iterator. It returns false instantly when no range tombstones exist.
func (db *DB) coveredByRangeTombstone(key []byte, version, readTs uint64) bool {
	if db.rangeTombstones.empty() {
		return false
	}
	return db.rangeTombstones.covered(key, version, readTs)
}

// DeleteRange records a non-blocking range tombstone over the half-open
// user-key interval [begin, end). Every key in that interval written before this
// call's commit timestamp becomes invisible to subsequent reads, while keys
// written afterwards (higher version) reappear. Unlike DropPrefix/DropAll it
// does NOT stall writes, flush memtables, or stop compactions.
//
// Reclamation of covered keys and of the tombstone itself happens lazily; in
// this build it is enforced purely on the read path (see DESIGN.md).
func (db *DB) DeleteRange(begin, end []byte) error {
	return db.Update(func(txn *Txn) error {
		return txn.DeleteRange(begin, end)
	})
}

// DeleteRangeAt is the managed-mode variant of DeleteRange, committing the range
// tombstone at the provided commit timestamp.
func (db *DB) DeleteRangeAt(begin, end []byte, commitTs uint64) error {
	if !db.opt.managedTxns {
		panic("Cannot use DeleteRangeAt with managedDB=false. Use DeleteRange instead.")
	}
	txn := db.newTransaction(true, true)
	defer txn.Discard()
	if err := txn.DeleteRange(begin, end); err != nil {
		return err
	}
	return txn.CommitAt(commitTs, nil)
}

// DeleteRange adds a range tombstone over [begin, end) to the transaction's
// pending writes. It composes with other operations in the same commit.
func (txn *Txn) DeleteRange(begin, end []byte) error {
	if len(begin) == 0 {
		return ErrEmptyKey
	}
	if bytes.Compare(begin, end) >= 0 {
		return ErrInvalidRange
	}
	e := &Entry{
		Key:   begin,
		Value: y.Copy(end),
		meta:  bitRangeDelete,
	}
	return txn.modify(e)
}

// rebuildRangeTombstoneIndex scans the DB for persisted range-tombstone entries
// and repopulates the in-memory index. Called once at Open(). Mirrors
// initBannedNamespaces: it uses an internal, all-versions iterator so every
// tombstone version is captured and the tombstone entries (which are otherwise
// hidden from user reads) are visible to the scan.
func (db *DB) rebuildRangeTombstoneIndex() error {
	return db.View(func(txn *Txn) error {
		iopts := DefaultIteratorOptions
		iopts.PrefetchValues = false
		iopts.AllVersions = true
		iopts.InternalAccess = true
		it := txn.NewIterator(iopts)
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			if item.meta&bitRangeDelete == 0 {
				continue
			}
			begin := item.KeyCopy(nil)
			end, err := item.ValueCopy(nil)
			if err != nil {
				return err
			}
			db.rangeTombstones.add(rangeTombstone{
				Begin: begin,
				End:   end,
				Ts:    item.version,
			})
		}
		return nil
	})
}
