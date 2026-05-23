/*
 * SPDX-FileCopyrightText: © 2017-2025 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package badger

import (
	"encoding/binary"
	"math"
	"math/rand"
	"os"
	"testing"

	"github.com/dgraph-io/badger/v4/y"
)

// dgraph-shaped iterator micro-benchmarks.
//
// These benchmarks model the hot iterator paths in dgraph's posting layer
// (posting/mvcc.go: ReadPostingList, IterateDisk, sort.go: index bucket scan).
// They are intentionally distinct from the existing root-package benchmarks
// because none of those exercise the combination dgraph actually uses:
//
//   - NamespaceOffset = 1 (so DB.isBanned runs on every key)
//   - NumVersionsToKeep = math.MaxInt32 (dgraph keeps all versions)
//   - DetectConflicts = false (dgraph owns OCC)
//   - PrefetchValues = false (key-only scans in hot paths)
//   - AllVersions = true (MVCC-aware reads in ReadPostingList / rollup)
//   - Prefix-bounded scans over mid-sized structured keys
//
// Key layout mirrors dgraph's data keys (see x/keys.go in dgraph):
//
//   byte 0      : type prefix (0x00 = data)
//   bytes 1..8  : namespace, big-endian uint64
//   bytes 9..10 : 2-byte attr length, big-endian uint16
//   bytes 11..  : attr name (variable, ~16 bytes typical)
//   next 1 byte : subtype
//   last 8 bytes: UID, big-endian
//
// Total: 1 + 8 + 2 + len(attr) + 1 + 8 ≈ 36 bytes for a 16-byte attr.

const (
	dgKeyTypeData = 0x00
	dgAttrName    = "predicate_namespaced" // 20 bytes -> 40-byte keys
)

// dgKey constructs a dgraph-shaped data key with the given namespace, attr,
// subtype, and UID.
func dgKey(ns uint64, attr string, subtype byte, uid uint64) []byte {
	buf := make([]byte, 1+8+2+len(attr)+1+8)
	buf[0] = dgKeyTypeData
	binary.BigEndian.PutUint64(buf[1:9], ns)
	binary.BigEndian.PutUint16(buf[9:11], uint16(len(attr)))
	copy(buf[11:11+len(attr)], attr)
	buf[11+len(attr)] = subtype
	binary.BigEndian.PutUint64(buf[12+len(attr):], uid)
	return buf
}

// dgPrefix returns the (ns + attr) prefix shared by all keys for one predicate.
// This is what dgraph iterates with for prefix-bounded scans.
func dgPrefix(ns uint64, attr string) []byte {
	buf := make([]byte, 1+8+2+len(attr))
	buf[0] = dgKeyTypeData
	binary.BigEndian.PutUint64(buf[1:9], ns)
	binary.BigEndian.PutUint16(buf[9:11], uint16(len(attr)))
	copy(buf[11:], attr)
	return buf
}

// dgraphTestOptions returns the badger options that match dgraph's
// production configuration: managed DB, all-versions retention, no conflict
// detection, namespace offset = 1.
func dgraphTestOptions(dir string) Options {
	return DefaultOptions(dir).
		WithSyncWrites(false).
		WithLoggingLevel(WARNING).
		WithNumVersionsToKeep(math.MaxInt32).
		WithDetectConflicts(false).
		WithNamespaceOffset(1)
}

// dgraphLoadDB populates a managed DB with `nKeys` unique keys under a single
// predicate, each carrying `versionsPerKey` MVCC versions. UIDs are dense
// (1..nKeys) so prefix scans land contiguous keys, mirroring dgraph's
// `has()` predicate scans.
func dgraphLoadDB(b *testing.B, db *DB, ns uint64, attr string, nKeys, versionsPerKey int) {
	b.Helper()
	// Small value: realistic for posting-list deltas (a few hundred bytes is
	// the upper end; we use 64 bytes to keep total disk footprint bounded).
	val := make([]byte, 64)
	for i := range val {
		val[i] = byte(i)
	}

	const batch = 4000
	commitTs := uint64(1)
	for start := 0; start < nKeys; start += batch {
		end := start + batch
		if end > nKeys {
			end = nKeys
		}
		// Write `versionsPerKey` versions of each key in this batch.
		for v := 0; v < versionsPerKey; v++ {
			txn := db.NewTransactionAt(math.MaxUint64, true)
			for i := start; i < end; i++ {
				key := dgKey(ns, attr, 0, uint64(i+1))
				if err := txn.SetEntry(&Entry{Key: key, Value: val}); err != nil {
					b.Fatalf("SetEntry: %v", err)
				}
			}
			if err := txn.CommitAt(commitTs, nil); err != nil {
				b.Fatalf("CommitAt: %v", err)
			}
			commitTs++
		}
	}
	// Force everything to disk so iteration reflects the on-disk format
	// (avoids measuring only the memtable hit).
	if err := db.Flatten(2); err != nil {
		b.Fatalf("Flatten: %v", err)
	}
}

// openDgraphDB opens a managed DB with the dgraph-shaped options.
func openDgraphDB(b *testing.B) (*DB, string) {
	b.Helper()
	dir, err := os.MkdirTemp(".", "badger-dgraph-bench")
	y.Check(err)
	db, err := OpenManaged(dgraphTestOptions(dir))
	y.Check(err)
	return db, dir
}

// BenchmarkDgraphPrefixScanKeyOnly models the IterateDisk / index-scan path:
// prefix-bounded forward iteration with PrefetchValues=false, one version per
// key. This is what dgraph's `has()` predicate evaluator and sort.go index
// scans drive.
func BenchmarkDgraphPrefixScanKeyOnly(b *testing.B) {
	const (
		ns    = uint64(0x0102030405060708)
		nKeys = 200_000
	)
	db, dir := openDgraphDB(b)
	defer func() { db.Close(); removeDir(dir) }()
	dgraphLoadDB(b, db, ns, dgAttrName, nKeys, 1)

	prefix := dgPrefix(ns, dgAttrName)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		txn := db.NewTransactionAt(math.MaxUint64, false)
		opt := DefaultIteratorOptions
		opt.Prefix = prefix
		opt.PrefetchValues = false
		it := txn.NewIterator(opt)
		count := 0
		for it.Rewind(); it.Valid(); it.Next() {
			count++
		}
		it.Close()
		txn.Discard()
		if count != nKeys {
			b.Fatalf("expected %d keys, got %d", nKeys, count)
		}
	}
	b.ReportMetric(float64(nKeys), "keys/op")
}

// BenchmarkDgraphPrefixScanAllVersions models the rollup path:
// prefix-bounded forward iteration with PrefetchValues=false and
// AllVersions=true, where each key has several MVCC versions.
func BenchmarkDgraphPrefixScanAllVersions(b *testing.B) {
	const (
		ns             = uint64(0x0102030405060708)
		nKeys          = 50_000
		versionsPerKey = 5
	)
	db, dir := openDgraphDB(b)
	defer func() { db.Close(); removeDir(dir) }()
	dgraphLoadDB(b, db, ns, dgAttrName, nKeys, versionsPerKey)

	prefix := dgPrefix(ns, dgAttrName)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		txn := db.NewTransactionAt(math.MaxUint64, false)
		opt := DefaultIteratorOptions
		opt.Prefix = prefix
		opt.PrefetchValues = false
		opt.AllVersions = true
		it := txn.NewIterator(opt)
		count := 0
		for it.Rewind(); it.Valid(); it.Next() {
			count++
		}
		it.Close()
		txn.Discard()
		// Should see nKeys * versionsPerKey items.
		if count != nKeys*versionsPerKey {
			b.Fatalf("expected %d items, got %d", nKeys*versionsPerKey, count)
		}
	}
	b.ReportMetric(float64(nKeys*versionsPerKey), "items/op")
}

// BenchmarkDgraphKeyIteratorAllVersions models readFromDisk:
// a NewKeyIterator over a single posting key with AllVersions=true. dgraph
// invokes this on every cache miss in the posting layer.
func BenchmarkDgraphKeyIteratorAllVersions(b *testing.B) {
	const (
		ns             = uint64(0x0102030405060708)
		nKeys          = 20_000
		versionsPerKey = 8
	)
	db, dir := openDgraphDB(b)
	defer func() { db.Close(); removeDir(dir) }()
	dgraphLoadDB(b, db, ns, dgAttrName, nKeys, versionsPerKey)

	// Pre-build the lookup key set so the random selection cost stays out
	// of the measured loop.
	keys := make([][]byte, nKeys)
	for i := 0; i < nKeys; i++ {
		keys[i] = dgKey(ns, dgAttrName, 0, uint64(i+1))
	}
	rng := rand.New(rand.NewSource(1))

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		key := keys[rng.Intn(nKeys)]
		txn := db.NewTransactionAt(math.MaxUint64, false)
		opt := DefaultIteratorOptions
		opt.PrefetchValues = false
		opt.AllVersions = true
		it := txn.NewKeyIterator(key, opt)
		count := 0
		for it.Seek(key); it.Valid(); it.Next() {
			count++
		}
		it.Close()
		txn.Discard()
		if count != versionsPerKey {
			b.Fatalf("expected %d versions, got %d", versionsPerKey, count)
		}
	}
}

// BenchmarkDgraphPrefixScanNamespaceOffset measures the cost of
// NamespaceOffset=1 vs NamespaceOffset=-1 (off) under identical workload.
// This isolates the per-key cost of DB.isBanned, which today takes an
// RWMutex on every iterator step even when no namespaces are banned.
func BenchmarkDgraphPrefixScanNamespaceOffset(b *testing.B) {
	const (
		ns    = uint64(0x0102030405060708)
		nKeys = 100_000
	)
	cases := []struct {
		name   string
		nsOff  int // -1 = off, 1 = dgraph-style
	}{
		{"off", -1},
		{"dgraph", 1},
	}

	for _, c := range cases {
		b.Run(c.name, func(b *testing.B) {
			dir, err := os.MkdirTemp(".", "badger-dgraph-nsoff")
			y.Check(err)
			defer removeDir(dir)
			opts := DefaultOptions(dir).
				WithSyncWrites(false).
				WithLoggingLevel(WARNING).
				WithNumVersionsToKeep(math.MaxInt32).
				WithDetectConflicts(false).
				WithNamespaceOffset(c.nsOff)
			db, err := OpenManaged(opts)
			y.Check(err)
			defer db.Close()
			dgraphLoadDB(b, db, ns, dgAttrName, nKeys, 1)

			prefix := dgPrefix(ns, dgAttrName)
			b.ResetTimer()
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				txn := db.NewTransactionAt(math.MaxUint64, false)
				opt := DefaultIteratorOptions
				opt.Prefix = prefix
				opt.PrefetchValues = false
				it := txn.NewIterator(opt)
				count := 0
				for it.Rewind(); it.Valid(); it.Next() {
					count++
				}
				it.Close()
				txn.Discard()
				if count != nKeys {
					b.Fatalf("expected %d keys, got %d", nKeys, count)
				}
			}
		})
	}
}

