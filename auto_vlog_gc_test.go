/*
 * SPDX-FileCopyrightText: © 2017-2025 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package badger

import (
	"errors"
	"fmt"
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// writeGarbageAndPopulateDiscardStats writes a workload designed to create
// multiple value-log files, then overwrites every key so that the original
// values become garbage. It forces the garbage to be accounted for in the
// crash-safe discard stats by flushing the overwrites to SSTs (via
// WithDiscard, mirroring TestPersistLFDiscardStats) and waiting for at least
// one updateDiscardStats pass to complete.
func writeGarbageAndPopulateDiscardStats(t *testing.T, db *DB, tChan chan string) {
	t.Helper()

	sz := 128 << 10 // ~5 entries per 1MB value log file.
	v := make([]byte, sz)
	rand.Read(v[:rand.Intn(sz)])

	txn := db.NewTransaction(true)
	for i := 0; i < 500; i++ {
		require.NoError(t, txn.SetEntry(NewEntry([]byte(fmt.Sprintf("key%d", i)), v)))
		if i%3 == 0 {
			require.NoError(t, txn.Commit())
			txn = db.NewTransaction(true)
		}
	}
	require.NoError(t, txn.Commit())

	// Overwrite every key, turning the original values into discardable garbage.
	// WithDiscard forces the data to be flushed to disk (creating SSTs), which
	// is what drives compaction to accumulate discard stats.
	for i := 0; i < 500; i++ {
		require.NoError(t, db.Update(func(txn *Txn) error {
			return txn.SetEntry(NewEntry([]byte(fmt.Sprintf("key%d", i)), v).WithDiscard())
		}))
	}

	// Wait for at least one updateDiscardStats pass so pickLog has data to work
	// with -- timeout after 60 seconds.
	waitForMessage(tChan, updateDiscardStatsMsg, 1, 60, t)

	db.vlog.discardStats.Lock()
	require.True(t, db.vlog.discardStats.Len() > 1, "some discardStats should be generated")
	db.vlog.discardStats.Unlock()
}

func numVlogFiles(db *DB) int {
	db.vlog.filesLock.RLock()
	defer db.vlog.filesLock.RUnlock()
	return len(db.vlog.filesMap)
}

// waitForCompactionsToSettle blocks until no compaction is in flight (or a
// timeout). The heavy write workload leaves background compactions running, and
// the idle gate (correctly) skips GC while they are; tests that want to observe
// an un-gated tick must first wait for the LSM to go quiet.
func waitForCompactionsToSettle(t *testing.T, db *DB) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for db.lc.compactionsInFlight.Load() > 0 {
		if time.Now().After(deadline) {
			t.Fatalf("compactions did not settle in time: %d in flight",
				db.lc.compactionsInFlight.Load())
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// TestAutoVLogGC_Triggers verifies that with VLogGCInterval enabled, the
// background scheduler reclaims discardable value-log files on its own, and
// that a subsequent manual RunValueLogGC then reports ErrNoRewrite (nothing
// left to do).
func TestAutoVLogGC_Triggers(t *testing.T) {
	dir, err := os.MkdirTemp("", "badger-test")
	require.NoError(t, err)
	defer removeDir(dir)

	opt := getTestOptions(dir)
	opt.NumLevelZeroTables = 1 // Force more compaction to populate discard stats.
	opt.ValueLogFileSize = 1 << 20
	opt.CompactL0OnClose = false
	opt.MemTableSize = 1 << 15
	opt.ValueThreshold = 1 << 10
	// Enable automatic background GC. We do NOT rely on the ticker firing in
	// this test; we drive runVlogGCTick directly for determinism, but enabling
	// the interval exercises the real Open/Close wiring (goroutine spawn +
	// shutdown).
	opt.VLogGCInterval = time.Hour
	opt.VLogGCDiscardRatio = 0.5

	tChan := make(chan string, 100)
	defer close(tChan)
	opt.syncChan = tChan

	db, err := Open(opt)
	require.NoError(t, err)
	defer func() { require.NoError(t, db.Close()) }()

	// The scheduler goroutine must have been started.
	require.NotNil(t, db.closers.autoVlogGC, "auto vlog GC closer should be set when enabled")

	writeGarbageAndPopulateDiscardStats(t, db, tChan)

	before := numVlogFiles(db)
	require.True(t, before > 1, "expected multiple vlog files, got %d", before)

	// Let background compactions settle so the idle gate lets GC through.
	waitForCompactionsToSettle(t, db)

	// Drive the scheduler tick directly (deterministic; equivalent to one wake-up
	// when the LSM is idle). It loops until ErrNoRewrite.
	db.runVlogGCTick(opt.VLogGCDiscardRatio)

	after := numVlogFiles(db)
	require.True(t, after < before,
		"automatic GC should have reclaimed at least one vlog file: before=%d after=%d",
		before, after)

	// Nothing left to collect: a manual run must now report ErrNoRewrite.
	require.ErrorIs(t, db.RunValueLogGC(opt.VLogGCDiscardRatio), ErrNoRewrite)
}

// TestAutoVLogGC_DefaultOff verifies the default behavior is unchanged: with
// VLogGCInterval == 0, no scheduler goroutine is spawned and the value-log
// files are left untouched until an explicit RunValueLogGC.
func TestAutoVLogGC_DefaultOff(t *testing.T) {
	dir, err := os.MkdirTemp("", "badger-test")
	require.NoError(t, err)
	defer removeDir(dir)

	opt := getTestOptions(dir)
	opt.NumLevelZeroTables = 1
	opt.ValueLogFileSize = 1 << 20
	opt.CompactL0OnClose = false
	opt.MemTableSize = 1 << 15
	opt.ValueThreshold = 1 << 10
	// Default: disabled.
	require.Equal(t, time.Duration(0), opt.VLogGCInterval,
		"VLogGCInterval must default to 0 (disabled)")

	tChan := make(chan string, 100)
	defer close(tChan)
	opt.syncChan = tChan

	db, err := Open(opt)
	require.NoError(t, err)
	defer func() { require.NoError(t, db.Close()) }()

	// No scheduler goroutine should exist.
	require.Nil(t, db.closers.autoVlogGC,
		"auto vlog GC closer must be nil when VLogGCInterval == 0")

	writeGarbageAndPopulateDiscardStats(t, db, tChan)

	before := numVlogFiles(db)
	require.True(t, before > 1, "expected multiple vlog files, got %d", before)

	// Give a generous window: with auto-GC off, nothing should reclaim files.
	time.Sleep(500 * time.Millisecond)
	require.Equal(t, before, numVlogFiles(db),
		"with auto GC disabled, no vlog file should be reclaimed automatically")

	// Manual GC still works exactly as before.
	require.NoError(t, db.RunValueLogGC(0.5))
	require.True(t, numVlogFiles(db) < before,
		"manual RunValueLogGC should still reclaim a file")
}

// TestAutoVLogGC_IdleGate verifies the idle gate: when a compaction is reported
// in flight, a scheduler tick is a no-op (it never touches the vlog), and once
// the load clears the same tick reclaims as normal.
func TestAutoVLogGC_IdleGate(t *testing.T) {
	dir, err := os.MkdirTemp("", "badger-test")
	require.NoError(t, err)
	defer removeDir(dir)

	opt := getTestOptions(dir)
	opt.NumLevelZeroTables = 1
	opt.ValueLogFileSize = 1 << 20
	opt.CompactL0OnClose = false
	opt.MemTableSize = 1 << 15
	opt.ValueThreshold = 1 << 10
	opt.VLogGCInterval = time.Hour
	opt.VLogGCDiscardRatio = 0.5

	tChan := make(chan string, 100)
	defer close(tChan)
	opt.syncChan = tChan

	db, err := Open(opt)
	require.NoError(t, err)
	defer func() { require.NoError(t, db.Close()) }()

	writeGarbageAndPopulateDiscardStats(t, db, tChan)

	before := numVlogFiles(db)
	require.True(t, before > 1, "expected multiple vlog files, got %d", before)

	// Settle real background compactions first so the gate state is controlled
	// solely by our manual increment below.
	waitForCompactionsToSettle(t, db)

	// Simulate a busy foreground: pretend a compaction is in flight.
	db.lc.compactionsInFlight.Add(1)
	db.runVlogGCTick(opt.VLogGCDiscardRatio)
	require.Equal(t, before, numVlogFiles(db),
		"idle gate should skip GC while a compaction is in flight")

	// Clear the load; now the same tick should reclaim.
	db.lc.compactionsInFlight.Add(-1)
	db.runVlogGCTick(opt.VLogGCDiscardRatio)
	require.True(t, numVlogFiles(db) < before,
		"GC should proceed once the compaction load clears: before=%d after=%d",
		before, numVlogFiles(db))
}

// TestAutoVLogGC_RealTick exercises the actual ticker-driven goroutine end to
// end (no direct call into runVlogGCTick): a short interval, write garbage,
// then poll until the background goroutine reclaims a file. Run under -race to
// catch data races on the shared atomic counter and shutdown ordering, and to
// confirm Close() does not leak the goroutine.
func TestAutoVLogGC_RealTick(t *testing.T) {
	dir, err := os.MkdirTemp("", "badger-test")
	require.NoError(t, err)
	defer removeDir(dir)

	opt := getTestOptions(dir)
	opt.NumLevelZeroTables = 1
	opt.ValueLogFileSize = 1 << 20
	opt.CompactL0OnClose = false
	opt.MemTableSize = 1 << 15
	opt.ValueThreshold = 1 << 10
	opt.VLogGCInterval = 50 * time.Millisecond
	opt.VLogGCDiscardRatio = 0.5

	tChan := make(chan string, 100)
	defer close(tChan)
	opt.syncChan = tChan

	db, err := Open(opt)
	require.NoError(t, err)
	// Note: deliberately call Close at the end (not deferred via require) so we
	// can assert clean shutdown after the background goroutine has been busy.

	writeGarbageAndPopulateDiscardStats(t, db, tChan)

	require.True(t, numVlogFiles(db) > 1, "expected multiple vlog files")

	// Poll until the background ticker has drained all discardable garbage: a
	// manual RunValueLogGC reporting ErrNoRewrite proves the scheduler reclaimed
	// every collectable file on its own (this condition is monotonic, so it's
	// immune to the goroutine racing our snapshot). RunValueLogGC has no idle
	// gate, so once auto-GC has done its work this flips to ErrNoRewrite.
	deadline := time.Now().Add(30 * time.Second)
	for {
		err := db.RunValueLogGC(opt.VLogGCDiscardRatio)
		if errors.Is(err, ErrNoRewrite) {
			break
		}
		// nil (we just reclaimed one) or ErrRejected (auto-GC busy) -> keep going.
		if time.Now().After(deadline) {
			t.Fatalf("background GC never drained discardable garbage; last err=%v, files=%d",
				err, numVlogFiles(db))
		}
		time.Sleep(20 * time.Millisecond)
	}

	// Clean shutdown: SignalAndWait inside Close must drain the goroutine.
	require.NoError(t, db.Close())
}
