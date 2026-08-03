// Copyright 2026 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

package rawdb

import (
	"math/big"
	"testing"

	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethdb/memorydb"
	"github.com/ethereum/go-ethereum/params"
)

// writeTestChainSegment writes a contiguous canonical chain segment [from, to]
// into the key-value store, with full block data attached.
func writeTestChainSegment(db *freezerdb, from, to uint64) []*types.Header {
	var headers []*types.Header
	for number := from; number <= to; number++ {
		header := &types.Header{
			Number: new(big.Int).SetUint64(number),
			Extra:  []byte("test header"),
		}
		hash := header.Hash()
		WriteHeader(db, header)
		WriteCanonicalHash(db, hash, number)
		WriteBody(db, hash, number, &types.Body{})
		WriteReceipts(db, hash, number, types.Receipts{})
		headers = append(headers, header)
	}
	return headers
}

// writeTestHeadBlock plants a chain head marker at the given block number.
func writeTestHeadBlock(db *freezerdb, number uint64) {
	header := &types.Header{
		Number: new(big.Int).SetUint64(number),
		Extra:  []byte("test head"),
	}
	WriteHeader(db, header)
	WriteCanonicalHash(db, header.Hash(), number)
	WriteHeadBlockHash(db, header.Hash())
}

// TestChainFreezerPruneAncient checks that with ancient pruning enabled, the
// chain freezer retains headers and hashes but drops block bodies and receipts
// once blocks age out of the immutability window.
func TestChainFreezerPruneAncient(t *testing.T) {
	rawDb, err := Open(memorydb.New(), OpenOptions{PruneAncient: true})
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	db := rawDb.(*freezerdb)
	defer db.Close()

	// Chain head sits at FullImmutabilityThreshold+50, so blocks [0, 50] are
	// eligible for freezing while everything above must stay in the key-value
	// store as the serving window.
	var (
		head    = uint64(params.FullImmutabilityThreshold + 50)
		headers = writeTestChainSegment(db, 0, 60)
	)
	writeTestHeadBlock(db, head)
	WriteTxIndexTail(db, 0)

	if err := db.Freeze(); err != nil {
		t.Fatalf("failed to trigger freeze cycle: %v", err)
	}
	frozen, _ := db.Ancients()
	if frozen != 51 {
		t.Fatalf("unexpected number of frozen items, got %d, want %d", frozen, 51)
	}
	// Block data of the frozen range must be gone, in both the ancient store
	// and the key-value store.
	tail, _ := db.Tail(ChainFreezerBlockDataGroup)
	if tail != 51 {
		t.Fatalf("unexpected block data tail, got %d, want %d", tail, 51)
	}
	tail, _ = db.Tail(ChainFreezerBALGroup)
	if tail != 51 {
		t.Fatalf("unexpected access list tail, got %d, want %d", tail, 51)
	}
	for _, number := range []uint64{1, 25, 50} {
		hash := headers[number].Hash()
		if blob := ReadHeaderRLP(db, hash, number); len(blob) == 0 {
			t.Fatalf("header %d missing after pruning", number)
		}
		if got := ReadCanonicalHash(db, number); got != hash {
			t.Fatalf("canonical hash %d mismatch after pruning", number)
		}
		if blob := ReadBodyRLP(db, hash, number); len(blob) != 0 {
			t.Fatalf("body %d still present after pruning", number)
		}
		if blob := ReadReceiptsRLP(db, hash, number); len(blob) != 0 {
			t.Fatalf("receipts %d still present after pruning", number)
		}
	}
	// Blocks above the threshold must remain fully accessible from the
	// key-value store.
	for _, number := range []uint64{51, 60} {
		hash := headers[number].Hash()
		if blob := ReadBodyRLP(db, hash, number); len(blob) == 0 {
			t.Fatalf("body %d missing from serving window", number)
		}
	}
	// The transaction index tail must have advanced along with the pruning.
	if txTail := ReadTxIndexTail(db); txTail == nil || *txTail != 51 {
		t.Fatalf("unexpected tx index tail, got %v, want %d", txTail, 51)
	}
}

// TestChainFreezerPruneAncientLegacy checks that enabling ancient pruning on a
// database with a pre-existing ancient store removes the historical block data
// in the background while retaining headers and hashes.
func TestChainFreezerPruneAncientLegacy(t *testing.T) {
	var (
		kvdb    = memorydb.New()
		ancient = t.TempDir()
	)
	// Phase 1: run the freezer without pruning, moving blocks [0, 50] with
	// their full block data into the ancient store.
	rawDb, err := Open(kvdb, OpenOptions{Ancient: ancient})
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	db := rawDb.(*freezerdb)

	head := uint64(params.FullImmutabilityThreshold + 50)
	headers := writeTestChainSegment(db, 0, 60)
	writeTestHeadBlock(db, head)
	WriteTxIndexTail(db, 0)

	if err := db.Freeze(); err != nil {
		t.Fatalf("failed to trigger freeze cycle: %v", err)
	}
	if frozen, _ := db.Ancients(); frozen != 51 {
		t.Fatalf("unexpected number of frozen items, got %d, want %d", frozen, 51)
	}
	if blob, err := db.Ancient(ChainFreezerBodiesTable, 25); err != nil || len(blob) == 0 {
		t.Fatalf("body 25 missing from ancient store: %v", err)
	}
	// Close the freezer only: closing the wrapper would also wipe the
	// in-memory key-value store which must survive into phase 2.
	if err := db.chainFreezer.Close(); err != nil {
		t.Fatalf("failed to close chain freezer: %v", err)
	}

	// Phase 2: reopen the database with pruning enabled. The pre-existing
	// ancient block data must be removed by the background pruner.
	rawDb, err = Open(kvdb, OpenOptions{Ancient: ancient, PruneAncient: true})
	if err != nil {
		t.Fatalf("failed to reopen database: %v", err)
	}
	db = rawDb.(*freezerdb)
	defer db.Close()

	if err := db.Freeze(); err != nil {
		t.Fatalf("failed to trigger freeze cycle: %v", err)
	}
	tail, _ := db.Tail(ChainFreezerBlockDataGroup)
	if tail != 51 {
		t.Fatalf("unexpected block data tail, got %d, want %d", tail, 51)
	}
	for _, number := range []uint64{1, 25, 50} {
		hash := headers[number].Hash()
		if blob := ReadHeaderRLP(db, hash, number); len(blob) == 0 {
			t.Fatalf("header %d missing after pruning", number)
		}
		if blob := ReadBodyRLP(db, hash, number); len(blob) != 0 {
			t.Fatalf("body %d still present after pruning", number)
		}
	}
	if txTail := ReadTxIndexTail(db); txTail == nil || *txTail != 51 {
		t.Fatalf("unexpected tx index tail, got %v, want %d", txTail, 51)
	}
}
