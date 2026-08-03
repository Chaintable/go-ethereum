// Copyright 2022 The go-ethereum Authors
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
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/rawdb/eradb"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/params"
)

const (
	// freezerRecheckInterval is the frequency to check the key-value database for
	// chain progression that might permit new blocks to be frozen into immutable
	// storage.
	freezerRecheckInterval = time.Minute

	// freezerBatchLimit is the maximum number of blocks to freeze in one batch
	// before doing an fsync and deleting it from the key-value store.
	freezerBatchLimit = 30000

	// pruneAncientBatchLimit is the maximum number of blocks whose ancient
	// block data is removed in one prune step when ancient pruning is enabled.
	// Bounded steps let the transaction-index removal persist its progress and
	// the reclaimed disk space become visible continuously while a large
	// pre-existing ancient store is being pruned away.
	pruneAncientBatchLimit = 100000
)

// chainFreezer is a wrapper of chain ancient store with additional chain freezing
// feature. The background thread will keep moving ancient chain segments from
// key-value database to flat files for saving space on live database.
type chainFreezer struct {
	ancients ethdb.AncientStore // Ancient store for storing cold chain segment
	eradb    *eradb.Store       // Optional Era database used as a backup for the pruned chain

	// pruneAncient indicates that historical block data (bodies, receipts and
	// access lists) should not be moved into the ancient store: nil placeholder
	// entries are appended instead and dropped again by tail truncation, while
	// any pre-existing ancient block data is removed in the background. Headers
	// and canonical hashes are always retained.
	pruneAncient bool

	quit    chan struct{}
	wg      sync.WaitGroup
	trigger chan chan struct{} // Manual blocking freeze trigger, test determinism
}

// newChainFreezer initializes the freezer for ancient chain segment.
//
//   - if the empty directory is given, initializes the pure in-memory
//     state freezer (e.g. dev mode).
//   - if non-empty directory is given, initializes the regular file-based
//     state freezer.
func newChainFreezer(datadir string, eraDir string, namespace string, readonly bool, pruneAncient bool) (*chainFreezer, error) {
	if datadir == "" {
		return &chainFreezer{
			ancients:     NewMemoryFreezer(readonly, chainFreezerTableConfigs),
			pruneAncient: pruneAncient,
			quit:         make(chan struct{}),
			trigger:      make(chan chan struct{}),
		}, nil
	}
	freezer, err := NewFreezer(datadir, namespace, readonly, freezerTableSize, chainFreezerTableConfigs)
	if err != nil {
		return nil, err
	}
	edb, err := eradb.New(resolveChainEraDir(datadir, eraDir))
	if err != nil {
		return nil, err
	}
	return &chainFreezer{
		ancients:     freezer,
		eradb:        edb,
		pruneAncient: pruneAncient,
		quit:         make(chan struct{}),
		trigger:      make(chan chan struct{}),
	}, nil
}

// Close closes the chain freezer instance and terminates the background thread.
func (f *chainFreezer) Close() error {
	select {
	case <-f.quit:
	default:
		close(f.quit)
	}
	f.wg.Wait()

	if f.eradb != nil {
		f.eradb.Close()
	}
	return f.ancients.Close()
}

// readHeadNumber returns the number of chain head block. 0 is returned if the
// block is unknown or not available yet.
func (f *chainFreezer) readHeadNumber(db ethdb.KeyValueReader) uint64 {
	hash := ReadHeadBlockHash(db)
	if hash == (common.Hash{}) {
		log.Warn("Head block is not reachable")
		return 0
	}
	number, ok := ReadHeaderNumber(db, hash)
	if !ok {
		log.Error("Number of head block is missing")
		return 0
	}
	return number
}

// readFinalizedNumber returns the number of finalized block. 0 is returned
// if the block is unknown or not available yet.
func (f *chainFreezer) readFinalizedNumber(db ethdb.KeyValueReader) uint64 {
	hash := ReadFinalizedBlockHash(db)
	if hash == (common.Hash{}) {
		return 0
	}
	number, ok := ReadHeaderNumber(db, hash)
	if !ok {
		log.Error("Number of finalized block is missing")
		return 0
	}
	return number
}

// freezeThreshold returns the threshold for chain freezing. It's determined
// by formula: max(finality, HEAD-params.FullImmutabilityThreshold).
func (f *chainFreezer) freezeThreshold(db ethdb.KeyValueReader) (uint64, error) {
	var (
		head      = f.readHeadNumber(db)
		final     = f.readFinalizedNumber(db)
		headLimit uint64
	)
	if head > params.FullImmutabilityThreshold {
		headLimit = head - params.FullImmutabilityThreshold
	}
	// In ancient pruning mode, block data is discarded once frozen. Ignore the
	// finality signal, which may trail the head very closely, and always retain
	// the most recent blocks in the key-value store as the serving window.
	if f.pruneAncient {
		if headLimit == 0 {
			return 0, errors.New("freezing threshold is not available")
		}
		return headLimit, nil
	}
	if final == 0 && headLimit == 0 {
		return 0, errors.New("freezing threshold is not available")
	}
	if final > headLimit {
		return final, nil
	}
	return headLimit, nil
}

// freeze is a background thread that periodically checks the blockchain for any
// import progress and moves ancient data from the fast database into the freezer.
//
// This functionality is deliberately broken off from block importing to avoid
// incurring additional data shuffling delays on block propagation.
func (f *chainFreezer) freeze(db ethdb.KeyValueStore) {
	var (
		backoff   bool
		triggered chan struct{} // Used in tests
		nfdb      = &nofreezedb{KeyValueStore: db}
	)
	timer := time.NewTimer(freezerRecheckInterval)
	defer timer.Stop()

	for {
		select {
		case <-f.quit:
			log.Info("Freezer shutting down")
			return
		default:
		}
		if backoff {
			// If we were doing a manual trigger, notify it
			if triggered != nil {
				triggered <- struct{}{}
				triggered = nil
			}
			select {
			case <-timer.C:
				backoff = false
				timer.Reset(freezerRecheckInterval)
			case triggered = <-f.trigger:
				backoff = false
			case <-f.quit:
				return
			}
		}
		threshold, err := f.freezeThreshold(nfdb)
		if err != nil {
			backoff = true
			log.Debug("Current full block not old enough to freeze", "err", err)
			continue
		}
		frozen, _ := f.Ancients() // no error will occur, safe to ignore

		// Short circuit if the blocks below threshold are already frozen.
		if frozen != 0 && frozen-1 >= threshold {
			// Even without new blocks to freeze, previously frozen block data
			// (e.g. from before ancient pruning was enabled) might still be
			// pending removal.
			if f.pruneAncient {
				f.pruneAncientHistory(db, threshold+1)
			}
			backoff = true
			log.Debug("Ancient blocks frozen already", "threshold", threshold, "frozen", frozen)
			continue
		}
		// Seems we have data ready to be frozen, process in usable batches
		var (
			start = time.Now()
			first = frozen    // the first block to freeze
			last  = threshold // the last block to freeze
		)
		if last-first+1 > freezerBatchLimit {
			last = freezerBatchLimit + first - 1
		}
		// In ancient pruning mode, remove the transaction index entries of the
		// scheduled blocks before freezing them. Once frozen, only their nil
		// placeholders remain after the key-value data is wiped below, so an
		// index entry surviving past this point could never be unindexed again.
		// Abort the cycle if the removal was interrupted; the untouched blocks
		// are rescheduled in the next cycle.
		if f.pruneAncient && !f.unindexBeforeFreeze(db, last+1) {
			backoff = true
			continue
		}
		ancients, err := f.freezeRange(nfdb, first, last)
		if err != nil {
			log.Error("Error in block freeze operation", "err", err)
			backoff = true
			continue
		}
		// Batch of blocks have been frozen, flush them before wiping from key-value store
		if err := f.SyncAncient(); err != nil {
			log.Crit("Failed to flush frozen tables", "err", err)
		}
		// In ancient pruning mode, drop the freshly appended placeholder
		// entries again by advancing the group tails. The transaction index
		// entries of the frozen blocks have already been removed above.
		if f.pruneAncient {
			f.pruneAncientHistory(db, last+1)
		}
		// Wipe out all data from the active database
		batch := db.NewBatch()
		for i := 0; i < len(ancients); i++ {
			// Always keep the genesis block in active database
			if first+uint64(i) != 0 {
				DeleteBlockWithoutNumber(batch, ancients[i], first+uint64(i))
				DeleteCanonicalHash(batch, first+uint64(i))
			}
		}
		if err := batch.Write(); err != nil {
			log.Crit("Failed to delete frozen canonical blocks", "err", err)
		}
		batch.Reset()

		// Wipe out side chains also and track dangling side chains
		var dangling []common.Hash
		frozen, _ = f.Ancients() // Needs reload after during freezeRange
		for number := first; number < frozen; number++ {
			// Always keep the genesis block in active database
			if number != 0 {
				dangling = ReadAllHashes(db, number)
				for _, hash := range dangling {
					log.Trace("Deleting side chain", "number", number, "hash", hash)
					DeleteBlock(batch, hash, number)
				}
			}
		}
		if err := batch.Write(); err != nil {
			log.Crit("Failed to delete frozen side blocks", "err", err)
		}
		batch.Reset()

		// Step into the future and delete any dangling side chains
		if frozen > 0 {
			tip := frozen
			for len(dangling) > 0 {
				drop := make(map[common.Hash]struct{})
				for _, hash := range dangling {
					log.Debug("Dangling parent from Freezer", "number", tip-1, "hash", hash)
					drop[hash] = struct{}{}
				}
				children := ReadAllHashes(db, tip)
				for i := 0; i < len(children); i++ {
					// Dig up the child and ensure it's dangling
					child := ReadHeader(nfdb, children[i], tip)
					if child == nil {
						log.Error("Missing dangling header", "number", tip, "hash", children[i])
						continue
					}
					if _, ok := drop[child.ParentHash]; !ok {
						children = append(children[:i], children[i+1:]...)
						i--
						continue
					}
					// Delete all block data associated with the child
					log.Debug("Deleting dangling block", "number", tip, "hash", children[i], "parent", child.ParentHash)
					DeleteBlock(batch, children[i], tip)
				}
				dangling = children
				tip++
			}
			if err := batch.Write(); err != nil {
				log.Crit("Failed to delete dangling side blocks", "err", err)
			}
		}

		// Log something friendly for the user
		context := []interface{}{
			"blocks", frozen - first, "elapsed", common.PrettyDuration(time.Since(start)), "number", frozen - 1,
		}
		if n := len(ancients); n > 0 {
			context = append(context, []interface{}{"hash", ancients[n-1]}...)
		}
		log.Debug("Deep froze chain segment", context...)

		// Avoid database thrashing with tiny writes
		if frozen-first < freezerBatchLimit {
			backoff = true
		}
	}
}

// freezeRange moves a batch of chain segments from the fast database to the freezer.
// The parameters (number, limit) specify the relevant block range, both of which
// are included.
func (f *chainFreezer) freezeRange(nfdb *nofreezedb, number, limit uint64) (hashes []common.Hash, err error) {
	hashes = make([]common.Hash, 0, limit-number+1)

	_, err = f.ModifyAncients(func(op ethdb.AncientWriteOp) error {
		for ; number <= limit; number++ {
			// Retrieve all the components of the canonical block.
			hash := ReadCanonicalHash(nfdb, number)
			if hash == (common.Hash{}) {
				return fmt.Errorf("canonical hash missing, can't freeze block %d", number)
			}
			header := ReadHeaderRLP(nfdb, hash, number)
			if len(header) == 0 {
				return fmt.Errorf("block header missing, can't freeze block %d", number)
			}
			// In ancient pruning mode, block bodies, receipts and access lists
			// are not moved into the ancient store. Nil placeholder entries are
			// appended instead, keeping the table heads aligned; they are
			// dropped right away by the subsequent tail truncation.
			var body, receipts, bals []byte
			if !f.pruneAncient {
				body = ReadBodyRLP(nfdb, hash, number)
				if len(body) == 0 {
					return fmt.Errorf("block body missing, can't freeze block %d", number)
				}
				receipts = ReadReceiptsRLP(nfdb, hash, number)
				if len(receipts) == 0 {
					return fmt.Errorf("block receipts missing, can't freeze block %d", number)
				}
				// An empty block access list is allowed and may occur in multiple
				// scenarios, such as:
				//   - pre-Amsterdam blocks
				//   - post-Amsterdam blocks with the BAL absent (e.g. pruned by network)
				//   - post-Amsterdam blocks with an explicitly empty BAL
				//
				// In these cases, a nil entry will be stored in the BAL table as the
				// absence placeholder.
				bals = ReadAccessListRLP(nfdb, hash, number)
			}

			// Write to the batch.
			if err := op.AppendRaw(ChainFreezerHashTable, number, hash[:]); err != nil {
				return fmt.Errorf("can't write hash to Freezer: %v", err)
			}
			if err := op.AppendRaw(ChainFreezerHeaderTable, number, header); err != nil {
				return fmt.Errorf("can't write header to Freezer: %v", err)
			}
			if err := op.AppendRaw(ChainFreezerBodiesTable, number, body); err != nil {
				return fmt.Errorf("can't write body to Freezer: %v", err)
			}
			if err := op.AppendRaw(ChainFreezerReceiptTable, number, receipts); err != nil {
				return fmt.Errorf("can't write receipts to Freezer: %v", err)
			}
			if err := op.AppendRaw(ChainFreezerBALTable, number, bals); err != nil {
				return fmt.Errorf("can't write bals to Freezer: %v", err)
			}
			hashes = append(hashes, hash)
		}
		return nil
	})
	return hashes, err
}

// unindexBeforeFreeze removes the transaction index entries of all blocks
// below the given boundary, ahead of their block data being dropped by the
// ancient pruning mode. It reports whether the index tail has reached the
// boundary. Freezing must not proceed otherwise: the bodies required for
// enumerating the transaction hashes become unavailable once the blocks are
// frozen as placeholders and wiped from the key-value store, which would
// leave permanently dangling index entries behind.
// Note: the check reflects the indexer's progress at this instant. With the
// transaction index retention capped at the block data retention window, the
// indexer never writes entries at or below the freezing threshold, except
// during the initial index construction (no index tail persisted yet) racing
// a chain head advance, where a handful of entries around the boundary may
// end up permanently dangling. Lookups of those return null gracefully.
func (f *chainFreezer) unindexBeforeFreeze(db ethdb.KeyValueStore, boundary uint64) bool {
	txTail := ReadTxIndexTail(db)
	if txTail == nil || *txTail >= boundary {
		return true
	}
	// The ancient-aware database view is needed to read the bodies of blocks
	// which have already been frozen with their data intact (e.g. before the
	// pruning mode was enabled).
	fulldb := &freezerdb{KeyValueStore: db, chainFreezer: f}
	UnindexTransactions(fulldb, *txTail, boundary, f.quit, false)

	txTail = ReadTxIndexTail(db)
	return txTail != nil && *txTail >= boundary
}

// pruneAncientHistory removes the block bodies, receipts and access lists of
// all frozen blocks below the given tail from the ancient store, along with
// any transaction index entries still referring to them. It's only invoked
// from the freeze thread when ancient pruning mode is enabled.
//
// The removal is performed in bounded steps, persisting the progress via the
// group tails and the transaction index tail, so an interrupted prune (e.g.
// the initial removal of a large pre-existing ancient store) resumes where it
// left off on the next cycle.
func (f *chainFreezer) pruneAncientHistory(db ethdb.KeyValueStore, tail uint64) {
	frozen, _ := f.Ancients() // no error will occur, safe to ignore
	if tail > frozen {
		tail = frozen
	}
	blockTail, err := f.Tail(ChainFreezerBlockDataGroup)
	if err != nil {
		return
	}
	balTail, err := f.Tail(ChainFreezerBALGroup)
	if err != nil {
		return
	}
	// Resume from the lowest group tail: the groups are truncated one after
	// another below and may diverge if the process dies in between; the group
	// which is already past a truncation mark short-circuits it internally.
	prev := min(blockTail, balTail)
	if prev >= tail {
		return
	}
	var (
		start  = time.Now()
		logged = start
		from   = prev
		// The ancient-aware database view is needed by the transaction
		// unindexer to read bodies which are still in the ancient store.
		fulldb = &freezerdb{KeyValueStore: db, chainFreezer: f}
	)
	for prev < tail {
		select {
		case <-f.quit:
			log.Info("Ancient history pruning interrupted", "pruned", prev-from, "remaining", tail-prev, "elapsed", common.PrettyDuration(time.Since(start)))
			return
		default:
		}
		next := min(prev+pruneAncientBatchLimit, tail)

		// Remove any transaction index entries still referring to the pruned
		// blocks, while their bodies remain readable from the ancient store
		// until truncated below. This only concerns blocks frozen with their
		// data intact (e.g. before the pruning mode was enabled): for blocks
		// frozen by the pruning mode itself, the index entries have already
		// been removed ahead of freezing.
		if txTail := ReadTxIndexTail(db); txTail != nil && *txTail < next {
			UnindexTransactions(fulldb, *txTail, next, f.quit, false)
			select {
			case <-f.quit:
				// Don't prune bodies the unindexer didn't get through.
				log.Info("Ancient history pruning interrupted", "pruned", prev-from, "remaining", tail-prev, "elapsed", common.PrettyDuration(time.Since(start)))
				return
			default:
			}
		}
		if _, err := f.TruncateTail(ChainFreezerBlockDataGroup, next); err != nil {
			log.Error("Failed to prune ancient block data", "tail", next, "err", err)
			return
		}
		if _, err := f.TruncateTail(ChainFreezerBALGroup, next); err != nil {
			log.Error("Failed to prune ancient block access lists", "tail", next, "err", err)
			return
		}
		prev = next

		// Report the progress of a long-running removal (e.g. the initial
		// pruning of a large pre-existing ancient store), but stay quiet
		// during the tiny steady-state rounds accompanying each freeze cycle.
		if time.Since(logged) > 8*time.Second {
			log.Info("Pruning ancient history", "pruned", prev-from, "remaining", tail-prev, "tail", prev, "elapsed", common.PrettyDuration(time.Since(start)))
			logged = time.Now()
		}
	}
	logger := log.Debug
	if time.Since(start) > 8*time.Second {
		logger = log.Info
	}
	logger("Pruned ancient history", "from", from, "tail", tail, "elapsed", common.PrettyDuration(time.Since(start)))
}

// Ancient retrieves an ancient binary blob from the append-only immutable files.
func (f *chainFreezer) Ancient(kind string, number uint64) ([]byte, error) {
	// Lookup the entry in the underlying ancient store, assuming that
	// headers and hashes are always available.
	if kind == ChainFreezerHeaderTable || kind == ChainFreezerHashTable {
		return f.ancients.Ancient(kind, number)
	}
	group, err := tableTailGroup(kind)
	if err != nil {
		return nil, err
	}
	tail, err := f.ancients.Tail(group)
	if err != nil {
		return nil, err
	}
	// Lookup the entry in the underlying ancient store if it's not pruned
	if number >= tail {
		return f.ancients.Ancient(kind, number)
	}
	// Lookup the entry in the optional era backend
	if f.eradb == nil {
		return nil, errOutOfBounds
	}
	switch kind {
	case ChainFreezerBodiesTable:
		return f.eradb.GetRawBody(number)
	case ChainFreezerReceiptTable:
		return f.eradb.GetRawReceipts(number)
	case ChainFreezerBALTable:
		return nil, errOutOfBounds
	}
	return nil, errUnknownTable
}

// tableTailGroup returns the tail group identifier for a chain freezer table.
func tableTailGroup(kind string) (string, error) {
	if cfg, ok := chainFreezerTableConfigs[kind]; ok {
		return cfg.tailGroup, nil
	}
	return "", errUnknownTable
}

// ReadAncients executes an operation while preventing mutations to the freezer,
// i.e. if fn performs multiple reads, they will be consistent with each other.
func (f *chainFreezer) ReadAncients(fn func(ethdb.AncientReaderOp) error) (err error) {
	if store, ok := f.ancients.(*Freezer); ok {
		store.writeLock.Lock()
		defer store.writeLock.Unlock()
	}
	return fn(f)
}

// Methods below are just pass-through to the underlying ancient store.

func (f *chainFreezer) Ancients() (uint64, error) {
	return f.ancients.Ancients()
}

func (f *chainFreezer) Tail(group string) (uint64, error) {
	return f.ancients.Tail(group)
}

func (f *chainFreezer) AncientSize(kind string) (uint64, error) {
	return f.ancients.AncientSize(kind)
}

func (f *chainFreezer) AncientRange(kind string, start, count, maxBytes uint64) ([][]byte, error) {
	return f.ancients.AncientRange(kind, start, count, maxBytes)
}

func (f *chainFreezer) AncientBytes(kind string, id, offset, length uint64) ([]byte, error) {
	return f.ancients.AncientBytes(kind, id, offset, length)
}

func (f *chainFreezer) ModifyAncients(fn func(ethdb.AncientWriteOp) error) (int64, error) {
	return f.ancients.ModifyAncients(fn)
}

func (f *chainFreezer) TruncateHead(items uint64) (uint64, error) {
	return f.ancients.TruncateHead(items)
}

func (f *chainFreezer) TruncateTail(group string, items uint64) (uint64, error) {
	return f.ancients.TruncateTail(group, items)
}

func (f *chainFreezer) SyncAncient() error {
	return f.ancients.SyncAncient()
}
