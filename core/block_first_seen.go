package core

import (
	"container/list"
	"sync"
	"time"

	ptypes "github.com/Chaintable/pipeline/types"
	"github.com/ethereum/go-ethereum/common"
)

const (
	blockFirstSeenTTL        = 24 * time.Hour
	blockFirstSeenMaxEntries = 65536
)

type blockFirstSeenEntry struct {
	hash   common.Hash
	seenAt time.Time
}

type blockFirstSeenTracker struct {
	mu      sync.Mutex
	entries map[common.Hash]*list.Element
	order   list.List
}

func (t *blockFirstSeenTracker) mark(hash common.Hash, seenAt time.Time) {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.prune(seenAt)
	if _, ok := t.entries[hash]; ok {
		return
	}
	entry := &blockFirstSeenEntry{hash: hash, seenAt: seenAt}
	if t.entries == nil {
		t.entries = make(map[common.Hash]*list.Element)
	}
	t.entries[hash] = t.order.PushBack(entry)
	t.prune(seenAt)
}

func (t *blockFirstSeenTracker) get(hash common.Hash, now time.Time) (time.Time, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()

	element, ok := t.entries[hash]
	if !ok {
		return time.Time{}, false
	}
	entry := element.Value.(*blockFirstSeenEntry)
	if now.Sub(entry.seenAt) >= blockFirstSeenTTL {
		delete(t.entries, hash)
		t.order.Remove(element)
		return time.Time{}, false
	}
	return entry.seenAt, true
}

func (t *blockFirstSeenTracker) prune(now time.Time) {
	for t.order.Len() > 0 {
		front := t.order.Front()
		entry := front.Value.(*blockFirstSeenEntry)
		if t.order.Len() <= blockFirstSeenMaxEntries && now.Sub(entry.seenAt) < blockFirstSeenTTL {
			return
		}
		delete(t.entries, entry.hash)
		t.order.Remove(front)
	}
}

// MarkBlockFirstSeen records the first time a block entered this execution
// client. Repeated payloads for the same hash do not move the timestamp.
func (bc *BlockChain) MarkBlockFirstSeen(hash common.Hash, seenAt time.Time) {
	bc.blockFirstSeen.mark(hash, seenAt)
}

func (bc *BlockChain) firstSeenAt(hash common.Hash) (time.Time, bool) {
	return bc.blockFirstSeen.get(hash, time.Now())
}

func (bc *BlockChain) pipelineBlockFirstSeenAt(blocks []ptypes.BlockContext) map[common.Hash]int64 {
	firstSeenAt := make(map[common.Hash]int64)
	for _, block := range blocks {
		if seenAt, ok := bc.firstSeenAt(block.Hash); ok {
			firstSeenAt[block.Hash] = seenAt.UnixMilli()
		}
	}
	if len(firstSeenAt) == 0 {
		return nil
	}
	return firstSeenAt
}
