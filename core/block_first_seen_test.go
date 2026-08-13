package core

import (
	"encoding/binary"
	"testing"
	"time"

	ptypes "github.com/Chaintable/pipeline/types"
	"github.com/ethereum/go-ethereum/common"
)

func TestBlockFirstSeenTrackerPreservesFirstObservation(t *testing.T) {
	var tracker blockFirstSeenTracker
	hash := common.HexToHash("0x01")
	firstSeen := time.Date(2026, time.August, 10, 12, 0, 0, 0, time.UTC)

	tracker.mark(hash, firstSeen)
	tracker.mark(hash, firstSeen.Add(time.Minute))
	got, ok := tracker.get(hash, firstSeen.Add(2*time.Minute))
	if !ok {
		t.Fatal("first-seen timing was not found")
	}
	if !got.Equal(firstSeen) {
		t.Fatalf("first-seen timing = %v, want %v", got, firstSeen)
	}
}

func TestBlockFirstSeenTrackerExpiresOldObservations(t *testing.T) {
	var tracker blockFirstSeenTracker
	hash := common.HexToHash("0x01")
	firstSeen := time.Date(2026, time.August, 10, 12, 0, 0, 0, time.UTC)

	tracker.mark(hash, firstSeen)
	if _, ok := tracker.get(hash, firstSeen.Add(blockFirstSeenTTL)); ok {
		t.Fatal("expired first-seen timing was returned")
	}
}

func TestBlockFirstSeenTrackerLimitsEntries(t *testing.T) {
	var tracker blockFirstSeenTracker
	seenAt := time.Date(2026, time.August, 10, 12, 0, 0, 0, time.UTC)
	for i := 0; i <= blockFirstSeenMaxEntries; i++ {
		var hash common.Hash
		binary.BigEndian.PutUint64(hash[common.HashLength-8:], uint64(i))
		tracker.mark(hash, seenAt)
	}
	if got := tracker.order.Len(); got != blockFirstSeenMaxEntries {
		t.Fatalf("tracker contains %d entries, want %d", got, blockFirstSeenMaxEntries)
	}
	if _, ok := tracker.entries[common.Hash{}]; ok {
		t.Fatal("oldest first-seen timing was not pruned")
	}
}

func TestPipelineBlockFirstSeenAt(t *testing.T) {
	bc := new(BlockChain)
	hash := common.HexToHash("0x01")
	firstSeen := time.Now().Add(-time.Second).Truncate(time.Millisecond)
	bc.MarkBlockFirstSeen(hash, firstSeen)

	blocks := []ptypes.BlockContext{
		{Hash: hash},
		{Hash: common.HexToHash("0x02")},
	}
	got := bc.pipelineBlockFirstSeenAt(blocks)
	if got[hash] != firstSeen.UnixMilli() {
		t.Fatalf("first-seen timing = %d, want %d", got[hash], firstSeen.UnixMilli())
	}
	if _, ok := got[blocks[1].Hash]; ok {
		t.Fatal("missing first-seen timing was included")
	}
}
