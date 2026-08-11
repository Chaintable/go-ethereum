package core

import (
	"math/big"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
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

func TestPipelineBlockContextIncludesFirstSeenTiming(t *testing.T) {
	bc := new(BlockChain)
	header := &types.Header{
		Number:     big.NewInt(12),
		ParentHash: common.HexToHash("0x01"),
		Time:       1234,
	}
	firstSeen := time.Now().Add(-time.Second).Truncate(time.Millisecond)
	bc.MarkBlockFirstSeen(header.Hash(), firstSeen)

	block := bc.pipelineBlockContext(header)
	if block.FirstSeenAtUnixMilli != firstSeen.UnixMilli() {
		t.Fatalf("FirstSeenAtUnixMilli = %d, want %d", block.FirstSeenAtUnixMilli, firstSeen.UnixMilli())
	}
}
