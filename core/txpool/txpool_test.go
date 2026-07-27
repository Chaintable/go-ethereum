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

package txpool

import (
	"errors"
	"math/big"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core"
	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/event"
	"github.com/ethereum/go-ethereum/params"
	"github.com/ethereum/go-ethereum/trie"
)

// unsyncedBlockChain simulates a chain whose states are all unavailable except
// the empty one, mimicking a path-scheme node restarted in the middle of the
// initial snap sync (neither the head state nor the genesis state is present).
type unsyncedBlockChain struct {
	statedb       *state.StateDB
	chainHeadFeed event.Feed
}

func (bc *unsyncedBlockChain) Config() *params.ChainConfig {
	return params.TestChainConfig
}

func (bc *unsyncedBlockChain) CurrentBlock() *types.Header {
	return &types.Header{
		Number:     new(big.Int),
		Difficulty: common.Big0,
		GasLimit:   30_000_000,
	}
}

func (bc *unsyncedBlockChain) Genesis() *types.Block {
	return types.NewBlock(bc.CurrentBlock(), nil, nil, trie.NewStackTrie(nil))
}

func (bc *unsyncedBlockChain) SubscribeChainHeadEvent(ch chan<- core.ChainHeadEvent) event.Subscription {
	return bc.chainHeadFeed.Subscribe(ch)
}

func (bc *unsyncedBlockChain) StateAt(header *types.Header) (*state.StateDB, error) {
	if header.Root == types.EmptyRootHash {
		return bc.statedb, nil
	}
	return nil, errors.New("state is not available")
}

// Tests that the pool can still be created when neither the head state nor the
// genesis state is available, by falling back to an empty state.
func TestNewWithoutState(t *testing.T) {
	statedb, _ := state.New(types.EmptyRootHash, state.NewDatabaseForTesting())
	pool, err := New(1, &unsyncedBlockChain{statedb: statedb}, nil)
	if err != nil {
		t.Fatalf("failed to create pool without available states: %v", err)
	}
	pool.Close()
}
