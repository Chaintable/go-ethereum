// Copyright 2021 The go-ethereum Authors
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

package txtrace

import (
	"context"
	"fmt"
	"sync"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/metrics"
)

var (
	txTraceWriteSuccessCounter = metrics.NewRegisteredCounter("chain/txtraces/write/success", nil)
)

var (
	once              sync.Once
	defaultTraceStore *traceStore
)

type traceStore struct {
	db ethdb.Database
}

// NewTraceStore creates a new trace store.
func NewTraceStore(db ethdb.Database) *traceStore {
	if defaultTraceStore != nil {
		return defaultTraceStore
	}
	once.Do(func() {
		defaultTraceStore = &traceStore{db: db}
	})
	return defaultTraceStore
}

// GetTraceStore get singleton traceStore.
func GetTraceStore() *traceStore {
	return defaultTraceStore
}

func Close() {
	if defaultTraceStore != nil {
		defaultTraceStore.db.Close()
	}
}

func (t *traceStore) guard() error {
	if t.db == nil {
		return fmt.Errorf("txtrace mode not enabled")
	}
	return nil
}

// codeKey = CodePrefix + hash
func codeKey(hash common.Hash) []byte {
	return append([]byte("c"), hash.Bytes()...)
}

func (t *traceStore) ReadTxTrace(ctx context.Context, txHash common.Hash) ([]byte, error) {
	if err := t.guard(); err != nil {
		return []byte{}, err
	}

	data, err := t.db.Get(codeKey(txHash))
	if err != nil {
		log.Error("Failed to read tx trace result", "err", err)
	}
	return data, nil
}

// WriteTxTrace write the result of tx tracing by evm-tracing to db.
func (t *traceStore) WriteTxTrace(ctx context.Context, txHash common.Hash, data []byte) error {
	if err := t.guard(); err != nil {
		return err
	}
	if err := t.db.Put(codeKey(txHash), data); err != nil {
		log.Crit("Failed to write tx trace result", "err", err)
		return err
	}
	txTraceWriteSuccessCounter.Inc(1)
	return nil
}
