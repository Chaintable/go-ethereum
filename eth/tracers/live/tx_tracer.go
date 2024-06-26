package live

import (
	"context"
	"encoding/json"
	"fmt"
	"math/big"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/core/tracing"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/eth/tracers"
	"github.com/ethereum/go-ethereum/eth/tracers/native"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/params"
	"github.com/ethereum/go-ethereum/txtrace"
)

func init() {
	tracers.LiveDirectory.Register("txtrace", newTxTracer)
}

type TxTracer struct {
	config     TxTracerConfig
	ctx        *tracers.Context
	callTracer *tracers.Tracer
}

type TxTracerConfig struct {
	TraceStore string `json:"trace_store"` // Path to the directory where the tracer logs will be stored
}

func newTxTracer(cfg json.RawMessage) (*tracing.Hooks, error) {
	var config TxTracerConfig
	if cfg != nil {
		if err := json.Unmarshal(cfg, &config); err != nil {
			return nil, fmt.Errorf("failed to parse config: %v", err)
		}
	}
	t := &TxTracer{
		config: config,
	}

	return &tracing.Hooks{
		OnBlockchainInit: t.OnBlockchainInit,
		//OnClose:          t.OnClose,
		OnBlockStart: t.OnBlockStart,
		OnTxStart:    t.OnTxStart,
		OnTxEnd:      t.OnTxEnd,
		OnEnter:      t.OnEnter,
		OnExit:       t.OnExit,
	}, nil
}

func (t *TxTracer) OnBlockchainInit(chainConfig *params.ChainConfig) {
	if t.config.TraceStore == "" {
		t.config.TraceStore = "/var/data/tracedb"
	}
	db, err := rawdb.NewLevelDBDatabase(t.config.TraceStore, 512, 4096, "eth/db/tracedb", false)
	if err != nil {
		panic("failed to open tx trace database: " + err.Error())
	}
	txtrace.NewTraceStore(db)
}

func (t *TxTracer) OnClose() {
	txtrace.Close()
}

func (t *TxTracer) OnBlockStart(event tracing.BlockEvent) {
	t.ctx = &tracers.Context{
		BlockNumber: event.Block.Number(),
		BlockHash:   event.Block.Hash(),
	}
}

func (t *TxTracer) OnTxStart(vm *tracing.VMContext, tx *types.Transaction, from common.Address) {
	t.ctx.TxHash = tx.Hash()
	callTracer, err := native.NewFlatCallTracer(t.ctx)
	if err != nil {
		log.Crit("Failed to create call tracer", "err", err)
	}
	if callTracer == nil {
		log.Crit("Failed to create call tracer")
	}
	t.callTracer = callTracer
	t.callTracer.OnTxStart(vm, tx, from)
}

func (t *TxTracer) OnTxEnd(receipt *types.Receipt, err error) {
	t.callTracer.OnTxEnd(receipt, err)
	traceRes, _ := t.callTracer.GetResult()
	txtrace.GetTraceStore().WriteTxTrace(context.Background(), t.ctx.TxHash, traceRes)
	log.Debug("TxTrace store", "txHash", t.ctx.TxHash.Hex(), "blockNumber", t.ctx.BlockNumber)
	t.ctx.TxIndex += 1
	t.callTracer = nil
}

func (t *TxTracer) OnEnter(depth int, typ byte, from common.Address, to common.Address, input []byte, gas uint64, value *big.Int) {
	if t.callTracer == nil {
		return
	}
	t.callTracer.OnEnter(depth, typ, from, to, input, gas, value)
}

func (t *TxTracer) OnExit(depth int, output []byte, gasUsed uint64, err error, reverted bool) {
	if t.callTracer == nil {
		return
	}
	t.callTracer.OnExit(depth, output, gasUsed, err, reverted)
}
