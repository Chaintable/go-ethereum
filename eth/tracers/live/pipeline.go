package live

import (
	"encoding/json"
	"fmt"
	"math/big"
	"time"

	"github.com/Chaintable/pipeline/processor"
	ptypes "github.com/Chaintable/pipeline/types"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/core/tracing"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/eth/tracers"
	"github.com/ethereum/go-ethereum/eth/tracers/native"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/params"
	"github.com/ethereum/go-ethereum/pipeline"
)

// 需要上传6种data
// 在生成时就上传
// 1. block
// 2. transaction
// 3. event
// 4. receipt
// 5. trace
// 6. state diff

func init() {
	tracers.LiveDirectory.Register("pipeline", newpipelineTracer)
}

type pipelineTracer struct {
	config     pipelineTracerConfig
	ctx        *tracers.Context
	callTracer *tracers.Tracer
}

type pipelineTracerConfig struct {
	ExtraInfoPath string   `json:"extra_info_path"`
	Region        string   `json:"region"`
	Bucket        string   `json:"bucket"`
	Brokers       []string `json:"brokers"`
	Topic         string   `json:"topic"`
}

func newpipelineTracer(cfg json.RawMessage) (*tracing.Hooks, error) {
	var config pipelineTracerConfig
	if cfg != nil {
		if err := json.Unmarshal(cfg, &config); err != nil {
			return nil, fmt.Errorf("failed to parse config: %v", err)
		}
	}
	t := &pipelineTracer{
		config: config,
	}

	return &tracing.Hooks{
		OnBlockchainInit: t.OnBlockchainInit,
		OnClose:          t.OnClose,
		OnBlockStart:     t.OnBlockStart,
		OnBlockEnd:       t.OnBlockEnd,
		OnTxStart:        t.OnTxStart,
		OnTxEnd:          t.OnTxEnd,
		OnEnter:          t.OnEnter,
		OnExit:           t.OnExit,
		OnLog:            t.OnLog,
		OnGenesisBlock:   t.OnGenesisBlock,
	}, nil
}

func (t *pipelineTracer) OnBlockchainInit(chainConfig *params.ChainConfig) {
	if t.config.ExtraInfoPath == "" {
		t.config.ExtraInfoPath = "/var/data/extraInfodb"
	}
	log.Info("Init pipeline with param", "chainID", chainConfig.ChainID.String())
	err := pipeline.InitPipeline(t.config.ExtraInfoPath, t.config.Region, t.config.Bucket, t.config.Brokers, t.config.Topic, chainConfig.ChainID)
	if err != nil {
		log.Crit("Failed to init pipeline", "err", err)
	}
}

func (t *pipelineTracer) OnClose() {
	pipeline.ExtraInfoStore.Close()
	pipeline.Pusher.Close()
}

func (t *pipelineTracer) BuildPilelineBlockHeader(block *types.Block) *ptypes.Header {
	blockHeader := ptypes.Header{
		Number:           (*hexutil.Big)(block.Number()),
		Hash:             block.Hash(),
		ParentHash:       block.ParentHash(),
		Nonce:            block.Header().Nonce,
		MixHash:          block.MixDigest(),
		Sha3Uncles:       block.UncleHash(),
		LogsBloom:        block.Bloom(),
		StateRoot:        block.Root(),
		Miner:            block.Coinbase(),
		Difficulty:       (*hexutil.Big)(block.Difficulty()),
		ExtraData:        hexutil.Bytes(block.Extra()),
		GasLimit:         hexutil.Uint64(block.GasLimit()),
		GasUsed:          hexutil.Uint64(block.GasUsed()),
		Timestamp:        hexutil.Uint64(block.Time()),
		TransactionsRoot: block.TxHash(),
		ReceiptsRoot:     block.ReceiptHash(),
	}
	if block.Header().BaseFee != nil {
		blockHeader.BaseFeePerGas = (*hexutil.Big)(block.Header().BaseFee)
	}
	if block.Header().WithdrawalsHash != nil {
		blockHeader.WithdrawalsRoot = block.Header().WithdrawalsHash
	}
	if block.Header().BlobGasUsed != nil {
		blockHeader.BlobGasUsed = (*hexutil.Uint64)(block.Header().BlobGasUsed)
	}
	if block.Header().ExcessBlobGas != nil {
		blockHeader.ExcessBlobGas = (*hexutil.Uint64)(block.Header().ExcessBlobGas)
	}
	if block.Header().ParentBeaconRoot != nil {
		blockHeader.ParentBeaconBlockRoot = block.Header().ParentBeaconRoot
	}
	if block.Header().RequestsHash != nil {
		blockHeader.RequestsRoot = block.Header().RequestsHash
	}
	uncles := block.Uncles()
	uncleHashes := make([]common.Hash, len(uncles))
	for i, uncle := range uncles {
		uncleHashes[i] = uncle.Hash()
	}
	txs := block.Transactions()
	txhashes := make([]common.Hash, len(txs))
	for i, tx := range txs {
		txhashes[i] = tx.Hash()
	}
	//pblock := ptypes.Block{
	//	Header:      blockHeader,
	//	Size:        hexutil.Uint64(block.Size()),
	//	Uncles:      uncleHashes,
	//	Withdrawals: block.Withdrawals(),
	//	Requests:    block.Requests(),
	//}
	return &blockHeader
}

func (t *pipelineTracer) uploadBlockHeader(blockHeader *ptypes.Header) error {
	s3BlockFile, err := processor.SerializeHeader((*hexutil.Big)(pipeline.ChainID), blockHeader)
	if err != nil {
		return err
	}
	err = pipeline.Pusher.UploadFileToS3(s3BlockFile)
	if err != nil {
		return err
	}
	return nil
}

func (t *pipelineTracer) OnBlockStart(event tracing.BlockEvent) {
	t.ctx = &tracers.Context{
		BlockNumber: event.Block.Number(),
		BlockHash:   event.Block.Hash(),
	}
	pipeline.PipelineCtx = &pipeline.ExtraInfo{
		BlockNumber: event.Block.Number().Uint64(),
		BlockHash:   event.Block.Hash(),
	}
	pipeline.PipelineCtx.Traces = make([]ptypes.Trace, 0)
	pipeline.PipelineCtx.EventPositions = make([]ptypes.Event, 0)
	totalEventCount, err := pipeline.ExtraInfoStore.GetBlockEventCount(pipeline.PipelineCtx.BlockHash)
	if err != nil {
		log.Crit("Failed to get block event count", "err", err)
	}
	pipeline.PipelineCtx.TotalEventCount = totalEventCount
	pipeline.PipelineCtx.BlockDiff = &ptypes.BlockStorageDiff{}
	pipeline.PipelineCtx.BlockHeader = t.BuildPilelineBlockHeader(event.Block)
	err = t.uploadBlockHeader(pipeline.PipelineCtx.BlockHeader)
	if err != nil {
		log.Crit("Failed to upload block", "err", err)
	}
	log.Info("1.upload block", "block hash", event.Block.Hash().Hex(), "block number", event.Block.Number().Uint64())
}

func (t *pipelineTracer) OnBlockEnd(blockErr error) {
	pipeline.PipelineCtx.TotalEventCount += uint64(len(pipeline.PipelineCtx.EventPositions))

	// 记录block event count
	pipeline.ExtraInfoStore.WriteBlockEventCount(pipeline.PipelineCtx.BlockHash, pipeline.PipelineCtx.TotalEventCount)

	// 上传state diff
	s3file, err := processor.SerializeStateDiff((*hexutil.Big)(pipeline.ChainID), pipeline.PipelineCtx.BlockDiff)
	if err != nil {
		log.Crit("Failed to serialize state diff", "err", err)
	}
	err = pipeline.Pusher.UploadFileToS3(s3file)
	if err != nil {
		log.Crit("Failed to upload files to s3", "err", err)
	}
	log.Info("6.upload state diff", "block", pipeline.PipelineCtx.BlockHash.Hex())
	if pipeline.PipelineCtx.BlockChange != nil {
		start := time.Now()
		pipeline.Pusher.PushBlockChangeNotification(pipeline.PipelineCtx.BlockChange)
		log.Info("Push kafka", "dropBlocks", pipeline.PipelineCtx.BlockChange.DropBlocks, "newBlocks", pipeline.PipelineCtx.BlockChange.NewBlocks, "elapsed", common.PrettyDuration(time.Since(start)))
	}
}

//func (t *pipelineTracer) buildPipelineTransaction(tx *types.Transaction, blockHash common.Hash, blockNumber uint64, index uint64, baseFee *big.Int, from common.Address) *ptypes.Transaction {
//	v, r, s := tx.RawSignatureValues()
//	result := &ptypes.Transaction{
//		Type:     hexutil.Uint64(tx.Type()),
//		From:     from,
//		Gas:      hexutil.Uint64(tx.Gas()),
//		GasPrice: (*hexutil.Big)(tx.GasPrice()),
//		Hash:     tx.Hash(),
//		Input:    hexutil.Bytes(tx.Data()),
//		Nonce:    hexutil.Uint64(tx.Nonce()),
//		To:       tx.To(),
//		Value:    (*hexutil.Big)(tx.Value()),
//		V:        (*hexutil.Big)(v),
//		R:        (*hexutil.Big)(r),
//		S:        (*hexutil.Big)(s),
//	}
//	if blockHash != (common.Hash{}) {
//		result.BlockHash = blockHash
//		result.BlockNumber = (*hexutil.Big)(new(big.Int).SetUint64(blockNumber))
//		result.TransactionIndex = (*hexutil.Uint64)(&index)
//	}
//
//	switch tx.Type() {
//	case types.LegacyTxType:
//		// if a legacy transaction has an EIP-155 chain id, include it explicitly
//		if id := tx.ChainId(); id.Sign() != 0 {
//			result.ChainID = (*hexutil.Big)(id)
//		}
//
//	case types.AccessListTxType:
//		al := tx.AccessList()
//		yparity := hexutil.Uint64(v.Sign())
//		result.Accesses = &al
//		result.ChainID = (*hexutil.Big)(tx.ChainId())
//		result.YParity = &yparity
//
//	case types.DynamicFeeTxType:
//		al := tx.AccessList()
//		yparity := hexutil.Uint64(v.Sign())
//		result.Accesses = &al
//		result.ChainID = (*hexutil.Big)(tx.ChainId())
//		result.YParity = &yparity
//		result.GasFeeCap = (*hexutil.Big)(tx.GasFeeCap())
//		result.GasTipCap = (*hexutil.Big)(tx.GasTipCap())
//		// if the transaction has been mined, compute the effective gas price
//		if baseFee != nil && blockHash != (common.Hash{}) {
//			// price = min(gasTipCap + baseFee, gasFeeCap)
//			result.GasPrice = (*hexutil.Big)(effectiveGasPrice(tx, baseFee))
//		} else {
//			result.GasPrice = (*hexutil.Big)(tx.GasFeeCap())
//		}
//
//	case types.BlobTxType:
//		al := tx.AccessList()
//		yparity := hexutil.Uint64(v.Sign())
//		result.Accesses = &al
//		result.ChainID = (*hexutil.Big)(tx.ChainId())
//		result.YParity = &yparity
//		result.GasFeeCap = (*hexutil.Big)(tx.GasFeeCap())
//		result.GasTipCap = (*hexutil.Big)(tx.GasTipCap())
//		// if the transaction has been mined, compute the effective gas price
//		if baseFee != nil && blockHash != (common.Hash{}) {
//			result.GasPrice = (*hexutil.Big)(effectiveGasPrice(tx, baseFee))
//		} else {
//			result.GasPrice = (*hexutil.Big)(tx.GasFeeCap())
//		}
//		result.MaxFeePerBlobGas = (*hexutil.Big)(tx.BlobGasFeeCap())
//		result.BlobVersionedHashes = tx.BlobHashes()
//	}
//	return result
//}

func effectiveGasPrice(tx *types.Transaction, baseFee *big.Int) *big.Int {
	fee := tx.GasTipCap()
	fee = fee.Add(fee, baseFee)
	if tx.GasFeeCapIntCmp(fee) < 0 {
		return tx.GasFeeCap()
	}
	return fee
}

func (t *pipelineTracer) uploadTransaction(ptx *ptypes.Transaction) error {
	//s3file, err := processor.SerializeTransaction((*hexutil.Big)(pipeline.ChainID), ptx)
	//if err != nil {
	//	return err
	//}
	//err = pipeline.Pusher.UploadFileToS3(s3file)
	//if err != nil {
	//	return err
	//}
	return nil
}

func (t *pipelineTracer) OnTxStart(vm *tracing.VMContext, tx *types.Transaction, from common.Address) {
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
	//pipeline.PipelineCtx.Tx = t.buildPipelineTransaction(tx, t.ctx.BlockHash, t.ctx.BlockNumber.Uint64(), uint64(t.ctx.TxIndex), (*big.Int)(pipeline.PipelineCtx.Block.BaseFeePerGas), from)
	//err = t.uploadTransaction(pipeline.PipelineCtx.Tx)
	//if err != nil {
	//	log.Crit("Failed to upload transaction", "err", err)
	//}
	//log.Info("2.upload transaction", "tx hash", tx.Hash().Hex(), "tx index", t.ctx.TxIndex)
}

//func (t *pipelineTracer) uploadReceiptAndLog(receipt *types.Receipt) error {
//	tx := pipeline.PipelineCtx.Tx
//	pReceipt := &ptypes.Receipt{
//		BlockHash:         receipt.BlockHash,
//		BlockNumber:       (hexutil.Uint64)(receipt.BlockNumber.Uint64()),
//		TransactionHash:   receipt.TxHash,
//		TransactionIndex:  (hexutil.Uint64)(receipt.TransactionIndex),
//		From:              tx.From,
//		To:                tx.To,
//		GasUsed:           (hexutil.Uint64)(receipt.GasUsed),
//		CumulativeGasUsed: (hexutil.Uint64)(receipt.CumulativeGasUsed),
//		LogsBloom:         receipt.Bloom,
//		EffectiveGasPrice: (*hexutil.Big)(receipt.EffectiveGasPrice),
//		Type:              (hexutil.Uint)(receipt.Type),
//	}
//	if len(receipt.PostState) > 0 {
//		root := hexutil.Bytes(receipt.PostState)
//		pReceipt.Root = &root
//	} else {
//		status := (hexutil.Uint)(receipt.Status)
//		pReceipt.Status = &status
//	}
//	if tx.Type == types.BlobTxType {
//		blobGasUsed := (hexutil.Uint64)(receipt.BlobGasUsed)
//		pReceipt.BlobGasUsed = &blobGasUsed
//		blobGasPrice := (*hexutil.Big)(receipt.BlobGasPrice)
//		pReceipt.BlobGasPrice = blobGasPrice
//	}
//
//	if receipt.ContractAddress != (common.Address{}) {
//		pReceipt.ContractAddress = &receipt.ContractAddress
//	}
//
//	s3files := make([]*processor.DataFile, 0)
//	s3file, err := processor.SerializeReceipt((*hexutil.Big)(pipeline.ChainID), pReceipt)
//	if err != nil {
//		return err
//	}
//	s3files = append(s3files, s3file)
//
//	for _, log := range receipt.Logs {
//		pos := pipeline.GetLogTraceContextByIndex(uint(log.Index))
//		event := &ptypes.Event{
//			Address:     log.Address,
//			Topics:      log.Topics,
//			Data:        log.Data,
//			BlockNumber: (hexutil.Uint64)(receipt.BlockNumber.Uint64()),
//			BlockHash:   receipt.BlockHash,
//			TxHash:      receipt.TxHash,
//			TxIndex:     (hexutil.Uint)(receipt.TransactionIndex),
//			Index:       (hexutil.Uint)(log.Index),
//			Removed:     false,
//
//			TraceAddress: pos.TraceAddress,
//			Position:     pos.Position,
//			GlobalIndex:  (hexutil.Uint)(pos.GlobalIndex),
//		}
//		s3file, err := processor.SerializeEvent((*hexutil.Big)(pipeline.ChainID), event)
//		if err != nil {
//			return err
//		}
//		s3files = append(s3files, s3file)
//	}
//
//	return pipeline.Pusher.UploadFilesToS3(s3files)
//}

func (t *pipelineTracer) OnTxEnd(receipt *types.Receipt, err error) {
	t.callTracer.OnTxEnd(receipt, err)
	t.callTracer.GetResult() // ignore the result
	t.ctx.TxIndex += 1
	t.callTracer = nil

	//if err := t.uploadReceiptAndLog(receipt); err != nil {
	//	log.Crit("Failed to upload receipt and log", "err", err)
	//}
	//log.Info("3-4.upload receipt and log", "tx hash", receipt.TxHash.Hex())
	//
	//if len(pipeline.PipelineCtx.Traces) > 0 {
	//	s3files := make([]*processor.DataFile, 0)
	//	for _, trace := range pipeline.PipelineCtx.Traces {
	//		s3file, err := processor.SerializeTrace((*hexutil.Big)(pipeline.ChainID), trace)
	//		if err != nil {
	//			log.Crit("Failed to serialize trace", "err", err)
	//		}
	//		s3files = append(s3files, s3file)
	//	}
	//	err := pipeline.Pusher.UploadFilesToS3(s3files)
	//	if err != nil {
	//		log.Crit("Failed to upload trace", "err", err)
	//	}
	//	log.Info("5.upload trace", "tx hash", receipt.TxHash.Hex(), "trace count", len(pipeline.PipelineCtx.Traces))
	//}

}

func (t *pipelineTracer) OnEnter(depth int, typ byte, from common.Address, to common.Address, input []byte, gas uint64, value *big.Int) {
	if t.callTracer == nil {
		return
	}
	t.callTracer.OnEnter(depth, typ, from, to, input, gas, value)
}

func (t *pipelineTracer) OnExit(depth int, output []byte, gasUsed uint64, err error, reverted bool) {
	if t.callTracer == nil {
		return
	}
	t.callTracer.OnExit(depth, output, gasUsed, err, reverted)
}

func (t *pipelineTracer) OnLog(log *types.Log) {
	if t.callTracer == nil {
		return
	}
	t.callTracer.OnLog(log)
}

func (t *pipelineTracer) OnGenesisBlock(block *types.Block, alloc types.GenesisAlloc) {
	if pipeline.Pusher.LastBlockNotice != nil {
		return
	}
	header := t.BuildPilelineBlockHeader(block)
	err := t.uploadBlockHeader(header)
	if err != nil {
		log.Crit("Failed to upload block", "err", err)
	}
	log.Info("1.upload genesis block", "block hash", block.Hash().Hex(), "block number", block.Number().Uint64())

	blockdiff := pipeline.GenesisAllocToStateDiff(alloc)
	blockdiff.Hash = block.Root()
	// genesis block has no parent
	blockdiff.ParentHash = types.EmptyRootHash
	s3file, err := processor.SerializeStateDiff((*hexutil.Big)(pipeline.ChainID), blockdiff)
	if err != nil {
		log.Crit("Failed to serialize state diff", "err", err)
	}
	err = pipeline.Pusher.UploadFileToS3(s3file)
	if err != nil {
		log.Crit("Failed to upload files to s3", "err", err)
	}
	log.Info("6.upload genesis state diff", "block", block.Hash().Hex())

	blockChanges := &ptypes.BlockChangeNotification{
		ChangeType: 1,
		NewBlocks: []ptypes.BlockContext{
			{
				Hash:        block.Hash(),
				ParentHash:  block.ParentHash(),
				BlockNumber: block.NumberU64(),
			},
		},
	}

	err = pipeline.Pusher.PushBlockChangeNotification(blockChanges)
	if err != nil {
		log.Crit("Failed to push block change notification", "err", err)
	}

	log.Info("push genesis block change notification", "block hash", block.Hash().Hex(), "block number", block.Number().Uint64())

}
