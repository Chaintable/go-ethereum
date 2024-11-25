package live

import (
	"encoding/json"
	"fmt"
	"math/big"
	"strings"
	"sync"
	"time"

	"github.com/Chaintable/pipeline/processor"
	ptypes "github.com/Chaintable/pipeline/types"
	"github.com/Chaintable/pipeline/util"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/core/tracing"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/eth/tracers"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/params"
	"github.com/ethereum/go-ethereum/pipeline"
)

// 需要上传3种data
// 1. block
// 2. state diff
// 3. block file

func init() {
	tracers.LiveDirectory.Register("pipeline", newpipelineTracer)
}

type pipelineTracer struct {
	config     pipelineTracerConfig
	ctx        *tracers.Context
	callTracer *callTracer
}

type pipelineTracerConfig struct {
	Region           string   `json:"region"`
	NodeXBucket      string   `json:"node_x_bucket"`
	ChainTableBucket string   `json:"chain_table_bucket"`
	Brokers          []string `json:"brokers"`
	Topic            string   `json:"topic"`
	ChainID          string   `json:"chain_id"`
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
		OnOpcode:         t.OnOpcode,
		OnBalanceChange:  t.OnBalanceChange,
		OnGenesisBlock:   t.OnGenesisBlock,
		OnCommit:         t.OnCommit,
	}, nil
}

func (t *pipelineTracer) OnBlockchainInit(chainConfig *params.ChainConfig) {
	log.Info("Init pipeline with param", "chainConfig", chainConfig.ChainID.String(), "config", t.config)
	if t.config.ChainID == "" {
		log.Crit("ChainID is required")
	}
	err := pipeline.InitPipeline(t.config.Region, t.config.NodeXBucket, t.config.ChainTableBucket, t.config.Brokers, t.config.Topic, chainConfig.ChainID)
	if err != nil {
		log.Crit("Failed to init pipeline", "err", err)
	}
}

func (t *pipelineTracer) OnClose() {
	pipeline.NodeXPusher.Close()
}

func (t *pipelineTracer) BuildPipelineBlock(rawBlock *types.Block) ptypes.Block {
	block := ptypes.Block{
		ID:            rawBlock.Hash().Hex(),
		Height:        rawBlock.Number(),
		ParentID:      rawBlock.ParentHash().Hex(),
		BaseFeePerGas: big.NewInt(0),
		Miner:         strings.ToLower(rawBlock.Coinbase().Hex()),
		GasLimit:      big.NewInt(int64(rawBlock.GasLimit())),
		GasUsed:       big.NewInt(int64(rawBlock.GasUsed())),
		Timestamp:     rawBlock.Time(),
	}
	if rawBlock.Header().BaseFee != nil {
		block.BaseFeePerGas = rawBlock.Header().BaseFee
	}
	return block
}

func (t *pipelineTracer) BuildPipelineWithdrawals(rawBlock *types.Block) []ptypes.SpecialTransfer {
	res := make([]ptypes.SpecialTransfer, 0)
	for _, withdrawal := range rawBlock.Withdrawals() {
		specialTransfer := ptypes.SpecialTransfer{
			FromAddress: strings.ToLower("0x00000000219ab540356cBB839Cbe05303d7705Fa"), //eth2 合约
			ToAddress:   strings.ToLower(withdrawal.Address.Hex()),
			Value:       (*hexutil.Big)(big.NewInt(int64(withdrawal.Amount))),
			Memo:        "beacon_withdrawl",
			Idx:         big.NewInt(int64(withdrawal.Index)),
		}
		specialTransfer.ID = util.ToHash([]string{rawBlock.Hash().Hex(), specialTransfer.ToAddress, fmt.Sprintf("%d", withdrawal.Index)})
		res = append(res, specialTransfer)
	}

	return res
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
	return &blockHeader
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
	pipeline.PipelineCtx.BlockDiff = &ptypes.BlockStorageDiff{}
	pipeline.PipelineCtx.BlockHeader = t.BuildPilelineBlockHeader(event.Block)
	pipeline.PipelineCtx.BlockFile = &ptypes.BlockFile{
		Block:            t.BuildPipelineBlock(event.Block),
		SpecialTransfers: t.BuildPipelineWithdrawals(event.Block),
		Events:           make([]ptypes.Event, 0),
		Txs:              make([]ptypes.Transaction, 0),
		Traces:           make([]ptypes.Trace, 0),
	}
	pipeline.PipelineCtx.Tx = nil
	pipeline.PipelineCtx.From = common.Address{}

}

func (t *pipelineTracer) uploadBlockHeader(blockHeader *ptypes.Header) error {
	s3BlockFile, err := processor.SerializeHeader(t.config.ChainID, blockHeader)
	if err != nil {
		return fmt.Errorf("failed to serialize block header: %v", err)
	}
	err = pipeline.NodeXPusher.UploadFileToS3(s3BlockFile)
	if err != nil {
		return fmt.Errorf("failed to upload block header: %v", err)
	}
	return nil
}

func (t *pipelineTracer) uploadBlockDiff(blockDiff *ptypes.BlockStorageDiff) error {
	s3file, err := processor.SerializeStateDiff(t.config.ChainID, blockDiff)
	if err != nil {
		return fmt.Errorf("failed to serialize state diff: %v", err)
	}
	err = pipeline.NodeXPusher.UploadFileToS3(s3file)
	if err != nil {
		return fmt.Errorf("failed to upload state diff: %v", err)
	}
	return nil
}

func (t *pipelineTracer) uploadBlockFile(blockFile *ptypes.BlockFile) error {
	s3file, err := processor.SerializeFile(t.config.ChainID, blockFile)
	if err != nil {
		return fmt.Errorf("failed to serialize block file: %v", err)
	}
	err = pipeline.ChainTableBucketPusher.UploadFileToS3(s3file)
	if err != nil {
		return fmt.Errorf("failed to upload block file: %v", err)
	}
	return nil
}

func (t *pipelineTracer) uploadblockFileValidation(blockFile *ptypes.BlockFile) error {
	blockFileValidation, err := processor.SerializeFileValidation(t.config.ChainID, blockFile)
	if err != nil {
		return fmt.Errorf("failed to serialize block file validation: %v", err)
	}
	err = pipeline.ChainTableBucketPusher.UploadFileToS3(blockFileValidation)
	if err != nil {
		return fmt.Errorf("failed to upload block file validation: %v", err)
	}
	return nil
}

func (t *pipelineTracer) OnCommit() {
	var wg sync.WaitGroup
	var mu sync.Mutex
	var uploadErrs []error

	s3start := time.Now()

	// Helper function to handle errors safely
	handleError := func(err error) {
		mu.Lock()
		defer mu.Unlock()
		if err != nil {
			uploadErrs = append(uploadErrs, err)
		}
	}

	// 上传 block head
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := t.uploadBlockHeader(pipeline.PipelineCtx.BlockHeader)
		if err != nil {
			handleError(err)
			return
		}
	}()

	// 上传 state diff
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := t.uploadBlockDiff(pipeline.PipelineCtx.BlockDiff)
		if err != nil {
			handleError(err)
			return
		}
	}()

	// 上传 block file
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := t.uploadBlockFile(pipeline.PipelineCtx.BlockFile)
		if err != nil {
			handleError(err)
			return
		}
	}()

	// 上传 block file validation
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := t.uploadblockFileValidation(pipeline.PipelineCtx.BlockFile)
		if err != nil {
			handleError(err)
			return
		}
	}()

	// 等待所有上传完成
	wg.Wait()
	s3Elapsed := time.Since(s3start)

	// 检查是否有错误
	if len(uploadErrs) > 0 {
		for _, err := range uploadErrs {
			log.Error("Upload error", "err", err)
		}
		log.Crit("One or more uploads failed")
	}
	log.Info("Upload to s3", "elapsed", common.PrettyDuration(s3Elapsed))
}

func (t *pipelineTracer) OnBlockEnd(blockErr error) {

	// push block change notification
	if pipeline.PipelineCtx.BlockChange != nil {
		start := time.Now()
		pipeline.NodeXPusher.PushBlockChangeNotification(pipeline.PipelineCtx.BlockChange)
		log.Info("Push kafka", "dropBlocks", pipeline.PipelineCtx.BlockChange.DropBlocks, "newBlocks", pipeline.PipelineCtx.BlockChange.NewBlocks, "kafka elapsed", common.PrettyDuration(time.Since(start)))
	}
}

func (t *pipelineTracer) buildPipelineTransaction(tx *types.Transaction, receipt *types.Receipt, from common.Address) ptypes.Transaction {
	to := receipt.ContractAddress
	if tx.To() != nil {
		to = *tx.To()
	}
	gasPrice := receipt.EffectiveGasPrice
	if gasPrice == nil {
		gasPrice = tx.GasPrice()
	}
	transaction := ptypes.Transaction{
		ID:               tx.Hash().Hex(),
		From:             strings.ToLower(from.Hex()),
		To:               strings.ToLower(to.Hex()),
		Gas:              big.NewInt(int64(tx.Gas())),
		GasPrice:         gasPrice,
		GasUsed:          big.NewInt(int64(receipt.GasUsed)),
		Status:           receipt.Status == types.ReceiptStatusSuccessful,
		GasFeeCap:        common.Big0,
		GasTipCap:        common.Big0,
		Input:            tx.Data(),
		Nonce:            big.NewInt(int64(tx.Nonce())),
		TransactionIndex: int64(receipt.TransactionIndex),
		Value:            (*hexutil.Big)(tx.Value()),
	}
	switch tx.Type() {
	case types.DynamicFeeTxType:
		transaction.GasFeeCap = tx.GasFeeCap()
		transaction.GasTipCap = tx.GasTipCap()
	case types.BlobTxType:
		transaction.GasFeeCap = tx.GasFeeCap()
		transaction.GasTipCap = tx.GasTipCap()
	}
	return transaction
}

func (t *pipelineTracer) OnTxStart(vm *tracing.VMContext, tx *types.Transaction, from common.Address) {
	t.ctx.TxHash = tx.Hash()
	callTracer := newCallTracerRaw(t.ctx)
	t.callTracer = callTracer
	t.callTracer.OnTxStart(vm, tx, from)
	pipeline.PipelineCtx.Tx = tx
	pipeline.PipelineCtx.From = from
}

func (t *pipelineTracer) OnTxEnd(receipt *types.Receipt, err error) {
	t.callTracer.OnTxEnd(receipt, err)
	t.ctx.TxIndex += 1
	t.callTracer = nil

	tx := t.buildPipelineTransaction(pipeline.PipelineCtx.Tx, receipt, pipeline.PipelineCtx.From)
	pipeline.PipelineCtx.BlockFile.Txs = append(pipeline.PipelineCtx.BlockFile.Txs, tx)
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

func (t *pipelineTracer) OnOpcode(pc uint64, op byte, gas, cost uint64, scope tracing.OpContext, rData []byte, depth int, err error) {
	if t.callTracer == nil {
		return
	}
	t.callTracer.OnOpcode(pc, op, gas, cost, scope, rData, depth, err)
}

func (t *pipelineTracer) OnLog(log *types.Log) {
	if t.callTracer == nil {
		return
	}
	t.callTracer.OnLog(log)
}

func (t *pipelineTracer) OnBalanceChange(a common.Address, prevBalance, newBalance *big.Int, reason tracing.BalanceChangeReason) {
	diff := new(big.Int).Sub(newBalance, prevBalance)

	if reason == tracing.BalanceIncreaseRewardMineUncle || reason == tracing.BalanceIncreaseRewardMineBlock {
		for i := range pipeline.PipelineCtx.BlockFile.SpecialTransfers {
			sp := &pipeline.PipelineCtx.BlockFile.SpecialTransfers[i]
			if sp.ToAddress == strings.ToLower(a.Hex()) && sp.Memo == "block_reward" {
				sp.Value = (*hexutil.Big)(new(big.Int).Add(sp.Value.ToInt(), diff))
				return
			}
		}
		specialTransfer := ptypes.SpecialTransfer{
			FromAddress: common.Address{}.Hex(),
			ToAddress:   strings.ToLower(a.Hex()),
			Value:       (*hexutil.Big)(diff),
			Memo:        "block_reward",
			Idx:         big.NewInt(int64(reason)),
		}
		specialTransfer.ID = util.ToHash([]string{pipeline.PipelineCtx.BlockHash.Hex(), specialTransfer.ToAddress, fmt.Sprintf("%d", tracing.BalanceIncreaseRewardMineBlock)})
		pipeline.PipelineCtx.BlockFile.SpecialTransfers = append(pipeline.PipelineCtx.BlockFile.SpecialTransfers, specialTransfer)
	}
	if reason == tracing.BalanceIncreaseRewardTransactionFee {
		for i := range pipeline.PipelineCtx.BlockFile.SpecialTransfers {
			sp := &pipeline.PipelineCtx.BlockFile.SpecialTransfers[i]
			if sp.ToAddress == strings.ToLower(a.Hex()) && sp.Memo == "gasfee_reward" {
				sp.Value = (*hexutil.Big)(new(big.Int).Add(sp.Value.ToInt(), diff))
				return
			}
		}
		specialTransfer := ptypes.SpecialTransfer{
			FromAddress: common.Address{}.Hex(),
			ToAddress:   strings.ToLower(a.Hex()),
			Value:       (*hexutil.Big)(diff),
			Memo:        "gasfee_reward",
			Idx:         big.NewInt(int64(reason)),
		}
		specialTransfer.ID = util.ToHash([]string{pipeline.PipelineCtx.BlockHash.Hex(), specialTransfer.ToAddress, fmt.Sprintf("%d", tracing.BalanceIncreaseRewardTransactionFee)})
		pipeline.PipelineCtx.BlockFile.SpecialTransfers = append(pipeline.PipelineCtx.BlockFile.SpecialTransfers, specialTransfer)
	}
}

func (t *pipelineTracer) OnGenesisBlock(block *types.Block, alloc types.GenesisAlloc) {
	if pipeline.NodeXPusher.LastBlockNotice != nil {
		return
	}

	// 内部s3
	header := t.BuildPilelineBlockHeader(block)
	err := t.uploadBlockHeader(header)
	if err != nil {
		log.Crit("Failed to upload block", "err", err)
	}
	log.Info("[inner s3] 1.upload genesis block", "block hash", block.Hash().Hex(), "block number", block.Number().Uint64())

	blockdiff := pipeline.GenesisAllocToStateDiff(alloc)
	blockdiff.Hash = block.Root()
	// genesis block has no parent
	blockdiff.ParentHash = types.EmptyRootHash
	err = t.uploadBlockDiff(blockdiff)
	if err != nil {
		log.Crit("Failed to upload block diff files to s3", "err", err)
	}
	log.Info("[inner s3] 2.upload genesis state diff", "block", block.Hash().Hex())

	// 业务s3
	blockFile := &ptypes.BlockFile{
		Block: t.BuildPipelineBlock(block),
	}
	for addr, acc := range alloc {
		if acc.Balance.Cmp(big.NewInt(0)) > 0 {
			specialTransfer := ptypes.SpecialTransfer{
				FromAddress: common.Address{}.Hex(),
				ToAddress:   strings.ToLower(addr.Hex()),
				Value:       (*hexutil.Big)(acc.Balance),
				Memo:        "genesis",
				Idx:         big.NewInt(0),
			}
			specialTransfer.ID = util.ToHash([]string{block.Hash().Hex(), specialTransfer.ToAddress, fmt.Sprintf("%d", specialTransfer.Idx)})
			blockFile.SpecialTransfers = append(blockFile.SpecialTransfers, specialTransfer)
		}
	}
	// upload block file and meta data
	err = t.uploadBlockFile(blockFile)
	if err != nil {
		log.Crit("Failed to upload block files to s3", "err", err)
	}
	log.Info("3.upload block file", "block hash", header.Hash.Hex(), "block number", header.Number.ToInt().Uint64())

	// upload block file validation
	err = t.uploadblockFileValidation(blockFile)
	if err != nil {
		log.Crit("Failed to upload file validation to s3", "err", err)
	}
	log.Info("4.upload block file validation", "block hash", header.Hash.Hex(), "block number", header.Number.ToInt().Uint64())

	// push block change notification
	blockChanges := &ptypes.BlockChangeNotification{
		ChangeType: 1,
		NewBlocks: []ptypes.BlockContext{
			{
				Hash:        block.Hash(),
				ParentHash:  block.ParentHash(),
				BlockNumber: block.NumberU64(),
				Timestamp:   block.Time(),
			},
		},
	}

	err = pipeline.NodeXPusher.PushBlockChangeNotification(blockChanges)
	if err != nil {
		log.Crit("Failed to push block change notification", "err", err)
	}

	log.Info("push genesis block change notification", "block hash", block.Hash().Hex(), "block number", block.Number().Uint64())
}
