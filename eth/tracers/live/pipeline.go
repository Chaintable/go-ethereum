package live

import (
	"encoding/json"
	"fmt"
	"math/big"
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
// 在生成时就上传
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
		BaseFeePerGas: rawBlock.BaseFee(),
		Miner:         rawBlock.Coinbase().Hex(),
		GasLimit:      big.NewInt(int64(rawBlock.GasLimit())),
		GasUsed:       big.NewInt(int64(rawBlock.GasUsed())),
		Timestamp:     rawBlock.Time(),
	}
	return block
}

func (t *pipelineTracer) BuildPipelineWithdrawals(rawBlock *types.Block) []ptypes.SpecialTransfer {
	res := make([]ptypes.SpecialTransfer, 0)
	for _, withdrawal := range rawBlock.Withdrawals() {
		specialTransfer := ptypes.SpecialTransfer{
			FromAddress: "0x00000000219ab540356cBB839Cbe05303d7705Fa", //eth2 合约
			ToAddress:   withdrawal.Address.Hex(),
			Value:       (*hexutil.Big)(big.NewInt(int64(withdrawal.Amount))),
			Memo:        "beacon_withdrawl",
			Idx:         big.NewInt(int64(withdrawal.Index)),
		}
		specialTransfer.ID = util.ToHash([]string{rawBlock.Hash().Hex(), withdrawal.Address.Hex(), fmt.Sprintf("%d", withdrawal.Index)})
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
	return &blockHeader
}

func (t *pipelineTracer) uploadBlockHeader(blockHeader *ptypes.Header) error {
	s3BlockFile, err := processor.SerializeHeader(t.config.ChainID, blockHeader)
	if err != nil {
		return err
	}
	err = pipeline.NodeXPusher.UploadFileToS3(s3BlockFile)
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
	pipeline.PipelineCtx.BlockDiff = &ptypes.BlockStorageDiff{}
	pipeline.PipelineCtx.BlockHeader = t.BuildPilelineBlockHeader(event.Block)
	pipeline.PipelineCtx.BlockFile = &ptypes.BlockFile{
		Block:            t.BuildPipelineBlock(event.Block),
		SpecialTransfers: t.BuildPipelineWithdrawals(event.Block),
	}
	pipeline.PipelineCtx.Tx = nil
	pipeline.PipelineCtx.From = common.Address{}

	err := t.uploadBlockHeader(pipeline.PipelineCtx.BlockHeader)
	if err != nil {
		log.Crit("Failed to upload block", "err", err)
	}
	log.Info("1.upload block", "block hash", event.Block.Hash().Hex(), "block number", event.Block.Number().Uint64())
}

func (t *pipelineTracer) OnBlockEnd(blockErr error) {
	// 上传state diff
	s3file, err := processor.SerializeStateDiff(t.config.ChainID, pipeline.PipelineCtx.BlockDiff)
	if err != nil {
		log.Crit("Failed to serialize state diff", "err", err)
	}
	err = pipeline.NodeXPusher.UploadFileToS3(s3file)
	if err != nil {
		log.Crit("Failed to upload files to s3", "err", err)
	}
	log.Info("2.upload state diff", "block", pipeline.PipelineCtx.BlockHash.Hex())

	// 上传block file和meta到业务s3
	blockFile, err := processor.SerializeFile(t.config.ChainID, pipeline.PipelineCtx.BlockFile)
	if err != nil {
		log.Crit("Failed to serialize block file", "err", err)
	}
	err = pipeline.ChainTableBucketPusher.UploadFileToS3(blockFile)
	if err != nil {
		log.Crit("Failed to upload files to s3", "err", err)
	}
	log.Info("3.upload block file", "block hash", pipeline.PipelineCtx.BlockHash.Hex(), "block number", pipeline.PipelineCtx.BlockNumber)

	// 上传block file validation
	blockFileValidation, err := processor.SerializeFileValidation(t.config.ChainID, pipeline.PipelineCtx.BlockFile)
	if err != nil {
		log.Crit("Failed to serialize block file validation", "err", err)
	}
	err = pipeline.ChainTableBucketPusher.UploadFileToS3(blockFileValidation)
	if err != nil {
		log.Crit("Failed to upload files to s3", "err", err)
	}
	log.Info("4.upload block file validation", "block hash", pipeline.PipelineCtx.BlockHash.Hex(), "block number", pipeline.PipelineCtx.BlockNumber)

	// push block change notification
	if pipeline.PipelineCtx.BlockChange != nil {
		start := time.Now()
		pipeline.NodeXPusher.PushBlockChangeNotification(pipeline.PipelineCtx.BlockChange)
		log.Info("Push kafka", "dropBlocks", pipeline.PipelineCtx.BlockChange.DropBlocks, "newBlocks", pipeline.PipelineCtx.BlockChange.NewBlocks, "elapsed", common.PrettyDuration(time.Since(start)))
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
		From:             from.Hex(),
		To:               to.Hex(),
		Gas:              big.NewInt(int64(tx.Gas())),
		GasPrice:         gasPrice,
		GasUsed:          big.NewInt(int64(receipt.GasUsed)),
		Status:           receipt.Status == types.ReceiptStatusSuccessful,
		GasFeeCap:        tx.GasFeeCap(),
		GasTipCap:        tx.GasTipCap(),
		Input:            tx.Data(),
		Nonce:            big.NewInt(int64(tx.Nonce())),
		TransactionIndex: int64(receipt.TransactionIndex),
		Value:            (*hexutil.Big)(tx.Value()),
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

func (t *pipelineTracer) OnBalanceChange(a common.Address, prev, new *big.Int, reason tracing.BalanceChangeReason) {
	if reason == tracing.BalanceIncreaseRewardMineUncle || reason == tracing.BalanceIncreaseRewardMineBlock {
		specialTransfer := ptypes.SpecialTransfer{
			FromAddress: common.Address{}.Hex(),
			ToAddress:   a.Hex(),
			Value:       (*hexutil.Big)(new),
			Memo:        "block_reward",
			Idx:         big.NewInt(int64(reason)),
		}
		specialTransfer.ID = util.ToHash([]string{pipeline.PipelineCtx.BlockHash.Hex(), a.Hex(), fmt.Sprintf("%d", reason)})
		pipeline.PipelineCtx.BlockFile.SpecialTransfers = append(pipeline.PipelineCtx.BlockFile.SpecialTransfers, specialTransfer)
	}
	if reason == tracing.BalanceIncreaseRewardTransactionFee {
		specialTransfer := ptypes.SpecialTransfer{
			FromAddress: common.Address{}.Hex(),
			ToAddress:   a.Hex(),
			Value:       (*hexutil.Big)(new),
			Memo:        "gasfee_reward",
			Idx:         big.NewInt(int64(reason)),
		}
		specialTransfer.ID = util.ToHash([]string{pipeline.PipelineCtx.BlockHash.Hex(), a.Hex(), fmt.Sprintf("%d", reason)})
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
	s3file, err := processor.SerializeStateDiff(t.config.ChainID, blockdiff)
	if err != nil {
		log.Crit("Failed to serialize state diff", "err", err)
	}
	err = pipeline.NodeXPusher.UploadFileToS3(s3file)
	if err != nil {
		log.Crit("Failed to upload files to s3", "err", err)
	}
	log.Info("inner s3] 2.upload genesis state diff", "block", block.Hash().Hex())

	// 业务s3
	blockFile := &ptypes.BlockFile{
		Block: t.BuildPipelineBlock(block),
	}
	for addr, acc := range alloc {
		if acc.Balance.Cmp(big.NewInt(0)) > 0 {
			specialTransfer := ptypes.SpecialTransfer{
				FromAddress: common.Address{}.Hex(),
				ToAddress:   addr.Hex(),
				Value:       (*hexutil.Big)(acc.Balance),
				Memo:        "genesis",
				Idx:         big.NewInt(0),
			}
			blockFile.SpecialTransfers = append(blockFile.SpecialTransfers, specialTransfer)
		}
	}
	// upload block file and meta data
	blockDataFile, err := processor.SerializeFile(t.config.ChainID, blockFile)
	if err != nil {
		log.Crit("Failed to serialize block file", "err", err)
	}
	err = pipeline.ChainTableBucketPusher.UploadFileToS3(blockDataFile)
	if err != nil {
		log.Crit("Failed to upload files to s3", "err", err)
	}
	log.Info("3.upload block file", "block hash", header.Hash.Hex(), "block number", header.Number.ToInt().Uint64())

	// 上传block file validation
	blockFileValidation, err := processor.SerializeFileValidation(t.config.ChainID, blockFile)
	if err != nil {
		log.Crit("Failed to serialize block file validation", "err", err)
	}
	err = pipeline.ChainTableBucketPusher.UploadFileToS3(blockFileValidation)
	if err != nil {
		log.Crit("Failed to upload files to s3", "err", err)
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
			},
		},
	}

	err = pipeline.NodeXPusher.PushBlockChangeNotification(blockChanges)
	if err != nil {
		log.Crit("Failed to push block change notification", "err", err)
	}

	log.Info("push genesis block change notification", "block hash", block.Hash().Hex(), "block number", block.Number().Uint64())
}
