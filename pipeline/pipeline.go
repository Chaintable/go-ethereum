package pipeline

import (
	"math/big"

	"github.com/Chaintable/pipeline/processor"
	ptypes "github.com/Chaintable/pipeline/types"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/holiman/uint256"
)

type ExtraInfo struct {
	BlockNumber uint64
	BlockHash   common.Hash
	BlockFile   *ptypes.BlockFile
	Tx          *types.Transaction
	From        common.Address
	BlockHeader *ptypes.Header
	BlockDiff   *ptypes.BlockStorageDiff
	BlockChange *ptypes.BlockChangeNotification
}

var (
	Pusher      *processor.PushProcessor
	PipelineCtx *ExtraInfo
)

func InitPipeline(extraInfoPath string, region string, bucket string, brokers []string, topic string, chainID *big.Int) (err error) {
	Pusher, err = processor.NewPushProcessor(region, bucket, brokers, topic)
	if err != nil {
		return err
	}
	return nil
}

func GenesisAllocToStateDiff(genesisAlloc types.GenesisAlloc) *ptypes.BlockStorageDiff {
	diff := &ptypes.BlockStorageDiff{}
	diff.NewAccounts = make([]ptypes.NewAccount, 0)
	diff.NewCodes = make([]ptypes.NewCode, 0)
	diff.StorageDiff = make([]ptypes.AccountStorageDiff, 0)
	diff.DeletedAccounts = make([]common.Hash, 0)
	for addr, acc := range genesisAlloc {
		diff.NewAccounts = append(diff.NewAccounts, ptypes.NewAccount{
			Address:  crypto.Keccak256Hash(addr[:]),
			Balance:  uint256.MustFromBig(acc.Balance),
			Nonce:    uint64(acc.Nonce),
			CodeHash: common.BytesToHash(acc.Code),
		})
		if len(acc.Code) > 0 {
			diff.NewCodes = append(diff.NewCodes, ptypes.NewCode{
				CodeHash: common.BytesToHash(acc.Code),
				Code:     acc.Code,
			})
		}
		values := make([]ptypes.IndexValuePair, 0)
		for index, v := range acc.Storage {
			value := uint256.NewInt(0)
			if len(v) > 0 {
				value = uint256.NewInt(0).SetBytes(v.Bytes())
			}
			values = append(values, ptypes.IndexValuePair{
				Index: index,
				Value: value,
			})
		}
		diff.StorageDiff = append(diff.StorageDiff, ptypes.AccountStorageDiff{
			Address: crypto.Keccak256Hash(addr[:]),
			Values:  values,
		})
	}
	return diff
}
