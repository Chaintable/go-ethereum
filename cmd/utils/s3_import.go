package utils

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"path"
	"syscall"

	ptypes "github.com/Chaintable/pipeline/types"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/rlp"
)

func NewS3Client(region string) (*s3.Client, error) {
	cfg, err := config.LoadDefaultConfig(context.TODO())
	if err != nil {
		return nil, err
	}
	cfg.Region = region
	client := s3.NewFromConfig(cfg)
	return client, nil
}

func ImportChainFromS3(chain *core.BlockChain, blockHeightBucket string, blockBucket string, endHeight int64, region string) error {
	// Watch for Ctrl-C while the import is running.
	// If a signal is received, the import will stop at the next batch.
	interrupt := make(chan os.Signal, 1)
	stop := make(chan struct{})
	signal.Notify(interrupt, syscall.SIGINT, syscall.SIGTERM)
	defer signal.Stop(interrupt)
	defer close(interrupt)
	go func() {
		if _, ok := <-interrupt; ok {
			log.Info("Interrupted during import, stopping at next batch")
		}
		close(stop)
	}()
	checkInterrupt := func() bool {
		select {
		case <-stop:
			return true
		default:
			return false
		}
	}

	log.Info("Importing blockchain")

	header := chain.CurrentBlock()

	if endHeight <= header.Number.Int64() {
		return errors.New("end height is less than current height")
	}

	importCh := make(chan *BlockImnport, 100)

	s3Client, err := NewS3Client(region)
	if err != nil {
		log.Error("Failed to create S3 client", "error", err)
	}
	go func() {
		for start := header.Number.Int64() + 1; start <= endHeight; start++ {
			if checkInterrupt() {
				log.Info("Interrupted during import, stopping at %d", start)
				break
			}
			blockImport, err := ImportSingleFromS3(s3Client, chain, blockHeightBucket, blockBucket, start)
			if err != nil {
				log.Error("Failed to import block", "height", start, "error", err)
				break
			}
			importCh <- blockImport
		}
		close(importCh)
	}()

	parentHeader := header

	for blockImport := range importCh {
		if checkInterrupt() {
			break
		}

		rawBlock := blockImport.RawBLock
		preLoad := blockImport.PreLoad

		snap := chain.GetSnaps().Snapshot(parentHeader.Root)
		if snap == nil {
			log.Error("Failed to get snapshot", "root", parentHeader.Root.Hex())
			return errors.New("snapshot is not available")
		}

		for _, accountLoad := range preLoad.AccountLoads {
			go func(account common.Address) {
				buff := crypto.NewKeccakState()
				_, err := snap.Account(crypto.HashData(buff, account.Bytes()))
				if err != nil {
					log.Error("Failed to get account", "address", account, "error", err)
				}
			}(accountLoad)
		}

		for _, storageLoad := range preLoad.StorageLoads {
			for _, key := range storageLoad.Keys {
				go func(account common.Address, key common.Hash) {
					buff := crypto.NewKeccakState()
					_, err := snap.Storage(crypto.HashData(buff, account.Bytes()), crypto.HashData(buff, key.Bytes()))
					if err != nil {
						log.Error("Failed to get storage", "address", account, "key", key, "error", err)
					}
				}(storageLoad.Address, key)
			}
		}

		if _, err := chain.InsertChain([]*types.Block{rawBlock}); err != nil {
			return fmt.Errorf("invalid block %+v: %v", rawBlock.Header(), err)
		}
		log.Info("Imported block", "height", rawBlock.Number().Int64(), "root", rawBlock.Root().Hex())
		parentHeader = rawBlock.Header()
	}

	return nil

}

func DecodeFromGzipJson(v []byte, target any) error {
	gz, err := gzip.NewReader(bytes.NewBuffer(v))
	if err != nil {
		return err
	}
	return json.NewDecoder(gz).Decode(target)
}

func DecodeFromRlp(v []byte, target any) error {
	return rlp.DecodeBytes(v, target)
}

func downloadFileFromS3(downloader *s3.Client, bucket string, key string) ([]byte, error) {
	output, err := downloader.GetObject(context.TODO(), &s3.GetObjectInput{
		Bucket: &bucket,
		Key:    &key,
	})
	if err != nil {
		return nil, err
	}
	defer output.Body.Close()
	buf := new(bytes.Buffer)
	buf.ReadFrom(output.Body)
	return buf.Bytes(), nil
}

func downloadHashFromS3(downloader *s3.Client, bucket string, chainID int64, blockNumber int64) (common.Hash, error) {
	prefix := fmt.Sprintf("%d/%d/", chainID, blockNumber)
	input := &s3.ListObjectsV2Input{
		Bucket: &bucket,
		Prefix: &prefix,
	}

	output, err := downloader.ListObjectsV2(context.TODO(), input)
	if err != nil {
		return common.Hash{}, fmt.Errorf("failed to list objects from S3: %w", err)
	}

	for _, object := range output.Contents {
		key := *object.Key
		var blockValidation ptypes.BlockValidation
		buf, err := downloadFileFromS3(downloader, bucket, key)
		if err != nil {
			return common.Hash{}, fmt.Errorf("failed to download JSON: %w", err)
		}
		err = DecodeFromGzipJson(buf, &blockValidation)
		if err != nil {
			return common.Hash{}, fmt.Errorf("failed to download and decode JSON: %w", err)
		}

		if !blockValidation.IsFork {
			return common.HexToHash(path.Base(key)), nil
		}
	}

	return common.Hash{}, fmt.Errorf("no valid block found")
}

type BlockImnport struct {
	PreLoad  *ptypes.BlockLoad
	RawBLock *types.Block
}

func ImportSingleFromS3(downloader *s3.Client, chain *core.BlockChain, blockHeightBucket string, blockBucket string, height int64) (*BlockImnport, error) {
	blockHash, err := downloadHashFromS3(downloader, blockHeightBucket, chain.Config().ChainID.Int64(), int64(height))
	if err != nil {
		log.Error("Failed to download block hash from S3", "error", err)
		return nil, err
	}

	blockLoads3Key := fmt.Sprintf("%d/%s/stateLoad", chain.Config().ChainID.Int64(), blockHash.Hex())
	var blockLoad ptypes.BlockLoad
	blockLoadBytes, err := downloadFileFromS3(downloader, blockBucket, blockLoads3Key)
	if err != nil {
		log.Error("Failed to download block load from S3", "blockLoads3Key", blockLoads3Key, "error", err)
		return nil, err
	}
	err = DecodeFromGzipJson(blockLoadBytes, &blockLoad)
	if err != nil {
		log.Error("Failed to decode block load from S3", "error", err)
		return nil, err
	}

	blockRaws3Key := fmt.Sprintf("%d/%s/rawBlock", chain.Config().ChainID.Int64(), blockHash.Hex())
	rawBlock := new(types.Block)
	rawBlockBytes, err := downloadFileFromS3(downloader, blockBucket, blockRaws3Key)
	if err != nil {
		log.Error("Failed to download raw block from S3", "error", err)
		return nil, err
	}
	err = DecodeFromRlp(rawBlockBytes, rawBlock)
	if err != nil {
		log.Error("Failed to decode raw block from S3", "error", err)
		return nil, err
	}

	return &BlockImnport{
		PreLoad:  &blockLoad,
		RawBLock: rawBlock,
	}, nil
}
