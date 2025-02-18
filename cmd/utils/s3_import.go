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
	"sync"
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
		maxConcurrent := int64(100)
		for start := header.Number.Int64() + 1; start <= endHeight; start += maxConcurrent {
			if checkInterrupt() {
				log.Info("Interrupted during import, stopping at %d", start)
				break
			}
			wg := sync.WaitGroup{}
			out := make([]*BlockImnport, maxConcurrent)
			taskNum := int(maxConcurrent)
			if start+maxConcurrent > endHeight {
				taskNum = int(endHeight - start + 1)
			}
			for i := 0; i < taskNum; i++ {
				wg.Add(1)
				go func(i int) {
					defer wg.Done()
					blockImport, err := ImportSingleFromS3(s3Client, chain, blockHeightBucket, blockBucket, start+int64(i))
					if err != nil {
						log.Error("Failed to import block", "height", start+int64(i), "error", err)
						return
					}
					out[i] = blockImport
				}(i)
			}
			wg.Wait()
			for i := 0; i < taskNum; i++ {
				if out[i] == nil {
					log.Error("Failed to import block", "height", start+int64(i), "error", "blockImport is nil")
					return
				}
				importCh <- out[i]
			}
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

		wg := sync.WaitGroup{}
		for _, accountLoad := range preLoad.AccountLoads {
			wg.Add(1)
			go func(account common.Address) {
				defer wg.Done()
				buff := crypto.NewKeccakState()
				_, err := snap.Account(crypto.HashData(buff, account.Bytes()))
				if err != nil {
					log.Error("Failed to get account", "address", account, "error", err)
				}
			}(accountLoad)
		}

		for _, storageLoad := range preLoad.StorageLoads {
			for _, key := range storageLoad.Keys {
				wg.Add(1)
				go func(account common.Address, key common.Hash) {
					defer wg.Done()
					buff := crypto.NewKeccakState()
					_, err := snap.Storage(crypto.HashData(buff, account.Bytes()), crypto.HashData(buff, key.Bytes()))
					if err != nil {
						log.Error("Failed to get storage", "address", account, "key", key, "error", err)
					}
				}(storageLoad.Address, key)
			}
		}
		wg.Wait()

		if _, err := chain.InsertChain([]*types.Block{rawBlock}); err != nil {
			return fmt.Errorf("invalid block %+v: %v", rawBlock.Header(), err)
		}
		parentHeader = rawBlock.Header()
	}

	return nil

}
func ConcurrentFormS3(chain *core.BlockChain, blockHeightBucket string, blockBucket string, startHeight int64, endHeight int64, region string, importCh chan *BlockImnport) {

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

	wg := sync.WaitGroup{}
	var blockLoad ptypes.BlockLoad
	wg.Add(1)
	var err0 error
	go func() {
		defer wg.Done()
		blockLoads3Key := fmt.Sprintf("%d/%s/stateLoad", chain.Config().ChainID.Int64(), blockHash.Hex())
		blockLoadBytes, err := downloadFileFromS3(downloader, blockBucket, blockLoads3Key)
		if err != nil {
			log.Error("Failed to download block load from S3", "blockLoads3Key", blockLoads3Key, "error", err)
			err0 = err
			return
		}
		err = DecodeFromGzipJson(blockLoadBytes, &blockLoad)
		if err != nil {
			log.Error("Failed to decode block load from S3", "error", err)
			err0 = err
			return
		}
	}()

	rawBlock := new(types.Block)
	wg.Add(1)
	var err1 error
	go func() {
		defer wg.Done()
		blockRaws3Key := fmt.Sprintf("%d/%s/rawBlock", chain.Config().ChainID.Int64(), blockHash.Hex())
		rawBlockBytes, err := downloadFileFromS3(downloader, blockBucket, blockRaws3Key)
		if err != nil {
			log.Error("Failed to download raw block from S3", "error", err)
			err1 = err
			return
		}
		err = DecodeFromRlp(rawBlockBytes, rawBlock)
		if err != nil {
			log.Error("Failed to decode raw block from S3", "error", err)
			err1 = err
			return
		}
	}()

	wg.Wait()
	if err0 != nil || err1 != nil {
		return nil, fmt.Errorf("failed to download block data: %w,%w", err0, err1)
	}

	return &BlockImnport{
		PreLoad:  &blockLoad,
		RawBLock: rawBlock,
	}, nil
}
