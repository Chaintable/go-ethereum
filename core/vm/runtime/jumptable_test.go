package runtime

import (
	"sync"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/vm"
)

// SLOAD and SSTORE a cold slot, the opcodes whose gas the extra EIPs rewrite.
var storageCode = common.Hex2Bytes("6000546001600155" + "00")

func gasWithExtraEips(t *testing.T) uint64 {
	cfg := &Config{GasLimit: 100000, EVMConfig: vm.Config{ExtraEips: []int{2929, 2200, 1884, 1344}}}
	setDefaults(cfg)
	cfg.State = newState()
	address := common.BytesToAddress([]byte("contract"))
	cfg.State.SetCode(address, storageCode)
	_, left, err := Call(address, nil, cfg)
	if err != nil {
		t.Error(err)
	}
	return cfg.GasLimit - left
}

// Building interpreters with extra EIPs concurrently must not change each
// other's gas: EnableEIP used to edit the shared instruction set in place.
func TestExtraEipsConcurrentGas(t *testing.T) {
	want := gasWithExtraEips(t)
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 200; j++ {
				if got := gasWithExtraEips(t); got != want {
					t.Errorf("gas %d, want %d", got, want)
					return
				}
			}
		}()
	}
	wg.Wait()
}
