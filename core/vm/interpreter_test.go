package vm

import (
	"fmt"
	"testing"

	"github.com/ethereum/go-ethereum/params"
)

func TestExtraEipsLeaveInstructionSetUnchanged(t *testing.T) {
	sstore, sload := *berlinInstructionSet[SSTORE], *berlinInstructionSet[SLOAD]
	in := NewEVMInterpreter(&EVM{chainRules: params.Rules{IsBerlin: true}}, Config{ExtraEips: []int{2929, 2200, 1884, 1344}})
	if berlinInstructionSet[SLOAD].constantGas != sload.constantGas ||
		fmt.Sprintf("%p", berlinInstructionSet[SSTORE].dynamicGas) != fmt.Sprintf("%p", sstore.dynamicGas) {
		t.Fatal("extra EIPs changed the shared instruction set")
	}
	if in.cfg.JumpTable[SLOAD].constantGas != params.SloadGasEIP1884 {
		t.Fatalf("interpreter SLOAD constant gas %d, want %d", in.cfg.JumpTable[SLOAD].constantGas, params.SloadGasEIP1884)
	}
}
