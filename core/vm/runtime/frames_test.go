package runtime

import (
	"math/big"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/core/vm"
	"github.com/ethereum/go-ethereum/crypto"
)

func newState() *state.StateDB {
	db, _ := state.New(common.Hash{}, state.NewDatabase(rawdb.NewMemoryDatabase()), nil)
	return db
}

type frame struct {
	typ     vm.OpCode
	to      common.Address
	value   *big.Int
	gasUsed uint64
	err     error
}

type frameTracer struct {
	roots       int
	rootEnds    int
	rootGasUsed uint64
	rootErr     error
	frames      []*frame
	open        []*frame
}

func (t *frameTracer) CaptureStart(*vm.EVM, common.Address, common.Address, bool, []byte, uint64, *big.Int) {
	t.roots++
}
func (t *frameTracer) CaptureState(*vm.EVM, uint64, vm.OpCode, uint64, uint64, *vm.ScopeContext, []byte, int, error) {
}
func (t *frameTracer) CaptureFault(*vm.EVM, uint64, vm.OpCode, uint64, uint64, *vm.ScopeContext, int, error) {
}
func (t *frameTracer) CaptureEnd(_ []byte, gasUsed uint64, _ time.Duration, err error) {
	t.rootEnds++
	t.rootGasUsed, t.rootErr = gasUsed, err
}

func (t *frameTracer) CaptureEnter(typ vm.OpCode, from, to common.Address, input []byte, gas uint64, value *big.Int) {
	f := &frame{typ: typ, to: to, value: value}
	t.frames = append(t.frames, f)
	t.open = append(t.open, f)
}

func (t *frameTracer) CaptureExit(output []byte, gasUsed uint64, err error) {
	f := t.open[len(t.open)-1]
	t.open = t.open[:len(t.open)-1]
	f.gasUsed, f.err = gasUsed, err
}

// STATICCALL identity precompile, CALL a reverting contract, CALL with value
// the caller does not have, CREATE an empty contract, SELFDESTRUCT to 0xdd.
var frameCode = common.Hex2Bytes(
	"6000600060006000" + "6004" + "61ffff" + "fa50" +
		"6000600060006000600060bb61ffff" + "f150" +
		"6000600060006000600160cc61ffff" + "f150" +
		"6000600053" + "600160006000f050" + "60ddff")

func TestCallFrameHooks(t *testing.T) {
	tracer := &frameTracer{}
	cfg := &Config{Debug: true, EVMConfig: vm.Config{Debug: true, Tracer: tracer}}
	setDefaults(cfg)
	cfg.State = newState()
	cfg.State.SetCode(common.HexToAddress("0xbb"), common.Hex2Bytes("60006000fd"))
	if _, _, err := Execute(frameCode, nil, cfg); err != nil {
		t.Fatal(err)
	}
	if tracer.roots != 1 {
		t.Fatalf("root CaptureStart called %d times", tracer.roots)
	}
	if len(tracer.open) != 0 {
		t.Fatalf("%d frames not exited", len(tracer.open))
	}
	contract := common.BytesToAddress([]byte("contract"))
	want := []frame{
		{typ: vm.STATICCALL, to: common.HexToAddress("0x04")},
		{typ: vm.CALL, to: common.HexToAddress("0xbb"), value: new(big.Int), gasUsed: 6, err: vm.ErrExecutionReverted},
		{typ: vm.CALL, to: common.HexToAddress("0xcc"), value: big.NewInt(1), gasUsed: 0, err: vm.ErrInsufficientBalance},
		{typ: vm.CREATE, to: crypto.CreateAddress(contract, 0), value: new(big.Int)},
		{typ: vm.SELFDESTRUCT, to: common.HexToAddress("0xdd"), value: new(big.Int)},
	}
	if len(tracer.frames) != len(want) {
		t.Fatalf("got %d frames, want %d", len(tracer.frames), len(want))
	}
	for i, w := range want {
		g := tracer.frames[i]
		if g.typ != w.typ || g.to != w.to || g.err != w.err {
			t.Errorf("frame %d: got %v %x %v, want %v %x %v", i, g.typ, g.to, g.err, w.typ, w.to, w.err)
		}
		if (w.value == nil) != (g.value == nil) || (w.value != nil && w.value.Cmp(g.value) != 0) {
			t.Errorf("frame %d: value %v, want %v", i, g.value, w.value)
		}
		if w.err != nil && g.gasUsed != w.gasUsed {
			t.Errorf("frame %d: gasUsed %d, want %d", i, g.gasUsed, w.gasUsed)
		}
	}
}

func TestCallFrameHooksNeedDebug(t *testing.T) {
	tracer := &frameTracer{}
	cfg := &Config{EVMConfig: vm.Config{Tracer: tracer}}
	setDefaults(cfg)
	cfg.State = newState()
	if _, _, err := Execute(frameCode, nil, cfg); err != nil {
		t.Fatal(err)
	}
	if len(tracer.frames) != 0 || tracer.roots != 0 {
		t.Fatalf("tracer called without Debug: %d frames, %d roots", len(tracer.frames), tracer.roots)
	}
}

// A top-level CREATE whose address is taken fails before running any code;
// the root frame must still be reported.
func TestRootCreateCollisionFrame(t *testing.T) {
	tracer := &frameTracer{}
	cfg := &Config{GasLimit: 100000, EVMConfig: vm.Config{Debug: true, Tracer: tracer}}
	setDefaults(cfg)
	cfg.State = newState()
	cfg.State.SetNonce(crypto.CreateAddress(cfg.Origin, 0), 1)
	_, _, leftOver, err := Create(common.Hex2Bytes("00"), cfg)
	if err != vm.ErrContractAddressCollision {
		t.Fatalf("got %v, want address collision", err)
	}
	if tracer.roots != 1 || tracer.rootEnds != 1 {
		t.Fatalf("root start/end called %d/%d times", tracer.roots, tracer.rootEnds)
	}
	if tracer.rootErr != vm.ErrContractAddressCollision || tracer.rootGasUsed != 100000-leftOver {
		t.Fatalf("root end: %v, gas used %d, left over %d", tracer.rootErr, tracer.rootGasUsed, leftOver)
	}
	if len(tracer.frames) != 0 {
		t.Fatalf("%d nested frames", len(tracer.frames))
	}
}

// A top-level CALL whose value exceeds the sender's balance fails before
// running any code; the root frame must still be reported.
func TestRootCallInsufficientBalanceFrame(t *testing.T) {
	tracer := &frameTracer{}
	cfg := &Config{GasLimit: 100000, Value: big.NewInt(1), EVMConfig: vm.Config{Debug: true, Tracer: tracer}}
	setDefaults(cfg)
	cfg.State = newState()
	_, leftOver, err := Call(common.HexToAddress("0xbb"), nil, cfg)
	if err != vm.ErrInsufficientBalance {
		t.Fatalf("got %v, want insufficient balance", err)
	}
	if tracer.roots != 1 || tracer.rootEnds != 1 {
		t.Fatalf("root start/end called %d/%d times", tracer.roots, tracer.rootEnds)
	}
	if tracer.rootErr != vm.ErrInsufficientBalance || tracer.rootGasUsed != 100000-leftOver {
		t.Fatalf("root end: %v, gas used %d, left over %d", tracer.rootErr, tracer.rootGasUsed, leftOver)
	}
}

// A DELEGATECALL frame reports the value it inherits from its parent.
func TestDelegateCallFrameInheritsValue(t *testing.T) {
	tracer := &frameTracer{}
	cfg := &Config{GasLimit: 100000, Value: big.NewInt(5), EVMConfig: vm.Config{Debug: true, Tracer: tracer}}
	setDefaults(cfg)
	cfg.State = newState()
	cfg.State.AddBalance(cfg.Origin, big.NewInt(10))
	proxy := common.HexToAddress("0xaa")
	cfg.State.SetCode(proxy, common.Hex2Bytes("6000600060006000"+"60bb"+"61ffff"+"f450"+"00"))
	cfg.State.SetCode(common.HexToAddress("0xbb"), common.Hex2Bytes("00"))
	if _, _, err := Call(proxy, nil, cfg); err != nil {
		t.Fatal(err)
	}
	if len(tracer.frames) != 1 || tracer.frames[0].typ != vm.DELEGATECALL {
		t.Fatalf("got %d frames", len(tracer.frames))
	}
	if v := tracer.frames[0].value; v == nil || v.Cmp(big.NewInt(5)) != 0 {
		t.Fatalf("DELEGATECALL value %v, want 5", v)
	}
}
