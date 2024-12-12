package live

import (
	"github.com/Chaintable/pipeline/tracer"
	"github.com/ethereum/go-ethereum/eth/tracers"
)

// 需要上传3种data
// 1. block
// 2. state diff
// 3. block file

func init() {
	tracers.LiveDirectory.Register("pipeline", tracer.NewPipelineTracer)
}
