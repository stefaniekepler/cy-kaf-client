// smartfilter.go is the thin app-layer wrapper around the domain filter.Engine
// that P1c Task 12's api handlers (registerFilter / executeSmartFilterTest)
// consume. Register is a straight passthrough to the engine's id-caching
// Register; Test is a one-shot compile+evaluate that never touches that id
// cache (filter.Engine.Compile, not Register), mapping a SmartFilterTest onto
// the domain filter.Record the compiled predicate evaluates.
package cluster

import "github.com/cy-kaf/cy-kaf-client/internal/domain/filter"

// SmartFilterTest is one executeSmartFilterTest request: the CEL FilterCode to
// try, plus the sample record fields it runs against. Field-for-field the
// same shape as filter.Record (minus FilterCode) -- the api layer derefs the
// contract's SmartFilterTestExecution pointer fields onto this, and Test maps
// it straight onto filter.Record.
type SmartFilterTest struct {
	FilterCode  string
	Key         string
	Value       string
	Headers     map[string]string
	Partition   int32
	Offset      int64
	TimestampMs int64
}

// SmartFilterService wraps a filter.Engine for the two smart-filter HTTP
// endpoints. It holds no per-cluster state -- filters are a global registry
// keyed by the engine's deterministic id, not cluster-scoped -- so a single
// instance backs every cluster, sharing the very same engine MessageService's
// Browse looks up SmartFilterID predicates in (that shared instance is why
// registerFilter's id is replayable as a later browse's smartFilterId).
type SmartFilterService struct {
	eng filter.Engine
}

// NewSmartFilterService wraps eng. Production passes the same
// infra/filter.NewEngine() instance handed to NewMessageService, so ids
// registered here resolve on the browse path.
func NewSmartFilterService(eng filter.Engine) *SmartFilterService {
	return &SmartFilterService{eng: eng}
}

// Register compiles filterCode and caches it under the engine's deterministic
// id, returning that id (idempotent for identical code). A compile error is
// returned unchanged for the handler to map to 400.
func (s *SmartFilterService) Register(filterCode string) (string, error) {
	return s.eng.Register(filterCode)
}

// Test compiles exec.FilterCode once (never cached -- Compile, not Register)
// and evaluates it against exec's sample record. It reports three distinct
// outcomes: a compile failure as compileErr (matched=false, no eval), an
// evaluation failure as a non-empty evalErr string (matched=false,
// compileErr nil), or a clean verdict as matched (both errors empty). The
// handler folds compileErr/evalErr into the same 200 {result:false,
// error:<msg>} response the contract requires.
func (s *SmartFilterService) Test(exec SmartFilterTest) (matched bool, evalErr string, compileErr error) {
	pred, err := s.eng.Compile(exec.FilterCode)
	if err != nil {
		return false, "", err
	}
	ok, err := pred.Eval(filter.Record{
		Key:         exec.Key,
		Value:       exec.Value,
		Headers:     exec.Headers,
		Partition:   exec.Partition,
		Offset:      exec.Offset,
		TimestampMs: exec.TimestampMs,
	})
	if err != nil {
		return false, err.Error(), nil
	}
	return ok, "", nil
}
