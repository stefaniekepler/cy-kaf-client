package cluster

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/cy-kaf/cy-kaf-client/internal/domain/filter"
)

// recordCapturePredicate records the last filter.Record its Eval saw and
// returns a preset (matched, err). It lets the SmartFilterService.Test tests
// assert that every SmartFilterTest field is mapped onto filter.Record
// one-for-one before the compiled predicate ever runs -- the one piece of
// real logic Test owns beyond delegating to the engine.
type recordCapturePredicate struct {
	matched bool
	err     error
	got     *filter.Record
}

func (p *recordCapturePredicate) Eval(rec filter.Record) (bool, error) {
	p.got = &rec
	return p.matched, p.err
}

// TestSmartFilterServiceRegisterReturnsEngineIdAndNilError proves Register is
// a straight passthrough to filter.Engine.Register on the success path (the
// registerFilter -> MessageFilterId round trip's backing call).
func TestSmartFilterServiceRegisterReturnsEngineIdAndNilError(t *testing.T) {
	eng := newFakeFilterEngine()
	eng.registerID = "deadbeef"
	svc := NewSmartFilterService(eng)

	id, err := svc.Register("record.value == 'x'")
	require.NoError(t, err)
	require.Equal(t, "deadbeef", id)
}

// TestSmartFilterServiceRegisterPropagatesCompileError proves a filter that
// won't compile surfaces the engine's error unchanged (the api handler maps
// it to 400).
func TestSmartFilterServiceRegisterPropagatesCompileError(t *testing.T) {
	eng := newFakeFilterEngine()
	eng.registerErr = errors.New("cel: undeclared reference to 'bogus'")
	svc := NewSmartFilterService(eng)

	_, err := svc.Register("bogus(")
	require.Error(t, err)
	require.Contains(t, err.Error(), "undeclared reference")
}

// TestSmartFilterServiceTestCompilesEvalsAndMapsEveryRecordField proves the
// happy path: Test compiles the code, maps every SmartFilterTest field onto
// filter.Record, evaluates, and reports the predicate's verdict with no
// compile/eval error.
func TestSmartFilterServiceTestCompilesEvalsAndMapsEveryRecordField(t *testing.T) {
	pred := &recordCapturePredicate{matched: true}
	eng := newFakeFilterEngine()
	eng.compilePred = pred
	svc := NewSmartFilterService(eng)

	exec := SmartFilterTest{
		FilterCode:  "record.value == 'v'",
		Key:         "k",
		Value:       "v",
		Headers:     map[string]string{"h": "1"},
		Partition:   3,
		Offset:      42,
		TimestampMs: 1700,
	}
	matched, evalErr, compileErr := svc.Test(exec)

	require.NoError(t, compileErr)
	require.Empty(t, evalErr)
	require.True(t, matched)
	require.NotNil(t, pred.got, "the compiled predicate must actually be evaluated")
	require.Equal(t, filter.Record{
		Key:         "k",
		Value:       "v",
		Headers:     map[string]string{"h": "1"},
		Partition:   3,
		Offset:      42,
		TimestampMs: 1700,
	}, *pred.got)
}

// TestSmartFilterServiceTestCompileErrorReturnsCompileErrAndSkipsEval proves a
// filter that won't compile reports compileErr (not evalErr), matched=false,
// and never reaches Eval. The api handler maps compileErr to a 200
// {result:false, error:<msg>} -- HTTP still 200, per the contract.
func TestSmartFilterServiceTestCompileErrorReturnsCompileErrAndSkipsEval(t *testing.T) {
	eng := newFakeFilterEngine()
	eng.compileErr = errors.New("cel syntax error near '('")
	// compilePred left nil on purpose: if Test ever called Eval on it the
	// test would panic, proving compile failure must short-circuit.
	svc := NewSmartFilterService(eng)

	matched, evalErr, compileErr := svc.Test(SmartFilterTest{FilterCode: "bogus("})

	require.Error(t, compileErr)
	require.Contains(t, compileErr.Error(), "syntax error")
	require.False(t, matched)
	require.Empty(t, evalErr)
}

// TestSmartFilterServiceTestEvalErrorReturnsEvalStringNotCompileErr proves a
// filter that compiles but errors at evaluation time (e.g. a runtime type
// mismatch) reports evalErr as a string with compileErr nil -- the two
// failure origins stay distinct, matching Test's (bool, string, error)
// contract. Reuses message_test.go's containsXOrBoomPredicate, which errors
// when the value contains "boom".
func TestSmartFilterServiceTestEvalErrorReturnsEvalStringNotCompileErr(t *testing.T) {
	eng := newFakeFilterEngine()
	eng.compilePred = containsXOrBoomPredicate{}
	svc := NewSmartFilterService(eng)

	matched, evalErr, compileErr := svc.Test(SmartFilterTest{
		FilterCode: "record.value.contains('x')",
		Value:      "boom",
	})

	require.NoError(t, compileErr)
	require.False(t, matched)
	require.Equal(t, "predicate eval boom", evalErr)
}
