package api_test

import (
	"encoding/json"
	"errors"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/cy-kaf/cy-kaf-client/internal/api"
	appcluster "github.com/cy-kaf/cy-kaf-client/internal/app/cluster"
)

// fakeSmartFilterServicer is a deterministic api.SmartFilterServicer: Register
// and Test each return caller-preset values and record their last argument, so
// handler tests can assert both the HTTP response and that the request body
// was mapped onto the app-layer call correctly -- without pulling the real
// CEL engine into the api package (that engine's correctness is
// infra/filter's own test suite's job).
type fakeSmartFilterServicer struct {
	registerID  string
	registerErr error

	testMatched    bool
	testEvalErr    string
	testCompileErr error

	lastRegisterCode string
	registerCalled   bool
	lastTest         appcluster.SmartFilterTest
	testCalled       bool
}

func newFakeSmartFilterServicer() *fakeSmartFilterServicer { return &fakeSmartFilterServicer{} }

func (f *fakeSmartFilterServicer) Register(filterCode string) (string, error) {
	f.registerCalled = true
	f.lastRegisterCode = filterCode
	return f.registerID, f.registerErr
}

func (f *fakeSmartFilterServicer) Test(exec appcluster.SmartFilterTest) (bool, string, error) {
	f.testCalled = true
	f.lastTest = exec
	return f.testMatched, f.testEvalErr, f.testCompileErr
}

// withSmartFilters wires fs as Deps.SmartFilters -- same pattern as
// withMessages/withSerdes.
func withSmartFilters(fs *fakeSmartFilterServicer) testServerOption {
	return func(d *api.Deps) { d.SmartFilters = fs }
}

// --- registerFilter (POST .../topics/{topicName}/smartfilters) ---

// TestRegisterFilter_ValidCELReturns200WithId proves a compilable filter is
// registered and its stable id returned as MessageFilterId{id}, and that the
// request body's filterCode reached the app call unchanged.
func TestRegisterFilter_ValidCELReturns200WithId(t *testing.T) {
	fake := newFakeSmartFilterServicer()
	fake.registerID = "deadbeef"
	srv := newTestServer(withSmartFilters(fake))
	defer srv.Close()

	var out struct {
		Id string `json:"id"`
	}
	req, code, hdr, body := bodyJSON(t, http.MethodPost, srv,
		"/api/clusters/prod/topics/t1/smartfilters", `{"filterCode":"record.value == 'x'"}`)
	require.Equal(t, 200, code)
	require.NoError(t, json.Unmarshal(body, &out))
	require.Equal(t, "deadbeef", out.Id)
	require.Equal(t, "record.value == 'x'", fake.lastRegisterCode)
	validateAgainstContract(t, req, code, hdr, body)
}

// TestRegisterFilter_CompileErrorReturns400 proves an uncompilable filter maps
// to 400 with the engine's compile error echoed in the message (safe: it is
// feedback on the client's own submitted CEL, not an internal server detail).
func TestRegisterFilter_CompileErrorReturns400(t *testing.T) {
	fake := newFakeSmartFilterServicer()
	fake.registerErr = errors.New("cel: undeclared reference to 'bogus'")
	srv := newTestServer(withSmartFilters(fake))
	defer srv.Close()

	req, code, hdr, body := bodyJSON(t, http.MethodPost, srv,
		"/api/clusters/prod/topics/t1/smartfilters", `{"filterCode":"bogus("}`)
	require.Equal(t, 400, code)
	assertErrorEnvelope(t, body, "undeclared reference", "")
	validateAgainstContract(t, req, code, hdr, body)
}

// TestRegisterFilter_MalformedBodyReturns400 proves a body that doesn't parse
// as JSON is a pre-call 400 -- Register must never run. Contract validation is
// skipped: the malformed request body would fail kin-openapi's request-side
// validation for a different reason than the assertion under test (same
// rationale as the topic/message malformed-body tests).
func TestRegisterFilter_MalformedBodyReturns400(t *testing.T) {
	fake := newFakeSmartFilterServicer()
	srv := newTestServer(withSmartFilters(fake))
	defer srv.Close()

	_, code, _, _ := bodyJSON(t, http.MethodPost, srv,
		"/api/clusters/prod/topics/t1/smartfilters", `{not json`)
	require.Equal(t, 400, code)
	require.False(t, fake.registerCalled, "Register must never be called on a malformed body")
}

// --- executeSmartFilterTest (PUT /api/smartfilters/testexecutions) ---

// TestExecuteSmartFilterTest_MatchReturns200ResultTrue proves a filter that
// matches its sample record yields 200 {result:true, error:absent}, and every
// sample-record field is mapped onto the app-layer SmartFilterTest.
func TestExecuteSmartFilterTest_MatchReturns200ResultTrue(t *testing.T) {
	fake := newFakeSmartFilterServicer()
	fake.testMatched = true
	srv := newTestServer(withSmartFilters(fake))
	defer srv.Close()

	var out struct {
		Result *bool   `json:"result"`
		Error  *string `json:"error"`
	}
	req, code, hdr, body := bodyJSON(t, http.MethodPut, srv,
		"/api/smartfilters/testexecutions",
		`{"filterCode":"record.value == 'x'","key":"k","value":"x","partition":3,"offset":42,"timestampMs":1700,"headers":{"h":"1"}}`)
	require.Equal(t, 200, code)
	require.NoError(t, json.Unmarshal(body, &out))
	require.NotNil(t, out.Result)
	require.True(t, *out.Result)
	require.Nil(t, out.Error, "a clean match must not carry an error field")
	require.Equal(t, appcluster.SmartFilterTest{
		FilterCode:  "record.value == 'x'",
		Key:         "k",
		Value:       "x",
		Headers:     map[string]string{"h": "1"},
		Partition:   3,
		Offset:      42,
		TimestampMs: 1700,
	}, fake.lastTest)
	validateAgainstContract(t, req, code, hdr, body)
}

// TestExecuteSmartFilterTest_CompileErrorReturns200ResultFalseWithError proves
// an uncompilable filter is NOT an HTTP error here (unlike registerFilter): it
// is a 200 {result:false, error:<msg>}, per the contract.
func TestExecuteSmartFilterTest_CompileErrorReturns200ResultFalseWithError(t *testing.T) {
	fake := newFakeSmartFilterServicer()
	fake.testCompileErr = errors.New("cel syntax error near '('")
	srv := newTestServer(withSmartFilters(fake))
	defer srv.Close()

	var out struct {
		Result *bool   `json:"result"`
		Error  *string `json:"error"`
	}
	req, code, hdr, body := bodyJSON(t, http.MethodPut, srv,
		"/api/smartfilters/testexecutions", `{"filterCode":"bogus("}`)
	require.Equal(t, 200, code)
	require.NoError(t, json.Unmarshal(body, &out))
	require.NotNil(t, out.Result)
	require.False(t, *out.Result)
	require.NotNil(t, out.Error)
	require.Contains(t, *out.Error, "syntax error")
	validateAgainstContract(t, req, code, hdr, body)
}

// TestExecuteSmartFilterTest_EvalErrorReturns200ResultFalseWithError proves a
// filter that compiles but errors at evaluation time is likewise a 200
// {result:false, error:<msg>}.
func TestExecuteSmartFilterTest_EvalErrorReturns200ResultFalseWithError(t *testing.T) {
	fake := newFakeSmartFilterServicer()
	fake.testEvalErr = "predicate eval boom"
	srv := newTestServer(withSmartFilters(fake))
	defer srv.Close()

	var out struct {
		Result *bool   `json:"result"`
		Error  *string `json:"error"`
	}
	req, code, hdr, body := bodyJSON(t, http.MethodPut, srv,
		"/api/smartfilters/testexecutions", `{"filterCode":"record.value.contains('x')","value":"boom"}`)
	require.Equal(t, 200, code)
	require.NoError(t, json.Unmarshal(body, &out))
	require.NotNil(t, out.Result)
	require.False(t, *out.Result)
	require.NotNil(t, out.Error)
	require.Contains(t, *out.Error, "boom")
	validateAgainstContract(t, req, code, hdr, body)
}

// TestExecuteSmartFilterTest_MalformedBodyReturns400 proves a body that
// doesn't parse as JSON is a pre-call 400 -- Test must never run.
func TestExecuteSmartFilterTest_MalformedBodyReturns400(t *testing.T) {
	fake := newFakeSmartFilterServicer()
	srv := newTestServer(withSmartFilters(fake))
	defer srv.Close()

	_, code, _, _ := bodyJSON(t, http.MethodPut, srv,
		"/api/smartfilters/testexecutions", `{bad`)
	require.Equal(t, 400, code)
	require.False(t, fake.testCalled, "Test must never be called on a malformed body")
}
