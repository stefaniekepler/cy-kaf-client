// handlers_smartfilter.go implements the two smart-filter endpoints (P1c Task
// 12): registerFilter (compile a CEL filter and hand back its replayable id)
// and executeSmartFilterTest (try a filter against a sample record). Both
// delegate to the api-narrow SmartFilterServicer (server.go), which
// *appcluster.SmartFilterService satisfies.
package api

import (
	"encoding/json"
	"net/http"

	"github.com/cy-kaf/cy-kaf-client/internal/api/generated"
	appcluster "github.com/cy-kaf/cy-kaf-client/internal/app/cluster"
)

// RegisterFilter serves POST .../topics/{topicName}/smartfilters: decodes a
// MessageFilterRegistration, compiles+registers its filterCode, and returns
// 200 MessageFilterId{id} -- the id the frontend later replays as a browse's
// smartFilterId query param.
//
// clusterName/topicName are path params but intentionally unused: filters live
// in one global, deterministic-id-keyed registry (the shared filter.Engine),
// not per cluster or per topic -- registering the same code anywhere yields
// the same id, and any browse of any topic can replay it. The path is
// cluster-scoped only to mirror upstream's URL shape.
//
// Errors: a body that won't parse as JSON is a 400 ("invalid request body"),
// same decode->400 pattern as the other write handlers; a filterCode that
// won't compile is a 400 echoing the compile error (safe -- it is direct
// feedback on the client's own submitted CEL, not an internal server detail,
// and materially helps them fix it). There is deliberately no unknown-cluster
// 404 here: with no cluster lookup on this path, there is nothing to 404 on.
func (s *apiServer) RegisterFilter(w http.ResponseWriter, r *http.Request, clusterName, topicName string) {
	var body generated.MessageFilterRegistration
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeJSON(w, http.StatusBadRequest, errorResponse(http.StatusBadRequest, "invalid request body"))
		return
	}
	var code string
	if body.FilterCode != nil {
		code = *body.FilterCode
	}
	id, err := s.deps.SmartFilters.Register(code)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, errorResponse(http.StatusBadRequest, err.Error()))
		return
	}
	writeJSON(w, http.StatusOK, generated.MessageFilterId{Id: &id})
}

// ExecuteSmartFilterTest serves PUT /api/smartfilters/testexecutions: decodes a
// SmartFilterTestExecution, compiles+evaluates its filterCode against the
// sample record once (never cached), and returns 200
// SmartFilterTestExecutionResult{result, error}.
//
// Only a body that won't parse as JSON is an HTTP error (400). A filter that
// won't compile, or that errors at evaluation time, is NOT an HTTP error --
// per the contract it is a 200 with result:false and the failure text in
// error. This is the deliberate difference from RegisterFilter, where a
// compile failure IS a 400: registerFilter commits a filter, so bad input is a
// request error; executeSmartFilterTest is a dry run whose whole job is to
// report "does this (possibly broken) filter match?", so a broken filter is a
// valid, successful answer of "no, and here's why".
func (s *apiServer) ExecuteSmartFilterTest(w http.ResponseWriter, r *http.Request) {
	var body generated.SmartFilterTestExecution
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeJSON(w, http.StatusBadRequest, errorResponse(http.StatusBadRequest, "invalid request body"))
		return
	}

	matched, evalErr, compileErr := s.deps.SmartFilters.Test(smartFilterTestFromGenerated(body))

	var result generated.SmartFilterTestExecutionResult
	switch {
	case compileErr != nil:
		no, msg := false, compileErr.Error()
		result.Result, result.Error = &no, &msg
	case evalErr != "":
		no := false
		result.Result, result.Error = &no, &evalErr
	default:
		result.Result = &matched
	}
	writeJSON(w, http.StatusOK, result)
}

// smartFilterTestFromGenerated derefs one SmartFilterTestExecution's pointer
// sample-record fields onto appcluster.SmartFilterTest, each omitted field
// defaulting to its Go zero value (which the CEL record model treats as "empty
// key"/"partition 0"/etc.) -- FilterCode is the one required, non-pointer
// field so it copies straight across.
func smartFilterTestFromGenerated(body generated.SmartFilterTestExecution) appcluster.SmartFilterTest {
	exec := appcluster.SmartFilterTest{FilterCode: body.FilterCode}
	if body.Key != nil {
		exec.Key = *body.Key
	}
	if body.Value != nil {
		exec.Value = *body.Value
	}
	if body.Headers != nil {
		exec.Headers = *body.Headers
	}
	if body.Partition != nil {
		exec.Partition = *body.Partition
	}
	if body.Offset != nil {
		exec.Offset = *body.Offset
	}
	if body.TimestampMs != nil {
		exec.TimestampMs = *body.TimestampMs
	}
	return exec
}
