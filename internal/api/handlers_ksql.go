package api

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"

	"github.com/cy-kaf/cy-kaf-client/internal/api/generated"
	appcluster "github.com/cy-kaf/cy-kaf-client/internal/app/cluster"
	domaincluster "github.com/cy-kaf/cy-kaf-client/internal/domain/cluster"
)

const (
	ksqlRequestBodyLimit = 1 << 20
	ksqlInvalidMessage   = "invalid ksql command"
)

var errKsqlTrailingJSON = errors.New("trailing JSON in ksql request")

// ExecuteKsql validates and registers one KSQL command.  Registration is
// intentionally lazy: the application service stores the command under an
// opaque pipe id, and the subsequent GET is what starts the downstream
// request.  This mirrors the UI's submit-then-open SSE sequence and keeps a
// browser that never opens the pipe from leaving a running query behind.
func (s *apiServer) ExecuteKsql(w http.ResponseWriter, r *http.Request, clusterName string) {
	if s.deps.Ksql == nil {
		serverError(w, "ExecuteKsql", "failed to execute ksql", errors.New("ksql service unavailable"))
		return
	}
	command, err := decodeKsqlCommand(w, r)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, errorResponse(http.StatusBadRequest, ksqlInvalidMessage))
		return
	}

	streamsProperties := make(map[string]string)
	if command.StreamsProperties != nil {
		for key, value := range *command.StreamsProperties {
			streamsProperties[key] = value
		}
	}
	pipeID, err := s.deps.Ksql.Register(r.Context(), clusterName, command.Ksql, streamsProperties)
	if err != nil {
		writeKsqlPreStreamError(w, "ExecuteKsql", err)
		return
	}
	writeJSON(w, http.StatusOK, generated.KsqlCommandV2Response{PipeId: pipeID})
}

// OpenKsqlResponsePipe claims a one-shot command and exposes its result
// frames through the common lazy SSE helper.  No streaming headers are set
// here: writeSSE only commits them when the first frame is successfully
// marshalled, which leaves room for a normal JSON 404/500 before that point.
func (s *apiServer) OpenKsqlResponsePipe(w http.ResponseWriter, r *http.Request, clusterName string, params generated.OpenKsqlResponsePipeParams) {
	if s.deps.Ksql == nil {
		serverError(w, "OpenKsqlResponsePipe", "failed to open ksql response", errors.New("ksql service unavailable"))
		return
	}
	err := writeSSE(w, r, func(send func(v any) error) error {
		return s.deps.Ksql.Open(r.Context(), clusterName, params.PipeId, func(table domaincluster.KsqlTable) error {
			return send(generated.KsqlResponse{Table: ksqlTableToGenerated(table)})
		})
	})
	if err == nil {
		return
	}
	writeKsqlPreStreamError(w, "OpenKsqlResponsePipe", err)
}

// ListStreams maps already-normalized domain descriptions to the generated
// OpenAPI response.  Keeping this conversion here (rather than decoding wire
// JSON in the API package) preserves the domain/infra boundary and optional
// pointer semantics of the generated model.
func (s *apiServer) ListStreams(w http.ResponseWriter, r *http.Request, clusterName string) {
	if s.deps.Ksql == nil {
		serverError(w, "ListStreams", "failed to list ksql streams", errors.New("ksql service unavailable"))
		return
	}
	streams, err := s.deps.Ksql.ListStreams(r.Context(), clusterName)
	if err != nil {
		writeKsqlPreStreamError(w, "ListStreams", err)
		return
	}
	out := make([]generated.KsqlStreamDescription, 0, len(streams))
	for _, stream := range streams {
		out = append(out, generated.KsqlStreamDescription{
			Name:        stream.Name,
			Topic:       stream.Topic,
			KeyFormat:   stream.KeyFormat,
			ValueFormat: stream.ValueFormat,
		})
	}
	writeJSON(w, http.StatusOK, out)
}

// ListTables is the table counterpart of ListStreams; order and nil pointer
// fields are retained exactly as returned by the application service.
func (s *apiServer) ListTables(w http.ResponseWriter, r *http.Request, clusterName string) {
	if s.deps.Ksql == nil {
		serverError(w, "ListTables", "failed to list ksql tables", errors.New("ksql service unavailable"))
		return
	}
	tables, err := s.deps.Ksql.ListTables(r.Context(), clusterName)
	if err != nil {
		writeKsqlPreStreamError(w, "ListTables", err)
		return
	}
	out := make([]generated.KsqlTableDescription, 0, len(tables))
	for _, table := range tables {
		out = append(out, generated.KsqlTableDescription{
			Name:        table.Name,
			Topic:       table.Topic,
			KeyFormat:   table.KeyFormat,
			ValueFormat: table.ValueFormat,
			IsWindowed:  table.IsWindowed,
		})
	}
	writeJSON(w, http.StatusOK, out)
}

func decodeKsqlCommand(w http.ResponseWriter, r *http.Request) (generated.KsqlCommandV2, error) {
	if r.Body == nil {
		return generated.KsqlCommandV2{}, io.EOF
	}
	r.Body = http.MaxBytesReader(w, r.Body, ksqlRequestBodyLimit)
	decoder := json.NewDecoder(r.Body)
	var command *generated.KsqlCommandV2
	if err := decoder.Decode(&command); err != nil || command == nil {
		if err == nil {
			err = errors.New("null ksql command")
		}
		return generated.KsqlCommandV2{}, err
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		if err == nil {
			return generated.KsqlCommandV2{}, errKsqlTrailingJSON
		}
		return generated.KsqlCommandV2{}, err
	}
	return *command, nil
}

func ksqlTableToGenerated(table domaincluster.KsqlTable) *generated.KsqlTableResponse {
	header := table.Header
	columns := append([]string(nil), table.ColumnNames...)
	values := append([][]any(nil), table.Values...)
	return &generated.KsqlTableResponse{
		Header:      &header,
		ColumnNames: &columns,
		Values:      &values,
	}
}

func writeKsqlPreStreamError(w http.ResponseWriter, op string, err error) {
	switch {
	case errors.Is(err, appcluster.ErrKsqlClusterNotFound):
		writeJSON(w, http.StatusNotFound, errorResponse(http.StatusNotFound, "cluster not found"))
	case errors.Is(err, appcluster.ErrKsqlNotConfigured):
		writeJSON(w, http.StatusNotFound, errorResponse(http.StatusNotFound, "ksql not configured"))
	case errors.Is(err, appcluster.ErrKsqlPipeNotFound):
		writeJSON(w, http.StatusNotFound, errorResponse(http.StatusNotFound, "pipe not found"))
	case errors.Is(err, appcluster.ErrKsqlInvalidCommand):
		writeJSON(w, http.StatusBadRequest, errorResponse(http.StatusBadRequest, ksqlInvalidMessage))
	default:
		message := "failed to execute ksql"
		switch op {
		case "OpenKsqlResponsePipe":
			message = "failed to open ksql response"
		case "ListStreams":
			message = "failed to list ksql streams"
		case "ListTables":
			message = "failed to list ksql tables"
		}
		serverError(w, op, message, err)
	}
}

var _ KsqlServicer = (*appcluster.KsqlService)(nil)
