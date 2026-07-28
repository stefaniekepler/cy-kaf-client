package api

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
)

const requestMaxBodyBytes int64 = 10 << 20

// decodeStrictJSON accepts exactly one non-null JSON value within the shared
// request-size limit. It deliberately leaves unknown-field compatibility to
// each generated contract type.
func decodeStrictJSON[T any](w http.ResponseWriter, r *http.Request) (*T, bool) {
	r.Body = http.MaxBytesReader(w, r.Body, requestMaxBodyBytes)
	decoder := json.NewDecoder(r.Body)
	var body *T
	if err := decoder.Decode(&body); err != nil || body == nil {
		writeJSON(w, http.StatusBadRequest, errorResponse(http.StatusBadRequest, "invalid request body"))
		return nil, false
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		writeJSON(w, http.StatusBadRequest, errorResponse(http.StatusBadRequest, "invalid request body"))
		return nil, false
	}
	return body, true
}
