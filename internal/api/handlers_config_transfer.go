package api

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"

	"github.com/cy-kaf/cy-kaf-client/internal/api/generated"
	"github.com/cy-kaf/cy-kaf-client/internal/domain/cluster"
)

type ConfigTransferServicer interface {
	Export() ([]byte, error)
	Preview([]byte) (cluster.ConfigImportPreview, error)
	Import(context.Context, []byte, []int, string) (cluster.ConfigImportResult, error)
}

func transferError(w http.ResponseWriter, err error) {
	code, message := http.StatusInternalServerError, "配置读写失败，请重试"
	var invalid *cluster.ConfigImportError
	if errors.As(err, &invalid) {
		code, message = http.StatusBadRequest, invalid.Message
	}
	if errors.Is(err, cluster.ErrConfigChanged) {
		code, message = http.StatusConflict, cluster.ErrConfigChanged.Error()
	}
	writeJSON(w, code, errorResponse(code, message))
}
func decodeTransfer(w http.ResponseWriter, r *http.Request, target any) bool {
	w.Header().Set("Cache-Control", "no-store")
	r.Body = http.MaxBytesReader(w, r.Body, 12<<20)
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		writeJSON(w, 400, errorResponse(400, "导入请求无效或文件过大"))
		return false
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		writeJSON(w, 400, errorResponse(400, "导入请求必须是单个对象"))
		return false
	}
	return true
}
func (s *apiServer) ExportKafkaConfig(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	content, err := s.deps.ConfigTransfer.Export()
	if err != nil {
		transferError(w, err)
		return
	}
	w.Header().Set("Content-Type", "application/yaml")
	w.Header().Set("Content-Disposition", `attachment; filename="kafka-environments.yaml"`)
	w.Header().Set("X-Content-Type-Options", "nosniff")
	_, _ = w.Write(content)
}
func (s *apiServer) PreviewKafkaConfigImport(w http.ResponseWriter, r *http.Request) {
	var req generated.ConfigImportFile
	if !decodeTransfer(w, r, &req) {
		return
	}
	preview, err := s.deps.ConfigTransfer.Preview([]byte(req.Content))
	if err != nil {
		transferError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, preview)
}
func (s *apiServer) ImportKafkaConfig(w http.ResponseWriter, r *http.Request) {
	var req generated.ConfigImportSelection
	if !decodeTransfer(w, r, &req) {
		return
	}
	if req.Selected == nil || req.Revision == "" {
		writeJSON(w, 400, errorResponse(400, "缺少导入选择或预览版本"))
		return
	}
	result, err := s.deps.ConfigTransfer.Import(r.Context(), []byte(req.Content), req.Selected, req.Revision)
	if err != nil {
		transferError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, result)
}
