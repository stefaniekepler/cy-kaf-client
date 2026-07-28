package main

import (
	"context"
	"errors"
	"io"
	"log/slog"
)

const (
	mcpLogStart = "MCP STDIO starting"
	mcpLogExit  = "MCP process exited"
)

type mcpRunError struct {
	code  string
	cause error
}

func (e *mcpRunError) Error() string {
	return "MCP failed: " + e.code
}

func (e *mcpRunError) Unwrap() error {
	return e.cause
}

func mcpFailure(code string, cause error) error {
	if !validMCPLogCode(code) {
		code = "INTERNAL_FAILED"
	}
	return &mcpRunError{code: code, cause: cause}
}

func mcpErrorCode(err error) string {
	var runErr *mcpRunError
	if errors.As(err, &runErr) && validMCPLogCode(runErr.code) {
		return runErr.code
	}
	return "INTERNAL_FAILED"
}

func logMCPExit(err error) {
	slog.Error(mcpLogExit, "code", mcpErrorCode(err))
}

func configureMCPLogging(debug bool, output io.Writer) {
	slog.SetDefault(newMCPLogger(debug, output))
}

func newMCPLogger(debug bool, output io.Writer) *slog.Logger {
	level := slog.LevelInfo
	if debug {
		level = slog.LevelDebug
	}
	return slog.New(&mcpSafeHandler{
		next: slog.NewTextHandler(output, &slog.HandlerOptions{Level: level}),
	})
}

// mcpSafeHandler is deliberately an allowlist rather than a redactor. MCP
// component and SDK errors can carry requests, endpoints, cluster names, or
// credentials in arbitrary attributes, so unrecognized records and
// attributes are dropped before they reach stderr.
type mcpSafeHandler struct {
	next slog.Handler
}

func (h *mcpSafeHandler) Enabled(ctx context.Context, level slog.Level) bool {
	return h.next.Enabled(ctx, level)
}

func (h *mcpSafeHandler) Handle(ctx context.Context, record slog.Record) error {
	safe := slog.NewRecord(record.Time, record.Level, record.Message, record.PC)
	switch record.Message {
	case mcpLogStart:
		tools, ok := safeToolCount(record)
		if !ok {
			return nil
		}
		safe.AddAttrs(slog.Int("tools", tools))
	case mcpLogExit:
		code, ok := safeStageCode(record)
		if !ok {
			return nil
		}
		safe.AddAttrs(slog.String("code", code))
	default:
		return nil
	}
	return h.next.Handle(ctx, safe)
}

func (h *mcpSafeHandler) WithAttrs([]slog.Attr) slog.Handler {
	return h
}

func (h *mcpSafeHandler) WithGroup(string) slog.Handler {
	return h
}

func safeToolCount(record slog.Record) (int, bool) {
	count := 0
	found := false
	record.Attrs(func(attr slog.Attr) bool {
		if attr.Key != "tools" || attr.Value.Kind() != slog.KindInt64 {
			return true
		}
		value := attr.Value.Int64()
		if value < 0 || value > 84 {
			return false
		}
		count = int(value)
		found = true
		return true
	})
	return count, found
}

func safeStageCode(record slog.Record) (string, bool) {
	code := ""
	record.Attrs(func(attr slog.Attr) bool {
		if attr.Key == "code" && attr.Value.Kind() == slog.KindString {
			code = attr.Value.String()
		}
		return true
	})
	return code, validMCPLogCode(code)
}

func validMCPLogCode(code string) bool {
	switch code {
	case "RUNTIME_INVALID",
		"OPTIONS_INVALID",
		"CONFIG_INVALID",
		"POLICY_INVALID",
		"POLICY_DISABLED",
		"WIRING_FAILED",
		"SERVER_FAILED",
		"TRANSPORT_FAILED",
		"CANCELLED",
		"INTERNAL_FAILED":
		return true
	default:
		return false
	}
}

var _ error = (*mcpRunError)(nil)
var _ slog.Handler = (*mcpSafeHandler)(nil)
