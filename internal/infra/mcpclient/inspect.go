package mcpclient

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"strings"
)

const knownCodexNotFound = "Error: No MCP server named 'cy-kaf-client' found."

type Entry struct {
	Name      string
	Transport string
	Enabled   bool
	Command   string
	Args      []string
	Env       map[string]string
	EnvVars   []string
	CWD       string
}

type CodexInspector interface {
	Get(context.Context, string) (Entry, bool, error)
}

type codexInspector struct {
	runner Runner
}

func NewCodexInspector(runner Runner) CodexInspector {
	return &codexInspector{runner: runner}
}

func (inspector *codexInspector) Get(
	ctx context.Context,
	executable string,
) (Entry, bool, error) {
	if inspector == nil || inspector.runner == nil || executable == "" {
		return Entry{}, false, newCommandError(CommandInvalidResponse)
	}

	result, runErr := inspector.runner.Run(ctx, executable, CodexGetArgs())
	if len(result.Stdout) > maxCommandOutput || len(result.Stderr) > maxCommandOutput {
		return Entry{}, false, newCommandError(CommandInvalidResponse)
	}
	if result.ExitCode != 0 {
		if isKnownCodexNotFound(result, runErr) {
			return Entry{}, false, nil
		}
		return Entry{}, false, newCommandError(CommandInvalidResponse)
	}
	if runErr != nil {
		return Entry{}, false, newCommandError(CommandInvalidResponse)
	}

	entry, err := parseCodexEntry(result.Stdout)
	if err != nil {
		return Entry{}, false, newCommandError(CommandInvalidResponse)
	}
	return entry, true, nil
}

func isKnownCodexNotFound(result CommandResult, runErr error) bool {
	if result.ExitCode <= 0 || len(result.Stdout) != 0 {
		return false
	}
	if runErr != nil {
		var commandErr *CommandError
		if !errors.As(runErr, &commandErr) || commandErr.Code != CommandExitNonZero {
			return false
		}
	}
	return strings.TrimSpace(string(result.Stderr)) == knownCodexNotFound
}

func parseCodexEntry(value []byte) (Entry, error) {
	if len(value) == 0 || len(value) > maxCommandOutput {
		return Entry{}, newCommandError(CommandInvalidResponse)
	}

	raw, valid := decodeObject(value, isCodexEntryField)
	if !valid ||
		len(raw["name"]) == 0 ||
		len(raw["enabled"]) == 0 ||
		len(raw["transport"]) == 0 ||
		!validCodexPresentationFields(raw) {
		return Entry{}, newCommandError(CommandInvalidResponse)
	}

	var (
		name    string
		enabled *bool
	)
	transport, valid := decodeObject(raw["transport"], isCodexTransportField)
	if json.Unmarshal(raw["name"], &name) != nil ||
		json.Unmarshal(raw["enabled"], &enabled) != nil ||
		enabled == nil ||
		!valid ||
		name != ServerName {
		return Entry{}, newCommandError(CommandInvalidResponse)
	}
	for _, field := range []string{"type", "command", "args", "env", "env_vars", "cwd"} {
		if len(transport[field]) == 0 {
			return Entry{}, newCommandError(CommandInvalidResponse)
		}
	}

	var (
		transportType string
		command       string
		args          []string
		envVars       []string
		cwd           *string
	)
	env, validEnv := decodeStringMap(transport["env"])
	if json.Unmarshal(transport["type"], &transportType) != nil ||
		json.Unmarshal(transport["command"], &command) != nil ||
		json.Unmarshal(transport["args"], &args) != nil ||
		!validEnv ||
		json.Unmarshal(transport["env_vars"], &envVars) != nil ||
		json.Unmarshal(transport["cwd"], &cwd) != nil ||
		transportType != "stdio" ||
		command == "" ||
		args == nil ||
		envVars == nil {
		return Entry{}, newCommandError(CommandInvalidResponse)
	}

	entry := Entry{
		Name:      name,
		Transport: transportType,
		Enabled:   *enabled,
		Command:   command,
		Args:      cloneStrings(args),
		Env:       cloneStringMap(env),
		EnvVars:   cloneStrings(envVars),
	}
	if cwd != nil {
		entry.CWD = *cwd
	}
	return entry, nil
}

func validCodexPresentationFields(raw map[string]json.RawMessage) bool {
	return validNullableString(raw["disabled_reason"]) &&
		validNullableStrings(raw["enabled_tools"]) &&
		validNullableStrings(raw["disabled_tools"]) &&
		validNullableNumber(raw["startup_timeout_sec"]) &&
		validNullableNumber(raw["tool_timeout_sec"])
}

func validNullableString(raw json.RawMessage) bool {
	if len(raw) == 0 {
		return true
	}
	var value *string
	return json.Unmarshal(raw, &value) == nil
}

func validNullableStrings(raw json.RawMessage) bool {
	if len(raw) == 0 {
		return true
	}
	var value []string
	return json.Unmarshal(raw, &value) == nil
}

func validNullableNumber(raw json.RawMessage) bool {
	if len(raw) == 0 {
		return true
	}
	var value *float64
	return json.Unmarshal(raw, &value) == nil
}

func decodeObject(
	value []byte,
	allowed func(string) bool,
) (map[string]json.RawMessage, bool) {
	decoder := json.NewDecoder(bytes.NewReader(value))
	token, err := decoder.Token()
	if err != nil || token != json.Delim('{') {
		return nil, false
	}

	decoded := make(map[string]json.RawMessage)
	for decoder.More() {
		token, err = decoder.Token()
		key, ok := token.(string)
		if err != nil || !ok || !allowed(key) {
			return nil, false
		}
		if _, duplicate := decoded[key]; duplicate {
			return nil, false
		}

		var raw json.RawMessage
		if decoder.Decode(&raw) != nil {
			return nil, false
		}
		decoded[key] = raw
	}
	token, err = decoder.Token()
	if err != nil || token != json.Delim('}') {
		return nil, false
	}
	var trailing json.RawMessage
	if decoder.Decode(&trailing) != io.EOF {
		return nil, false
	}
	return decoded, true
}

func isCodexEntryField(name string) bool {
	switch name {
	case "name",
		"enabled",
		"disabled_reason",
		"transport",
		"enabled_tools",
		"disabled_tools",
		"startup_timeout_sec",
		"tool_timeout_sec":
		return true
	default:
		return false
	}
}

func isCodexTransportField(name string) bool {
	switch name {
	case "type", "command", "args", "env", "env_vars", "cwd":
		return true
	default:
		return false
	}
}

func decodeStringMap(value []byte) (map[string]string, bool) {
	if bytes.Equal(bytes.TrimSpace(value), []byte("null")) {
		return nil, true
	}

	decoder := json.NewDecoder(bytes.NewReader(value))
	token, err := decoder.Token()
	if err != nil || token != json.Delim('{') {
		return nil, false
	}
	decoded := make(map[string]string)
	for decoder.More() {
		token, err = decoder.Token()
		key, ok := token.(string)
		if err != nil || !ok {
			return nil, false
		}
		if _, duplicate := decoded[key]; duplicate {
			return nil, false
		}
		var mapValue string
		if decoder.Decode(&mapValue) != nil {
			return nil, false
		}
		decoded[key] = mapValue
	}
	token, err = decoder.Token()
	if err != nil || token != json.Delim('}') {
		return nil, false
	}
	var trailing json.RawMessage
	if decoder.Decode(&trailing) != io.EOF {
		return nil, false
	}
	return decoded, true
}

func cloneStringMap(source map[string]string) map[string]string {
	if source == nil {
		return nil
	}
	cloned := make(map[string]string, len(source))
	for key, value := range source {
		cloned[key] = value
	}
	return cloned
}

func cloneStrings(source []string) []string {
	if source == nil {
		return nil
	}
	cloned := make([]string, len(source))
	copy(cloned, source)
	return cloned
}
