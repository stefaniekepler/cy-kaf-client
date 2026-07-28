package ksql

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/cy-kaf/cy-kaf-client/internal/domain/cluster"
)

const maxRemoteErrorBytes = 500

const (
	ksqlExecutionErrorFallback = "KSQL execution error"
	remoteErrorFallback        = "Remote error"
)

// jsonNode retains object field order.  KSQL's dynamic response tables use
// the order supplied by the server, just like the upstream UI parser.
type jsonNode struct {
	object []jsonField
	array  []*jsonNode
	scalar any
	kind   byte // o, a, or s
}

type jsonField struct {
	name  string
	value *jsonNode
}

func decodeNode(dec *json.Decoder) (*jsonNode, error) {
	tok, err := dec.Token()
	if err != nil {
		return nil, err
	}
	return decodeNodeFromToken(dec, tok)
}

func decodeNodeFromToken(dec *json.Decoder, tok json.Token) (*jsonNode, error) {
	if delim, ok := tok.(json.Delim); ok {
		switch delim {
		case '{':
			n := &jsonNode{kind: 'o'}
			for dec.More() {
				key, err := dec.Token()
				if err != nil {
					return nil, err
				}
				name, ok := key.(string)
				if !ok {
					return nil, errors.New("invalid JSON object key")
				}
				value, err := decodeNode(dec)
				if err != nil {
					return nil, err
				}
				n.object = append(n.object, jsonField{name: name, value: value})
			}
			end, err := dec.Token()
			if err != nil {
				return nil, err
			}
			if end != json.Delim('}') {
				return nil, errors.New("invalid JSON object terminator")
			}
			return n, nil
		case '[':
			n := &jsonNode{kind: 'a'}
			for dec.More() {
				value, err := decodeNode(dec)
				if err != nil {
					return nil, err
				}
				n.array = append(n.array, value)
			}
			end, err := dec.Token()
			if err != nil {
				return nil, err
			}
			if end != json.Delim(']') {
				return nil, errors.New("invalid JSON array terminator")
			}
			return n, nil
		default:
			return nil, errors.New("unexpected JSON delimiter")
		}
	}
	return &jsonNode{kind: 's', scalar: tok}, nil
}

func (n *jsonNode) field(name string) *jsonNode {
	if n == nil || n.kind != 'o' {
		return nil
	}
	for _, f := range n.object {
		if f.name == name {
			return f.value
		}
	}
	return nil
}

func (n *jsonNode) fieldNames() []string {
	if n == nil || n.kind != 'o' {
		return nil
	}
	out := make([]string, 0, len(n.object))
	for _, f := range n.object {
		out = append(out, f.name)
	}
	return out
}

func nodeToAny(n *jsonNode) any {
	if n == nil {
		return nil
	}
	switch n.kind {
	case 'o':
		out := make(map[string]any, len(n.object))
		for _, f := range n.object {
			out[f.name] = nodeToAny(f.value)
		}
		return out
	case 'a':
		out := make([]any, len(n.array))
		for i, v := range n.array {
			out[i] = nodeToAny(v)
		}
		return out
	default:
		if number, ok := n.scalar.(json.Number); ok {
			if integer, err := strconv.ParseInt(string(number), 10, 64); err == nil {
				return integer
			}
			if decimal, err := strconv.ParseFloat(string(number), 64); err == nil {
				return decimal
			}
		}
		return n.scalar
	}
}

func nodeString(n *jsonNode) (string, bool) {
	if n == nil || n.kind != 's' {
		return "", false
	}
	v, ok := n.scalar.(string)
	return v, ok
}

func nodeBool(n *jsonNode) (bool, bool) {
	if n == nil || n.kind != 's' {
		return false, false
	}
	v, ok := n.scalar.(bool)
	return v, ok
}

func nodeText(n *jsonNode) string {
	if n == nil {
		return ""
	}
	if s, ok := nodeString(n); ok {
		return s
	}
	if n.kind == 's' {
		if n.scalar == nil {
			return ""
		}
		return fmt.Sprint(n.scalar)
	}
	b, err := json.Marshal(nodeToAny(n))
	if err != nil {
		return ""
	}
	return string(b)
}

func emitTable(emit func(cluster.KsqlTable) error, table cluster.KsqlTable) error {
	if emit == nil {
		return errors.New("ksql result emitter is nil")
	}
	return emit(table)
}

// decodeQueryResponse consumes either newline-delimited JSON values or an
// outer JSON array.  It emits rows as they arrive instead of buffering the
// potentially unbounded query result.
func decodeQueryResponse(r io.Reader, emit func(cluster.KsqlTable) error) error {
	return decodeQueryResponseWithLimit(r, emit, maxKsqlResponseFrameBytes)
}

func decodeQueryResponseWithLimit(r io.Reader, emit func(cluster.KsqlTable) error, limit int) error {
	if r == nil {
		return errors.New("empty ksql query response")
	}
	framer := newQueryFramer(r, limit)
	emitted := 0
	for {
		raw, done, err := framer.next()
		if err != nil {
			if errors.Is(err, io.EOF) {
				if emitted == 0 {
					return errors.New("empty ksql query response")
				}
				return nil
			}
			if errors.Is(err, io.ErrUnexpectedEOF) {
				if emitted == 0 && len(raw) != 0 {
					if recovered, recoverErr := recoverPartialQuery(raw, emit); recoverErr != nil {
						return recoverErr
					} else if recovered > 0 {
						return nil
					}
				}
				if framer.outerArray && emitted > 0 {
					// ksqlDB may close a streaming outer array before writing
					// its final delimiter. Once a frame was delivered, preserve
					// the existing tolerant truncation behavior.
					return nil
				}
			}
			return err
		}
		if done {
			if emitted == 0 {
				return errors.New("empty ksql query response")
			}
			return nil
		}
		node, err := parseSingleNode(raw)
		if err != nil {
			if emitted == 0 {
				if recovered, recoverErr := recoverPartialQuery(raw, emit); recoverErr != nil {
					return recoverErr
				} else if recovered > 0 {
					return nil
				}
			}
			return err
		}
		ok, err := emitQueryNode(node, emit)
		if err != nil {
			return err
		}
		if ok {
			emitted++
		}
	}
}

// A query is intentionally not capped as a whole: long-running streams may
// contain arbitrarily many small rows.  This ceiling applies to each top-level
// JSON value/array element, preventing one unusually large row from forcing an
// unbounded decoder allocation.
const maxKsqlResponseFrameBytes = 8 << 20

type queryFramer struct {
	reader      *bufio.Reader
	limit       int
	initialized bool
	outerArray  bool
	outerClosed bool
}

func newQueryFramer(r io.Reader, limit int) *queryFramer {
	if limit < 0 {
		limit = 0
	}
	return &queryFramer{reader: bufio.NewReader(r), limit: limit}
}

// next returns one complete top-level JSON value.  The framing is deliberately
// lexical rather than decoder-buffer based: a single oversized string/object
// is rejected before encoding/json can allocate an unbounded node for it.
func (f *queryFramer) next() ([]byte, bool, error) {
	if f.outerClosed {
		return nil, true, nil
	}
	if !f.initialized {
		f.initialized = true
		first, err := f.readNonSpace()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil, true, io.EOF
			}
			return nil, false, err
		}
		if first == '[' {
			f.outerArray = true
			first, err = f.readNonSpace()
			if err != nil {
				if errors.Is(err, io.EOF) {
					return nil, false, io.ErrUnexpectedEOF
				}
				return nil, false, err
			}
			if first == ']' {
				return f.finishOuterArray()
			}
			if first == ',' {
				return nil, false, errors.New("invalid JSON query array separator")
			}
			if err := f.reader.UnreadByte(); err != nil {
				return nil, false, err
			}
		} else if err := f.reader.UnreadByte(); err != nil {
			return nil, false, err
		}
	} else if f.outerArray {
		separator, err := f.readNonSpace()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil, false, io.ErrUnexpectedEOF
			}
			return nil, false, err
		}
		if separator == ']' {
			return f.finishOuterArray()
		}
		if separator != ',' {
			return nil, false, errors.New("invalid JSON query array separator")
		}
		first, err := f.readNonSpace()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil, false, io.ErrUnexpectedEOF
			}
			return nil, false, err
		}
		if first == ']' {
			return nil, false, errors.New("trailing comma in JSON query array")
		}
		if err := f.reader.UnreadByte(); err != nil {
			return nil, false, err
		}
	} else {
		if _, err := f.readNonSpace(); err != nil {
			if errors.Is(err, io.EOF) {
				return nil, true, nil
			}
			return nil, false, err
		}
		if err := f.reader.UnreadByte(); err != nil {
			return nil, false, err
		}
	}

	raw, err := f.readValue()
	return raw, false, err
}

func (f *queryFramer) finishOuterArray() ([]byte, bool, error) {
	for {
		b, err := f.reader.ReadByte()
		if errors.Is(err, io.EOF) {
			f.outerClosed = true
			return nil, true, nil
		}
		if err != nil {
			return nil, false, err
		}
		if !isJSONWhitespace(b) {
			return nil, false, errors.New("trailing JSON after ksql query response")
		}
	}
}

func (f *queryFramer) readNonSpace() (byte, error) {
	for {
		b, err := f.reader.ReadByte()
		if err != nil {
			return 0, err
		}
		if !isJSONWhitespace(b) {
			return b, nil
		}
	}
}

func (f *queryFramer) readValue() ([]byte, error) {
	first, err := f.reader.ReadByte()
	if err != nil {
		return nil, err
	}
	raw := make([]byte, 0, minInt(f.limit, 256))
	if err := f.appendByte(&raw, first); err != nil {
		return raw, err
	}
	switch first {
	case '{', '[':
		return f.readStructuredValue(raw, first)
	case '"':
		return f.readStringValue(raw)
	default:
		return f.readScalarValue(raw)
	}
}

func (f *queryFramer) readStructuredValue(raw []byte, first byte) ([]byte, error) {
	stack := []byte{first}
	inString, escaped := false, false
	for len(stack) > 0 {
		b, err := f.reader.ReadByte()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return raw, io.ErrUnexpectedEOF
			}
			return raw, err
		}
		if err := f.appendByte(&raw, b); err != nil {
			return raw, err
		}
		if inString {
			if escaped {
				escaped = false
				continue
			}
			switch b {
			case '\\':
				escaped = true
			case '"':
				inString = false
			}
			continue
		}
		switch b {
		case '"':
			inString = true
		case '{', '[':
			stack = append(stack, b)
		case '}', ']':
			if !matchingJSONDelimiter(stack[len(stack)-1], b) {
				return raw, errors.New("invalid JSON query delimiter")
			}
			stack = stack[:len(stack)-1]
		}
	}
	return raw, nil
}

func (f *queryFramer) readStringValue(raw []byte) ([]byte, error) {
	escaped := false
	for {
		b, err := f.reader.ReadByte()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return raw, io.ErrUnexpectedEOF
			}
			return raw, err
		}
		if err := f.appendByte(&raw, b); err != nil {
			return raw, err
		}
		if escaped {
			escaped = false
			continue
		}
		switch b {
		case '\\':
			escaped = true
		case '"':
			return raw, nil
		}
	}
}

func (f *queryFramer) readScalarValue(raw []byte) ([]byte, error) {
	for {
		b, err := f.reader.ReadByte()
		if errors.Is(err, io.EOF) {
			return raw, nil
		}
		if err != nil {
			return raw, err
		}
		if isJSONWhitespace(b) || b == ',' || b == ']' {
			if unreadErr := f.reader.UnreadByte(); unreadErr != nil {
				return raw, unreadErr
			}
			return raw, nil
		}
		if err := f.appendByte(&raw, b); err != nil {
			return raw, err
		}
	}
}

func (f *queryFramer) appendByte(raw *[]byte, b byte) error {
	if f.limit <= 0 || len(*raw) >= f.limit {
		return errKsqlFrameLimit
	}
	*raw = append(*raw, b)
	return nil
}

func isJSONWhitespace(b byte) bool {
	switch b {
	case ' ', '\t', '\r', '\n':
		return true
	default:
		return false
	}
}

func ensureDecoderEOF(dec *json.Decoder) error {
	var extra json.RawMessage
	if err := dec.Decode(&extra); err == io.EOF {
		return nil
	} else if err != nil {
		return err
	}
	return errors.New("trailing JSON after ksql response")
}

// recoverPartialQuery handles a ksqlDB response that contains a complete
// known event value but is missing one or more closing delimiters around a
// top-level object.  It deliberately does not search arbitrary byte offsets:
// nested objects, arrays, and strings are parsed as JSON first, so an event
// name cannot be forged by malformed content below the root.
func recoverPartialQuery(raw []byte, emit func(cluster.KsqlTable) error) (int, error) {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 || trimmed[0] != '{' {
		return 0, nil
	}
	completed, ok := appendMissingJSONClosers(trimmed)
	if !ok {
		return 0, nil
	}
	node, err := parseSingleNode(completed)
	if err != nil || node == nil || node.kind != 'o' {
		return 0, nil
	}
	emitted := 0
	for _, field := range node.object {
		if !isQueryEventName(field.name) {
			continue
		}
		event := &jsonNode{kind: 'o', object: []jsonField{field}}
		frameEmitted, emitErr := emitQueryNode(event, emit)
		if emitErr != nil {
			return emitted, emitErr
		}
		if frameEmitted {
			emitted++
		}
	}
	return emitted, nil
}

// appendMissingJSONClosers returns a repaired candidate only when the input
// is lexically a JSON prefix with unmatched delimiters.  It never repairs an
// unterminated string or mismatched delimiter, and the caller still parses the
// candidate with encoding/json before emitting anything.
func appendMissingJSONClosers(raw []byte) ([]byte, bool) {
	stack := make([]byte, 0, 4)
	inString := false
	escaped := false
	for _, ch := range raw {
		if inString {
			if escaped {
				escaped = false
				continue
			}
			switch ch {
			case '\\':
				escaped = true
			case '"':
				inString = false
			}
			continue
		}
		if ch == '"' {
			inString = true
			continue
		}
		switch ch {
		case '{', '[':
			stack = append(stack, ch)
		case '}', ']':
			if len(stack) == 0 || !matchingJSONDelimiter(stack[len(stack)-1], ch) {
				return nil, false
			}
			stack = stack[:len(stack)-1]
		}
	}
	if inString || len(stack) == 0 {
		return nil, false
	}
	completed := make([]byte, 0, len(raw)+len(stack))
	completed = append(completed, raw...)
	for i := len(stack) - 1; i >= 0; i-- {
		if stack[i] == '{' {
			completed = append(completed, '}')
		} else {
			completed = append(completed, ']')
		}
	}
	return completed, true
}

func matchingJSONDelimiter(open, close byte) bool {
	return (open == '{' && close == '}') || (open == '[' && close == ']')
}

func isQueryEventName(name string) bool {
	switch name {
	case "header", "row", "finalMessage", "errorMessage":
		return true
	default:
		return false
	}
}

func emitQueryNode(node *jsonNode, emit func(cluster.KsqlTable) error) (bool, error) {
	if node == nil || node.kind != 'o' {
		return false, nil
	}
	if message := remoteErrorField(node, "errormessage"); message != nil {
		// Query error messages are remote-controlled just like /ksql error
		// objects. Route them through the same whitelist and content filter;
		// never stringify an arbitrary JSON value into the SSE frame.
		return true, emitTable(emit, safeRemoteErrorTable(node, ksqlExecutionErrorFallback))
	}
	if header := node.field("header"); header != nil {
		schema, ok := nodeString(header.field("schema"))
		if !ok {
			return false, errors.New("ksql query header schema is not a string")
		}
		return true, emitTable(emit, cluster.KsqlTable{Header: "Schema", ColumnNames: splitSchemaColumns(schema)})
	}
	if row := node.field("row"); row != nil {
		columns := row.field("columns")
		if columns == nil || columns.kind != 'a' {
			return false, errors.New("ksql query row columns is not an array")
		}
		values := make([]any, len(columns.array))
		for i, value := range columns.array {
			values[i] = nodeToAny(value)
		}
		return true, emitTable(emit, cluster.KsqlTable{Header: "Row", Values: [][]any{values}})
	}
	if message := node.field("finalMessage"); message != nil {
		text := nodeText(message)
		if strings.TrimSpace(text) == "" {
			text = "Success"
		}
		return true, emitTable(emit, resultTable("Query Result", text))
	}
	// Heartbeats and metadata records are intentionally ignored.
	return false, nil
}

func resultTable(header, message string) cluster.KsqlTable {
	return cluster.KsqlTable{Header: header, ColumnNames: []string{"Result"}, Values: [][]any{{message}}}
}

func errorTable(message string) cluster.KsqlTable {
	message = safeErrorFallbackText(message)
	return cluster.KsqlTable{Header: "Execution error", ColumnNames: []string{"message"}, Values: [][]any{{truncateUTF8(message, maxRemoteErrorBytes)}}, IsError: true}
}

// safeRemoteErrorTable is the single response boundary for remote execution
// errors.  KSQL error objects can echo request text and implementation details
// (including credentials or local paths), so only stable diagnostic fields are
// retained and all string values share a bounded budget.
func safeRemoteErrorTable(node *jsonNode, fallback string) cluster.KsqlTable {
	columns := make([]string, 0, 3)
	values := make([]any, 0, 3)
	remaining := maxRemoteErrorBytes
	seen := make(map[string]struct{}, 3)
	appendField := func(name string, node *jsonNode) {
		canonical, ok := canonicalRemoteErrorField(name)
		if !ok {
			return
		}
		if _, exists := seen[canonical]; exists {
			return
		}
		value, ok := safeRemoteErrorValueForField(canonical, node, &remaining)
		if !ok {
			return
		}
		seen[canonical] = struct{}{}
		columns = append(columns, remoteErrorDisplayField(name, canonical))
		values = append(values, value)
	}
	if node != nil && node.kind == 'o' {
		for _, field := range node.object {
			appendField(field.name, field.value)
		}
		// Newer ksqlDB versions nest the public status and message inside
		// commandStatus. Flatten only these two whitelisted fields; all other
		// nested data remains unavailable to the SSE response.
		if commandStatus := node.field("commandStatus"); commandStatus != nil && commandStatus.kind == 'o' {
			for _, name := range []string{"status", "message"} {
				appendField(name, commandStatus.field(name))
			}
		}
	}
	if len(columns) == 0 {
		return errorTable(safeErrorFallbackText(fallback))
	}
	return cluster.KsqlTable{Header: "Execution error", ColumnNames: columns, Values: [][]any{values}, IsError: true}
}

// canonicalRemoteErrorField recognizes only the small set of fields that are
// safe to expose from a remote execution error.  Matching is case-insensitive
// so a server cannot bypass the policy with Message/ERROR_MESSAGE variants.
func canonicalRemoteErrorField(name string) (string, bool) {
	normalized := strings.ToLower(strings.ReplaceAll(strings.TrimSpace(name), "-", "_"))
	switch normalized {
	case "error_code", "errorcode":
		return normalized, true
	case "message":
		return "message", true
	case "error_message", "errormessage":
		return "errormessage", true
	case "status":
		return "status", true
	default:
		return "", false
	}
}

func remoteErrorDisplayField(original, canonical string) string {
	// Preserve the wire spelling for the established lower/camel-case fields;
	// normalize unusual case variants so a diagnostic key cannot masquerade as
	// an unrelated response column.
	switch original {
	case "error_code", "errorCode", "message", "errorMessage", "status":
		return original
	default:
		return canonical
	}
}

func remoteErrorField(node *jsonNode, wanted ...string) *jsonNode {
	if node == nil || node.kind != 'o' {
		return nil
	}
	for _, field := range node.object {
		name := strings.ToLower(strings.ReplaceAll(strings.TrimSpace(field.name), "-", "_"))
		for _, candidate := range wanted {
			if name == strings.ToLower(strings.ReplaceAll(candidate, "-", "_")) {
				return field.value
			}
		}
	}
	return nil
}

func safeRemoteErrorValueForField(field string, node *jsonNode, remaining *int) (any, bool) {
	if node == nil || node.kind != 's' {
		// A whitelisted field must still be a scalar.  Stringifying an object or
		// array could smuggle password/path fields through message/status.
		return nil, false
	}
	if field == "error_code" || field == "errorcode" {
		return safeRemoteErrorCode(node)
	}
	if text, ok := node.scalar.(string); ok {
		safeText, safe := sanitizeRemoteErrorText(text)
		if !safe {
			if field == "message" || field == "errormessage" {
				safeText = ksqlExecutionErrorFallback
			} else {
				return nil, false
			}
		}
		return consumeRemoteErrorText(safeText, remaining)
	}
	if field == "status" {
		// status may be a numeric JSON scalar, but booleans/null are not useful
		// diagnostics and are not part of the public error contract.
		if number, ok := node.scalar.(json.Number); ok {
			return safeJSONNumber(number)
		}
	}
	return nil, false
}

func safeRemoteErrorCode(node *jsonNode) (any, bool) {
	switch value := node.scalar.(type) {
	case json.Number:
		return safeJSONNumber(value)
	case string:
		value = strings.TrimSpace(value)
		if value == "" || len(value) > maxRemoteErrorBytes || !isDecimalNumber(value) {
			return nil, false
		}
		return value, true
	default:
		return nil, false
	}
}

func safeJSONNumber(value json.Number) (any, bool) {
	raw := value.String()
	if raw == "" || len(raw) > maxRemoteErrorBytes {
		return nil, false
	}
	if integer, err := strconv.ParseInt(raw, 10, 64); err == nil {
		return integer, true
	}
	if decimal, err := strconv.ParseFloat(raw, 64); err == nil {
		return decimal, true
	}
	return nil, false
}

func isDecimalNumber(value string) bool {
	if value == "" {
		return false
	}
	for i, r := range value {
		if i == 0 && (r == '-' || r == '+') {
			if len(value) == 1 {
				return false
			}
			continue
		}
		if r < '0' || r > '9' {
			return false
		}
	}
	return true
}

func consumeRemoteErrorText(text string, remaining *int) (string, bool) {
	if remaining == nil || *remaining <= 0 {
		return "", false
	}
	text = truncateUTF8(text, *remaining)
	*remaining -= len(text)
	return text, true
}

func safeErrorFallbackText(message string) string {
	if safe, ok := sanitizeRemoteErrorText(message); ok && strings.TrimSpace(safe) != "" {
		return safe
	}
	return remoteErrorFallback
}

// sanitizeRemoteErrorText rejects rather than partially redacts suspicious
// remote text.  Partial redaction is easy to bypass with casing, punctuation,
// or a truncated URL and can still expose enough of a credential/statement to
// be useful to an attacker.  Benign diagnostics remain available, bounded by
// the caller's 500-byte budget.
func sanitizeRemoteErrorText(text string) (string, bool) {
	text = strings.TrimSpace(text)
	if text == "" {
		return "", true
	}
	lower := strings.ToLower(text)
	if containsSensitiveRemoteError(lower) {
		return "", false
	}
	return text, true
}

func containsSensitiveRemoteError(lower string) bool {
	// Credential labels use conservative substring matching instead of a
	// whole-word boundary. A remote field name such as passwordHash,
	// tokenValue, or authorizationHeader is still a credential-bearing label;
	// accepting it merely because it has an identifier suffix can leak the
	// value. False positives are preferable to exposing a secret.
	credentialMarkers := []string{
		"authorization", "password", "passwd", "pwd", "secret", "token",
		"access_token", "accesstoken", "refresh_token", "refreshtoken", "api_key", "apikey", "client_secret",
		"clientsecret", "private_key", "privatekey", "secretkey", "passphrase",
	}
	if containsRemoteMarker(lower, credentialMarkers) || containsRemoteMarker(normalizeRemoteLabelSeparators(lower), credentialMarkers) {
		return true
	}
	for _, marker := range []string{"http://", "https://", "file://", "jdbc:", "://"} {
		if strings.Contains(lower, marker) {
			return true
		}
	}
	if containsLocalPath(lower) {
		return true
	}
	// Statement and SQL markers follow the same conservative policy. This
	// blocks identifier variants such as statementTextPreview and
	// sqlStatementPreview instead of relying on a boundary that suffixes can
	// bypass.
	sqlMarkers := []string{
		"statementtext", "statement_text", "sqlstatement", "sqltext", "sql", "select", "insert", "upsert",
		"create", "drop", "alter", "delete", "update", "show", "list",
		"describe", "terminate", "explain", "unset", "define", "undefine",
		"print",
	}
	normalized := normalizeRemoteLabelSeparators(lower)
	for _, word := range sqlMarkers {
		if word == "sql" {
			if containsSQLMarker(lower) {
				return true
			}
			continue
		}
		// Match complete SQL command words in prose, and identifier/separator
		// variants in the normalized label form. Using a prefix check on the
		// raw prose would reject benign status text such as "Stream created"
		// because "create" is a prefix of the past-tense word "created".
		if containsRemoteWord(lower, word) || containsRemotePrefix(normalized, word) {
			return true
		}
	}
	return false
}

func containsRemoteMarker(text string, markers []string) bool {
	for _, marker := range markers {
		if strings.Contains(text, marker) {
			return true
		}
	}
	return false
}

func containsRemotePrefix(text, marker string) bool {
	start := 0
	for start < len(text) {
		rel := strings.Index(text[start:], marker)
		if rel < 0 {
			return false
		}
		idx := start + rel
		if idx == 0 || !isRemoteWordChar(text[idx-1]) {
			return true
		}
		start = idx + len(marker)
	}
	return false
}

func normalizeRemoteLabelSeparators(text string) string {
	var b strings.Builder
	b.Grow(len(text))
	for _, r := range text {
		switch r {
		case '-', '_', '.', ':', '/', '\\', ' ', '\t', '\n', '\r':
			b.WriteByte('_')
		default:
			b.WriteRune(r)
		}
	}
	return b.String()
}

func containsSQLMarker(text string) bool {
	start := 0
	for start < len(text) {
		rel := strings.Index(text[start:], "sql")
		if rel < 0 {
			return false
		}
		idx := start + rel
		// "ksql" as a standalone product name is used by our own safe
		// fallback and HTTP diagnostics, not an echoed SQL statement. If the
		// token continues into an identifier (for example ksqlStatement),
		// reject it instead of treating it as the product name.
		if idx == 0 || text[idx-1] != 'k' {
			return true
		}
		end := idx + len("sql")
		if end < len(text) && isRemoteWordChar(text[end]) {
			return true
		}
		start = end
	}
	return false
}

func containsRemoteWord(text, word string) bool {
	start := 0
	for {
		idx := strings.Index(text[start:], word)
		if idx < 0 {
			return false
		}
		idx += start
		beforeOK := idx == 0 || !isRemoteWordChar(text[idx-1])
		end := idx + len(word)
		afterOK := end >= len(text) || !isRemoteWordChar(text[end])
		if beforeOK && afterOK {
			return true
		}
		start = end
		if start >= len(text) {
			return false
		}
	}
}

func isRemoteWordChar(ch byte) bool {
	return (ch >= 'a' && ch <= 'z') || (ch >= '0' && ch <= '9') || ch == '_'
}

func containsLocalPath(lower string) bool {
	for _, root := range []string{
		"/etc", "/var", "/tmp", "/home", "/root", "/opt", "/usr",
		"/srv", "/private", "/users", "/volumes", "/library", "/system",
		"/workspace", "/app",
	} {
		start := 0
		for {
			idx := strings.Index(lower[start:], root)
			if idx < 0 {
				break
			}
			idx += start
			end := idx + len(root)
			if end == len(lower) || !isRemoteWordChar(lower[end]) {
				return true
			}
			start = end
			if start >= len(lower) {
				break
			}
		}
	}
	for i := 0; i+2 < len(lower); i++ {
		if ((lower[i] >= 'a' && lower[i] <= 'z') || (lower[i] >= '0' && lower[i] <= '9')) &&
			lower[i+1] == ':' && (lower[i+2] == '/' || lower[i+2] == '\\') {
			return true
		}
	}
	return strings.Contains(lower, `\\`) || strings.Contains(lower, "../") || strings.Contains(lower, "./")
}

func dynamicObjectTable(header string, node *jsonNode, columns []string) cluster.KsqlTable {
	if node == nil || node.kind != 'o' {
		return cluster.KsqlTable{Header: header, ColumnNames: []string{"value"}, Values: [][]any{{nodeToAny(node)}}}
	}
	if len(columns) == 0 {
		columns = node.fieldNames()
	}
	row := make([]any, len(columns))
	for i, column := range columns {
		row[i] = nodeToAny(node.field(column))
	}
	return cluster.KsqlTable{Header: header, ColumnNames: columns, Values: [][]any{row}}
}

func dynamicArrayTable(header string, node *jsonNode) cluster.KsqlTable {
	if node == nil || node.kind != 'a' {
		return dynamicObjectTable(header, node, nil)
	}
	columns := make([]string, 0)
	for _, item := range node.array {
		if item.kind == 'o' {
			for _, name := range item.fieldNames() {
				if !containsString(columns, name) {
					columns = append(columns, name)
				}
			}
		} else if !containsString(columns, "value") {
			columns = append(columns, "value")
		}
	}
	rows := make([][]any, len(node.array))
	for i, item := range node.array {
		row := make([]any, len(columns))
		if item.kind == 'o' {
			for j, name := range columns {
				row[j] = nodeToAny(item.field(name))
			}
		} else if len(columns) == 1 && columns[0] == "value" {
			row[0] = nodeToAny(item)
		}
		rows[i] = row
	}
	return cluster.KsqlTable{Header: header, ColumnNames: columns, Values: rows}
}

func containsString(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}

// decodeKsqlResponse converts a /ksql object or array into normalized result
// tables.  Dynamic tables deliberately preserve field order and tolerate
// fields introduced by newer ksqlDB versions.
func decodeKsqlResponse(body []byte, emit func(cluster.KsqlTable) error) error {
	if len(bytes.TrimSpace(body)) == 0 {
		return emitTable(emit, resultTable("Query Result", "Success"))
	}
	dec := json.NewDecoder(bytes.NewReader(body))
	dec.UseNumber()
	root, err := decodeNode(dec)
	if err != nil {
		return err
	}
	if err := ensureDecoderEOF(dec); err != nil {
		return err
	}
	if root.kind == 'a' && len(root.array) == 0 {
		return emitTable(emit, resultTable("Query Result", "Success"))
	}
	if root.kind == 'a' {
		for _, item := range root.array {
			if err := emitKsqlNode(item, emit); err != nil {
				return err
			}
		}
		return nil
	}
	return emitKsqlNode(root, emit)
}

func emitKsqlNode(node *jsonNode, emit func(cluster.KsqlTable) error) error {
	if node == nil {
		return nil
	}
	if node.kind != 'o' {
		// The public /ksql contract is an object or array. Treat a scalar body
		// as an unsafe remote error rather than exposing arbitrary text as a
		// dynamic response value.
		return emitTable(emit, errorTable(ksqlExecutionErrorFallback))
	}
	if isRemoteErrorNode(node) {
		return emitTable(emit, safeRemoteErrorTable(node, ksqlExecutionErrorFallback))
	}
	typeName, _ := nodeString(node.field("@type"))
	var tables []cluster.KsqlTable
	switch typeName {
	case "currentStatus":
		status, message := currentStatusFields(node)
		tables = []cluster.KsqlTable{{Header: "Status", ColumnNames: []string{"status", "message"}, Values: [][]any{{safeKsqlStatusValue(status), safeKsqlStatusMessage(message)}}}}
	case "properties":
		if properties := node.field("properties"); properties != nil && properties.kind == 'a' {
			tables = append(tables, dynamicArrayTable("properties", properties))
		} else {
			tables = append(tables, dynamicObjectTable("properties", node.field("properties"), nil))
		}
		if overwritten := node.field("overwrittenProperties"); overwritten != nil {
			if overwritten.kind == 'a' {
				tables = append(tables, dynamicArrayTable("overwrittenProperties", overwritten))
			} else {
				tables = append(tables, dynamicObjectTable("overwrittenProperties", overwritten, nil))
			}
		}
	case "queries":
		tables = []cluster.KsqlTable{dynamicArrayTable("Queries", node.field("queries"))}
	case "sourceDescription":
		value := node.field("sourceDescription")
		if value == nil {
			value = node
		}
		tables = []cluster.KsqlTable{dynamicObjectTable("Source Description", value, nil)}
	case "queryDescription":
		tables = []cluster.KsqlTable{dynamicObjectTable("Queries Description", node.field("queryDescription"), nil)}
	case "topicDescription":
		tables = []cluster.KsqlTable{dynamicObjectTable("Topic Description", node, []string{"name", "kafkaTopic", "format", "schemaString"})}
	case "streams":
		tables = []cluster.KsqlTable{dynamicArrayTable("Streams", node.field("streams"))}
	case "tables":
		tables = []cluster.KsqlTable{dynamicArrayTable("Tables", node.field("tables"))}
	case "kafka_topics", "kafka_topics_extended":
		header := "Topics"
		if typeName == "kafka_topics_extended" {
			header = "Topics extended"
		}
		tables = []cluster.KsqlTable{dynamicArrayTable(header, node.field("topics"))}
	case "executionPlan":
		tables = []cluster.KsqlTable{dynamicObjectTable("Execution plan", node, []string{"executionPlanText"})}
	case "source_descriptions":
		tables = []cluster.KsqlTable{dynamicArrayTable("Source descriptions", node.field("sourceDescriptions"))}
	case "query_descriptions":
		tables = []cluster.KsqlTable{dynamicArrayTable("Queries", node.field("queryDescriptions"))}
	case "describe_function":
		tables = []cluster.KsqlTable{dynamicObjectTable("Function description", node, []string{"name", "author", "version", "description", "functions", "path", "type"})}
	case "function_names":
		tables = []cluster.KsqlTable{dynamicArrayTable("Function Names", node.field("functions"))}
	case "connector_info":
		tables = []cluster.KsqlTable{dynamicObjectTable("Connector Info", node.field("info"), nil)}
	case "drop_connector":
		tables = []cluster.KsqlTable{dynamicObjectTable("Dropped connector", node, []string{"connectorName"})}
	case "connector_list":
		tables = []cluster.KsqlTable{dynamicArrayTable("Connectors", node.field("connectors"))}
	case "connector_plugins_list":
		tables = []cluster.KsqlTable{dynamicArrayTable("Connector Plugins", node.field("connectorPlugins"))}
	case "connector_description":
		tables = []cluster.KsqlTable{dynamicObjectTable("Connector Description", node, []string{"connectorClass", "status", "sources", "topics"})}
	default:
		tables = []cluster.KsqlTable{dynamicObjectTable("Ksql Response", node, nil)}
	}
	for _, table := range tables {
		if err := emitTable(emit, table); err != nil {
			return err
		}
	}
	return nil
}

// currentStatusFields extracts the public status/message pair from both the
// legacy scalar shape and the commandStatus object emitted by newer ksqlDB
// versions. The returned nodes are intentionally left uncoerced; the safe
// scalar helpers below reject objects, arrays, and sensitive text rather than
// stringifying arbitrary nested data into an SSE frame.
func currentStatusFields(node *jsonNode) (status, message *jsonNode) {
	if node == nil || node.kind != 'o' {
		return nil, nil
	}
	status = node.field("commandStatus")
	if status == nil {
		status = node.field("status")
	}
	message = node.field("message")
	if commandStatus := node.field("commandStatus"); commandStatus != nil && commandStatus.kind == 'o' {
		if nestedStatus := commandStatus.field("status"); nestedStatus != nil {
			status = nestedStatus
		}
		if nestedMessage := commandStatus.field("message"); nestedMessage != nil {
			message = nestedMessage
		}
	}
	return status, message
}

func isRemoteErrorNode(node *jsonNode) bool {
	if node == nil || node.kind != 'o' {
		return false
	}
	if remoteErrorField(node, "error_code", "errorcode", "errormessage") != nil {
		return true
	}
	typeName, _ := nodeString(node.field("@type"))
	if strings.Contains(strings.ToLower(typeName), "error") {
		return true
	}
	if strings.EqualFold(typeName, "currentStatus") {
		status, _ := currentStatusFields(node)
		if value, ok := nodeString(status); ok && isKsqlErrorStatus(value) {
			return true
		}
	}
	// A bare message object is the shape returned by several ksqlDB HTTP
	// error paths. It must not fall through to the permissive dynamic table,
	// where arbitrary message text would become an SSE value. Objects with an
	// explicit @type (for example currentStatus) retain their normal mapping.
	return remoteErrorField(node, "message") != nil && remoteErrorField(node, "@type") == nil
}

func isKsqlErrorStatus(value string) bool {
	value = strings.ToLower(strings.TrimSpace(value))
	return value == "error" || value == "failed" || value == "failure" || strings.HasPrefix(value, "error")
}

func safeKsqlStatusMessage(node *jsonNode) any {
	if node == nil || node.kind != 's' {
		if node == nil {
			return nil
		}
		return ksqlExecutionErrorFallback
	}
	text, ok := node.scalar.(string)
	if !ok {
		return ksqlExecutionErrorFallback
	}
	text, safe := sanitizeRemoteErrorText(text)
	if !safe {
		return ksqlExecutionErrorFallback
	}
	return truncateUTF8(text, maxRemoteErrorBytes)
}

func safeKsqlStatusValue(node *jsonNode) any {
	if node == nil {
		return nil
	}
	if node.kind != 's' {
		return ksqlExecutionErrorFallback
	}
	switch value := node.scalar.(type) {
	case string:
		safeText, safe := sanitizeRemoteErrorText(value)
		if !safe {
			return ksqlExecutionErrorFallback
		}
		return truncateUTF8(safeText, maxRemoteErrorBytes)
	case json.Number:
		if safeNumber, ok := safeJSONNumber(value); ok {
			return safeNumber
		}
		return ksqlExecutionErrorFallback
	default:
		return ksqlExecutionErrorFallback
	}
}

// decodeListResponse maps the upstream dynamic Streams/Tables table into
// optional domain descriptions. wantStreams selects the required response.
func decodeListResponse(body []byte, wantStreams bool) ([]cluster.KsqlStreamDescription, []cluster.KsqlTableDescription, error) {
	root, err := parseSingleNode(body)
	if err != nil {
		return nil, nil, err
	}
	if root == nil || (root.kind != 'o' && root.kind != 'a') {
		return nil, nil, errors.New("ksql list response root is not an object or array")
	}
	nodes := []*jsonNode{root}
	if root.kind == 'a' {
		nodes = root.array
	}
	expectedType, expectedField := "tables", "tables"
	if wantStreams {
		expectedType, expectedField = "streams", "streams"
	}
	found := false
	streams := make([]cluster.KsqlStreamDescription, 0)
	tables := make([]cluster.KsqlTableDescription, 0)
	for _, node := range nodes {
		if node == nil || node.kind != 'o' {
			return nil, nil, errors.New("ksql list response row is not an object")
		}
		typeName, hasType := nodeString(node.field("@type"))
		if !hasType {
			continue
		}
		if typeName != expectedType {
			continue
		}
		found = true
		list := node.field(expectedField)
		if list == nil || list.kind != 'a' {
			return nil, nil, fmt.Errorf("ksql list field %s is not an array", expectedField)
		}
		for _, row := range list.array {
			if row == nil || row.kind != 'o' {
				return nil, nil, fmt.Errorf("ksql list field %s contains a non-object row", expectedField)
			}
			if wantStreams {
				value, err := streamFromNode(row)
				if err != nil {
					return nil, nil, err
				}
				streams = append(streams, value)
			} else {
				value, err := tableFromNode(row)
				if err != nil {
					return nil, nil, err
				}
				tables = append(tables, value)
			}
		}
	}
	if !found {
		return nil, nil, fmt.Errorf("ksql response did not contain %s", expectedType)
	}
	return streams, tables, nil
}

func optionalNodeString(object *jsonNode, field string) (*string, error) {
	node := object.field(field)
	if node == nil || (node.kind == 's' && node.scalar == nil) {
		return nil, nil
	}
	value, ok := nodeString(node)
	if !ok {
		return nil, fmt.Errorf("ksql list field %s is not a string", field)
	}
	return &value, nil
}

func optionalNodeBool(object *jsonNode, field string) (*bool, error) {
	node := object.field(field)
	if node == nil || (node.kind == 's' && node.scalar == nil) {
		return nil, nil
	}
	value, ok := nodeBool(node)
	if !ok {
		return nil, fmt.Errorf("ksql list field %s is not a boolean", field)
	}
	return &value, nil
}

func streamFromNode(row *jsonNode) (cluster.KsqlStreamDescription, error) {
	name, err := optionalNodeString(row, "name")
	if err != nil {
		return cluster.KsqlStreamDescription{}, err
	}
	topic, err := optionalNodeString(row, "topic")
	if err != nil {
		return cluster.KsqlStreamDescription{}, err
	}
	key, err := optionalNodeString(row, "keyFormat")
	if err != nil {
		return cluster.KsqlStreamDescription{}, err
	}
	value, err := optionalNodeString(row, "valueFormat")
	if err != nil {
		return cluster.KsqlStreamDescription{}, err
	}
	if value == nil {
		value, err = optionalNodeString(row, "format")
		if err != nil {
			return cluster.KsqlStreamDescription{}, err
		}
	}
	return cluster.KsqlStreamDescription{Name: name, Topic: topic, KeyFormat: key, ValueFormat: value}, nil
}

func tableFromNode(row *jsonNode) (cluster.KsqlTableDescription, error) {
	name, err := optionalNodeString(row, "name")
	if err != nil {
		return cluster.KsqlTableDescription{}, err
	}
	topic, err := optionalNodeString(row, "topic")
	if err != nil {
		return cluster.KsqlTableDescription{}, err
	}
	key, err := optionalNodeString(row, "keyFormat")
	if err != nil {
		return cluster.KsqlTableDescription{}, err
	}
	value, err := optionalNodeString(row, "valueFormat")
	if err != nil {
		return cluster.KsqlTableDescription{}, err
	}
	windowed, err := optionalNodeBool(row, "isWindowed")
	if err != nil {
		return cluster.KsqlTableDescription{}, err
	}
	return cluster.KsqlTableDescription{Name: name, Topic: topic, KeyFormat: key, ValueFormat: value, IsWindowed: windowed}, nil
}

// splitSchemaColumns returns top-level KSQL column names.  Angle-bracketed
// nested types and quoted identifiers may contain commas of their own.
func splitSchemaColumns(schema string) []string {
	parts := make([]string, 0)
	start := 0
	angle, paren, bracket := 0, 0, 0
	var quote rune
	escaped := false
	flush := func(end int) {
		part := strings.TrimSpace(schema[start:end])
		if part == "" {
			return
		}
		parts = append(parts, schemaColumnName(part))
	}
	for i, r := range schema {
		if quote != 0 {
			if escaped {
				escaped = false
				continue
			}
			if r == '\\' && quote != '`' {
				escaped = true
				continue
			}
			if r == quote {
				quote = 0
			}
			continue
		}
		switch r {
		case '`', '\'', '"':
			quote = r
		case '<':
			angle++
		case '>':
			if angle > 0 {
				angle--
			}
		case '(':
			paren++
		case ')':
			if paren > 0 {
				paren--
			}
		case '[':
			bracket++
		case ']':
			if bracket > 0 {
				bracket--
			}
		case ',':
			if angle == 0 && paren == 0 && bracket == 0 {
				flush(i)
				start = i + 1
			}
		}
	}
	flush(len(schema))
	return parts
}

func schemaColumnName(part string) string {
	part = strings.TrimSpace(part)
	if part == "" {
		return ""
	}
	if part[0] == '`' || part[0] == '"' {
		quote := part[0]
		for i := 1; i < len(part); i++ {
			if part[i] == quote {
				return part[:i+1]
			}
		}
		return part
	}
	for i, r := range part {
		if r == ' ' || r == '\t' || r == '\n' || r == '\r' {
			return part[:i]
		}
	}
	return part
}

func truncateUTF8(s string, max int) string {
	if len(s) <= max {
		return s
	}
	cut := s[:max]
	for !utf8.ValidString(cut) && len(cut) > 0 {
		cut = cut[:len(cut)-1]
	}
	return cut
}
