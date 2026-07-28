package serde

import (
	"encoding/hex"
	"fmt"

	"github.com/cy-kaf/cy-kaf-client/internal/domain/serde"
)

// UUIDBinary is the built-in serde that shows a 16-byte value as the
// canonical dashed lowercase textual form (RFC 4122's "8-4-4-4-12" layout,
// e.g. what Kafka Streams' Uuid (de)serializer produces/expects).
//
// Parsing/formatting is handwritten rather than pulled from a UUID library:
// the layout is a fixed 36-character shape, so a dependency buys nothing
// here. github.com/google/uuid is already in go.mod, but only as an
// indirect dependency (pulled in transitively by testcontainers/otel) --
// this package deliberately doesn't promote it to a direct one.
type UUIDBinary struct{}

func (UUIDBinary) Name() string { return "UUIDBinary" }
func (UUIDBinary) Description() string {
	return "16-byte value shown as a canonical dashed UUID string"
}

func (UUIDBinary) CanDeserialize(_ string, _ serde.Target) bool { return true }
func (UUIDBinary) CanSerialize(_ string, _ serde.Target) bool   { return true }

func (UUIDBinary) Schema(_ string, _ serde.Target) (string, bool) { return "", false }

func (UUIDBinary) Serialize(_ string, _ serde.Target, input string) ([]byte, error) {
	return parseUUID(input)
}

func (UUIDBinary) Deserialize(_ string, _ serde.Target, data []byte) (string, error) {
	if len(data) != 16 {
		return "", fmt.Errorf("uuid: want 16 bytes, got %d", len(data))
	}
	return formatUUID(data), nil
}

// uuidDashPositions are the canonical "8-4-4-4-12" layout's dash indices
// within the 36-character textual form.
var uuidDashPositions = [4]int{8, 13, 18, 23}

// parseUUID parses s (expected canonical "8-4-4-4-12" form, case-
// insensitive on the hex digits) into its 16 raw bytes. Every rejection
// (wrong length, a dash in the wrong place, non-hex payload) returns an
// error rather than panicking.
func parseUUID(s string) ([]byte, error) {
	if len(s) != 36 {
		return nil, fmt.Errorf("uuid: want a 36-character canonical string, got %d characters", len(s))
	}
	for _, pos := range uuidDashPositions {
		if s[pos] != '-' {
			return nil, fmt.Errorf("uuid: expected '-' at position %d", pos)
		}
	}
	hexDigits := s[0:8] + s[9:13] + s[14:18] + s[19:23] + s[24:36]
	b, err := hex.DecodeString(hexDigits)
	if err != nil {
		return nil, fmt.Errorf("uuid: %w", err)
	}
	return b, nil
}

// formatUUID renders b's 16 bytes (b must have length 16 -- callers check)
// as the canonical lowercase "8-4-4-4-12" dashed string.
func formatUUID(b []byte) string {
	h := hex.EncodeToString(b)
	return h[0:8] + "-" + h[8:12] + "-" + h[12:16] + "-" + h[16:20] + "-" + h[20:32]
}
