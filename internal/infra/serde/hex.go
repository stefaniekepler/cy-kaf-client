package serde

import (
	"encoding/hex"
	"fmt"

	"github.com/cy-kaf/cy-kaf-client/internal/domain/serde"
)

// Hex is the built-in serde that shows raw bytes as lowercase hexadecimal
// text.
type Hex struct{}

func (Hex) Name() string        { return "Hex" }
func (Hex) Description() string { return "Raw bytes shown as hexadecimal text" }

func (Hex) CanDeserialize(_ string, _ serde.Target) bool { return true }
func (Hex) CanSerialize(_ string, _ serde.Target) bool   { return true }

func (Hex) Schema(_ string, _ serde.Target) (string, bool) { return "", false }

func (Hex) Serialize(_ string, _ serde.Target, input string) ([]byte, error) {
	b, err := hex.DecodeString(input)
	if err != nil {
		return nil, fmt.Errorf("hex: %w", err)
	}
	return b, nil
}

func (Hex) Deserialize(_ string, _ serde.Target, data []byte) (string, error) {
	return hex.EncodeToString(data), nil
}
