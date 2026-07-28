package serde

import (
	"encoding/base64"
	"fmt"

	"github.com/cy-kaf/cy-kaf-client/internal/domain/serde"
)

// Base64 is the built-in serde that shows raw bytes as standard (RFC 4648,
// padded) base64 text.
type Base64 struct{}

func (Base64) Name() string        { return "Base64" }
func (Base64) Description() string { return "Raw bytes shown as standard base64 text" }

func (Base64) CanDeserialize(_ string, _ serde.Target) bool { return true }
func (Base64) CanSerialize(_ string, _ serde.Target) bool   { return true }

func (Base64) Schema(_ string, _ serde.Target) (string, bool) { return "", false }

func (Base64) Serialize(_ string, _ serde.Target, input string) ([]byte, error) {
	b, err := base64.StdEncoding.DecodeString(input)
	if err != nil {
		return nil, fmt.Errorf("base64: %w", err)
	}
	return b, nil
}

func (Base64) Deserialize(_ string, _ serde.Target, data []byte) (string, error) {
	return base64.StdEncoding.EncodeToString(data), nil
}
