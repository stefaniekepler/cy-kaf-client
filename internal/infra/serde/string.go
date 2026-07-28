package serde

import "github.com/cy-kaf/cy-kaf-client/internal/domain/serde"

// String is the built-in serde that passes UTF-8 text straight through as
// raw bytes with no transformation -- the default view for arbitrary text
// payloads.
type String struct{}

func (String) Name() string        { return "String" }
func (String) Description() string { return "UTF-8 text, passed through unchanged" }

func (String) CanDeserialize(_ string, _ serde.Target) bool { return true }
func (String) CanSerialize(_ string, _ serde.Target) bool   { return true }

func (String) Schema(_ string, _ serde.Target) (string, bool) { return "", false }

func (String) Serialize(_ string, _ serde.Target, input string) ([]byte, error) {
	return []byte(input), nil
}

func (String) Deserialize(_ string, _ serde.Target, data []byte) (string, error) {
	return string(data), nil
}
