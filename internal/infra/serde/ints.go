package serde

import (
	"encoding/binary"
	"fmt"
	"strconv"

	"github.com/cy-kaf/cy-kaf-client/internal/domain/serde"
)

// Int32 is the built-in serde for a signed 32-bit big-endian integer,
// rendered as decimal text.
type Int32 struct{}

func (Int32) Name() string        { return "Int32" }
func (Int32) Description() string { return "Signed 32-bit big-endian integer" }

func (Int32) CanDeserialize(_ string, _ serde.Target) bool { return true }
func (Int32) CanSerialize(_ string, _ serde.Target) bool   { return true }

func (Int32) Schema(_ string, _ serde.Target) (string, bool) { return "", false }

func (Int32) Serialize(_ string, _ serde.Target, input string) ([]byte, error) {
	v, err := strconv.ParseInt(input, 10, 32)
	if err != nil {
		return nil, fmt.Errorf("int32: %w", err)
	}
	buf := make([]byte, 4)
	binary.BigEndian.PutUint32(buf, uint32(v))
	return buf, nil
}

func (Int32) Deserialize(_ string, _ serde.Target, data []byte) (string, error) {
	if len(data) != 4 {
		return "", fmt.Errorf("int32: want 4 bytes, got %d", len(data))
	}
	v := int32(binary.BigEndian.Uint32(data))
	return strconv.FormatInt(int64(v), 10), nil
}

// UInt32 is the built-in serde for an unsigned 32-bit big-endian integer,
// rendered as decimal text.
type UInt32 struct{}

func (UInt32) Name() string        { return "UInt32" }
func (UInt32) Description() string { return "Unsigned 32-bit big-endian integer" }

func (UInt32) CanDeserialize(_ string, _ serde.Target) bool { return true }
func (UInt32) CanSerialize(_ string, _ serde.Target) bool   { return true }

func (UInt32) Schema(_ string, _ serde.Target) (string, bool) { return "", false }

func (UInt32) Serialize(_ string, _ serde.Target, input string) ([]byte, error) {
	v, err := strconv.ParseUint(input, 10, 32)
	if err != nil {
		return nil, fmt.Errorf("uint32: %w", err)
	}
	buf := make([]byte, 4)
	binary.BigEndian.PutUint32(buf, uint32(v))
	return buf, nil
}

func (UInt32) Deserialize(_ string, _ serde.Target, data []byte) (string, error) {
	if len(data) != 4 {
		return "", fmt.Errorf("uint32: want 4 bytes, got %d", len(data))
	}
	v := binary.BigEndian.Uint32(data)
	return strconv.FormatUint(uint64(v), 10), nil
}

// Int64 is the built-in serde for a signed 64-bit big-endian integer,
// rendered as decimal text.
type Int64 struct{}

func (Int64) Name() string        { return "Int64" }
func (Int64) Description() string { return "Signed 64-bit big-endian integer" }

func (Int64) CanDeserialize(_ string, _ serde.Target) bool { return true }
func (Int64) CanSerialize(_ string, _ serde.Target) bool   { return true }

func (Int64) Schema(_ string, _ serde.Target) (string, bool) { return "", false }

func (Int64) Serialize(_ string, _ serde.Target, input string) ([]byte, error) {
	v, err := strconv.ParseInt(input, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("int64: %w", err)
	}
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, uint64(v))
	return buf, nil
}

func (Int64) Deserialize(_ string, _ serde.Target, data []byte) (string, error) {
	if len(data) != 8 {
		return "", fmt.Errorf("int64: want 8 bytes, got %d", len(data))
	}
	v := int64(binary.BigEndian.Uint64(data))
	return strconv.FormatInt(v, 10), nil
}

// UInt64 is the built-in serde for an unsigned 64-bit big-endian integer,
// rendered as decimal text.
type UInt64 struct{}

func (UInt64) Name() string        { return "UInt64" }
func (UInt64) Description() string { return "Unsigned 64-bit big-endian integer" }

func (UInt64) CanDeserialize(_ string, _ serde.Target) bool { return true }
func (UInt64) CanSerialize(_ string, _ serde.Target) bool   { return true }

func (UInt64) Schema(_ string, _ serde.Target) (string, bool) { return "", false }

func (UInt64) Serialize(_ string, _ serde.Target, input string) ([]byte, error) {
	v, err := strconv.ParseUint(input, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("uint64: %w", err)
	}
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, v)
	return buf, nil
}

func (UInt64) Deserialize(_ string, _ serde.Target, data []byte) (string, error) {
	if len(data) != 8 {
		return "", fmt.Errorf("uint64: want 8 bytes, got %d", len(data))
	}
	v := binary.BigEndian.Uint64(data)
	return strconv.FormatUint(v, 10), nil
}
