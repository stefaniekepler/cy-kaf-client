package serde

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/cy-kaf/cy-kaf-client/internal/domain/serde"
)

// validUUIDExample is the brief's canonical fixed value for the UUIDBinary
// codec's 16-byte <-> canonical-string mapping (RFC 4122 "8-4-4-4-12"
// lowercase dashed layout).
const validUUIDExample = "00112233-4455-6677-8899-aabbccddeeff"

// TestCodecRoundTrip is the table-driven round trip the brief asks for on
// every one of the 8 built-in codecs: Serialize(input) must produce
// exactly wantBytes, and Deserialize(wantBytes) must reproduce exactly
// input (self-consistent / "自反"). topic/target never affect a built-in
// codec's result, so every case passes topic="" and TargetValue.
func TestCodecRoundTrip(t *testing.T) {
	cases := []struct {
		name      string
		codec     serde.Serde
		input     string
		wantBytes []byte
	}{
		{
			name:      "String passthrough round trip including non-ASCII text",
			codec:     String{},
			input:     "héllo 你好",
			wantBytes: []byte("héllo 你好"),
		},
		{
			name:      "Int32 encodes decimal as 4-byte big-endian (brief case 1)",
			codec:     Int32{},
			input:     "1",
			wantBytes: []byte{0, 0, 0, 1},
		},
		{
			name:      "UInt32 4-byte big-endian round trip",
			codec:     UInt32{},
			input:     "42",
			wantBytes: []byte{0, 0, 0, 42},
		},
		{
			name:      "Int64 8-byte big-endian round trip",
			codec:     Int64{},
			input:     "1",
			wantBytes: []byte{0, 0, 0, 0, 0, 0, 0, 1},
		},
		{
			name:      "UInt64 8-byte big-endian round trip",
			codec:     UInt64{},
			input:     "42",
			wantBytes: []byte{0, 0, 0, 0, 0, 0, 0, 42},
		},
		{
			name:      "Base64 standard encoding round trip",
			codec:     Base64{},
			input:     "aGVsbG8=",
			wantBytes: []byte("hello"),
		},
		{
			name:      "Hex round trip",
			codec:     Hex{},
			input:     "68656c6c6f",
			wantBytes: []byte("hello"),
		},
		{
			name:      "UUIDBinary 16-byte <-> canonical string round trip (brief case 3)",
			codec:     UUIDBinary{},
			input:     validUUIDExample,
			wantBytes: []byte{0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := tc.codec.Serialize("", serde.TargetValue, tc.input)
			require.NoError(t, err)
			require.Equal(t, tc.wantBytes, got)

			back, err := tc.codec.Deserialize("", serde.TargetValue, tc.wantBytes)
			require.NoError(t, err)
			require.Equal(t, tc.input, back)
		})
	}
}

// TestInt32SerializeFixedValue pins brief case 1 as its own narrowly-named
// test (in addition to being the first row of TestCodecRoundTrip above), so
// the exact literal the brief calls out is traceable to one specific test.
func TestInt32SerializeFixedValue(t *testing.T) {
	got, err := Int32{}.Serialize("", serde.TargetValue, "1")
	require.NoError(t, err)
	require.Equal(t, []byte{0, 0, 0, 1}, got)
}

// TestSignedVsUnsignedDeserializeDiverge pins brief case 2: the same 8
// 0xFF bytes must deserialize to two different decimal strings depending on
// whether the codec is signed (Int64, two's-complement -1) or unsigned
// (UInt64, 2^64-1) -- the fork that proves sign-handling is actually wired
// up rather than one codec silently reusing the other's logic.
func TestSignedVsUnsignedDeserializeDiverge(t *testing.T) {
	allFF := []byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF}

	gotUnsigned, err := UInt64{}.Deserialize("", serde.TargetValue, allFF)
	require.NoError(t, err)
	require.Equal(t, "18446744073709551615", gotUnsigned)

	gotSigned, err := Int64{}.Deserialize("", serde.TargetValue, allFF)
	require.NoError(t, err)
	require.Equal(t, "-1", gotSigned)
}

// TestUUIDBinarySerializeProduces16Bytes pins brief case 3's explicit
// length assertion (the round-trip table above already proves the byte
// values, but the brief calls out "长度 == 16" as its own assertion).
func TestUUIDBinarySerializeProduces16Bytes(t *testing.T) {
	got, err := UUIDBinary{}.Serialize("", serde.TargetValue, validUUIDExample)
	require.NoError(t, err)
	require.Len(t, got, 16)

	back, err := UUIDBinary{}.Deserialize("", serde.TargetValue, got)
	require.NoError(t, err)
	require.Equal(t, validUUIDExample, back)
}

// TestInt32DeserializeWrongLengthReturnsError pins brief case 6: 3 bytes
// (not 4) must return an error, never panic.
func TestInt32DeserializeWrongLengthReturnsError(t *testing.T) {
	_, err := Int32{}.Deserialize("", serde.TargetValue, []byte{1, 2, 3})
	require.Error(t, err)
}

// TestNumericDeserializeRejectsWrongLength extends brief case 6's
// error-not-panic requirement to the other 3 fixed-width numeric codecs
// (Int32 itself is pinned individually above), so every numeric codec's
// length-guard branch is actually exercised, not just assumed symmetric
// with Int32.
func TestNumericDeserializeRejectsWrongLength(t *testing.T) {
	cases := []struct {
		name  string
		codec serde.Serde
		data  []byte
	}{
		{name: "UInt32 wrong length", codec: UInt32{}, data: []byte{1, 2, 3}},
		{name: "Int64 wrong length", codec: Int64{}, data: []byte{1, 2, 3, 4, 5, 6, 7}},
		{name: "UInt64 wrong length", codec: UInt64{}, data: []byte{1, 2, 3, 4, 5, 6, 7}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := tc.codec.Deserialize("", serde.TargetValue, tc.data)
			require.Error(t, err)
		})
	}
}

// TestNumericSerializeRejectsInvalidInput exercises each numeric codec's
// other error branch (Serialize's parse failure), which the brief's 7
// enumerated cases don't individually call out but the Global Constraints
// section's "每 codec ... 错误分支需覆盖" requirement does.
func TestNumericSerializeRejectsInvalidInput(t *testing.T) {
	cases := []struct {
		name  string
		codec serde.Serde
		input string
	}{
		{name: "Int32 non-numeric input", codec: Int32{}, input: "not-a-number"},
		{name: "UInt32 rejects negative (unsigned)", codec: UInt32{}, input: "-1"},
		{name: "Int64 non-numeric input", codec: Int64{}, input: "not-a-number"},
		{name: "UInt64 rejects negative (unsigned)", codec: UInt64{}, input: "-1"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := tc.codec.Serialize("", serde.TargetValue, tc.input)
			require.Error(t, err)
		})
	}
}

// TestBase64AndHexSerializeRejectInvalidInput covers Base64/Hex's Serialize
// error branch (Deserialize never fails for these two -- any byte slice has
// a valid base64/hex text form, so there is no error branch to exercise
// there).
func TestBase64AndHexSerializeRejectInvalidInput(t *testing.T) {
	_, err := Base64{}.Serialize("", serde.TargetValue, "not valid base64!!")
	require.Error(t, err)

	_, err = Hex{}.Serialize("", serde.TargetValue, "not-hex-zz")
	require.Error(t, err)
}

// TestUUIDBinarySerializeRejectsInvalidInput exercises all 3 of parseUUID's
// distinct error returns: wrong overall length, a dash landing in the wrong
// position, and non-hex characters in the hex payload.
func TestUUIDBinarySerializeRejectsInvalidInput(t *testing.T) {
	cases := []struct {
		name  string
		input string
	}{
		{name: "wrong length", input: validUUIDExample[:35]},
		{name: "dash in wrong position", input: validUUIDExample[:8] + "x" + validUUIDExample[9:]},
		{name: "non-hex characters", input: "zz112233-4455-6677-8899-aabbccddeeff"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := UUIDBinary{}.Serialize("", serde.TargetValue, tc.input)
			require.Error(t, err)
		})
	}
}

// TestUUIDBinaryDeserializeRejectsWrongLength extends brief case 6's
// error-not-panic requirement to UUIDBinary's Deserialize, which the brief
// groups together with the numeric codecs ("数值/UUID serde 的 Deserialize
// 收到长度不符的字节 → 返回错误").
func TestUUIDBinaryDeserializeRejectsWrongLength(t *testing.T) {
	_, err := UUIDBinary{}.Deserialize("", serde.TargetValue, []byte{1, 2, 3})
	require.Error(t, err)
}

// TestBuiltinCodecsTrivialContracts locks the brief's stated invariants for
// every built-in codec at once: CanDeserialize/CanSerialize are always
// true, Schema is always ("", false), and Name/Description are non-empty
// -- regardless of topic or target.
func TestBuiltinCodecsTrivialContracts(t *testing.T) {
	for _, s := range NewRegistry().All() {
		t.Run(s.Name(), func(t *testing.T) {
			require.NotEmpty(t, s.Name())
			require.NotEmpty(t, s.Description())
			require.True(t, s.CanDeserialize("any-topic", serde.TargetKey))
			require.True(t, s.CanSerialize("any-topic", serde.TargetValue))

			schema, ok := s.Schema("any-topic", serde.TargetKey)
			require.False(t, ok)
			require.Empty(t, schema)
		})
	}
}

// TestRegistryAllAndGet pins brief case 7: All() returns exactly the 8
// built-ins, Get hits a known name and misses an unknown one with
// (nil, false).
func TestRegistryAllAndGet(t *testing.T) {
	r := NewRegistry()

	require.Len(t, r.All(), 8)

	got, ok := r.Get("Int64")
	require.True(t, ok)
	require.Equal(t, "Int64", got.Name())

	got, ok = r.Get("Nope")
	require.False(t, ok)
	require.Nil(t, got)
}

// TestRegistryAllPreservesRegistrationOrder locks registration order (which
// doubles as display order, per registry.go's doc comment) to the exact
// listing the brief gives: String/Int32/Int64/UInt32/UInt64/Base64/Hex/UUIDBinary.
func TestRegistryAllPreservesRegistrationOrder(t *testing.T) {
	want := []string{"String", "Int32", "Int64", "UInt32", "UInt64", "Base64", "Hex", "UUIDBinary"}
	got := make([]string, 0, len(want))
	for _, s := range NewRegistry().All() {
		got = append(got, s.Name())
	}
	require.Equal(t, want, got)
}
