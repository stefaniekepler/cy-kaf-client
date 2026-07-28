package serde_test

import (
	"context"
	"encoding/binary"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/cy-kaf/cy-kaf-client/internal/domain/cluster"
	domainserde "github.com/cy-kaf/cy-kaf-client/internal/domain/serde"
	infraserde "github.com/cy-kaf/cy-kaf-client/internal/infra/serde"
)

// fakeSRPort is a minimal cluster.SchemaRegistryPort for the SR-serde tests:
// SchemaByID/SchemaByVersion serve canned schemas; every other method is an
// unused stub (the serde only calls those two). Keyed loosely (one schema per
// id / one per "latest") because each test drives a single subject.
type fakeSRPort struct {
	byID           map[int]cluster.RawSchema
	byIDErr        error
	latest         cluster.SchemaVersion
	latestErr      error
	lastVerSubject string
	versionCalls   map[string]int
}

func (f *fakeSRPort) SchemaByID(_ context.Context, _ cluster.Definition, id int) (cluster.RawSchema, error) {
	if f.byIDErr != nil {
		return cluster.RawSchema{}, f.byIDErr
	}
	rs, ok := f.byID[id]
	if !ok {
		return cluster.RawSchema{}, fmt.Errorf("fakeSRPort: no schema id %d", id)
	}
	return rs, nil
}

func (f *fakeSRPort) SchemaByVersion(_ context.Context, _ cluster.Definition, subject, _ string) (cluster.SchemaVersion, error) {
	f.lastVerSubject = subject
	if f.versionCalls != nil {
		f.versionCalls[subject]++
	}
	if f.latestErr != nil {
		return cluster.SchemaVersion{}, f.latestErr
	}
	return f.latest, nil
}

// unused SchemaRegistryPort methods
func (f *fakeSRPort) Subjects(context.Context, cluster.Definition) ([]string, error) { return nil, nil }
func (f *fakeSRPort) Versions(context.Context, cluster.Definition, string) ([]int, error) {
	return nil, nil
}
func (f *fakeSRPort) Register(context.Context, cluster.Definition, string, cluster.NewSchema) (int, error) {
	return 0, nil
}
func (f *fakeSRPort) DeleteSubject(context.Context, cluster.Definition, string, bool) ([]int, error) {
	return nil, nil
}
func (f *fakeSRPort) DeleteVersion(context.Context, cluster.Definition, string, string, bool) (int, error) {
	return 0, nil
}
func (f *fakeSRPort) GlobalCompat(context.Context, cluster.Definition) (string, error) {
	return "", nil
}
func (f *fakeSRPort) SetGlobalCompat(context.Context, cluster.Definition, string) error { return nil }
func (f *fakeSRPort) SubjectCompat(context.Context, cluster.Definition, string) (string, error) {
	return "", nil
}
func (f *fakeSRPort) SetSubjectCompat(context.Context, cluster.Definition, string, string) error {
	return nil
}
func (f *fakeSRPort) CheckCompat(context.Context, cluster.Definition, string, cluster.NewSchema) (bool, error) {
	return false, nil
}

var _ cluster.SchemaRegistryPort = (*fakeSRPort)(nil)

const avroUserSchema = `{"type":"record","name":"User","fields":[{"name":"name","type":"string"},{"name":"city","type":"string"}]}`

func srDef() cluster.Definition {
	return cluster.Definition{Name: "prod", SchemaRegistry: cluster.SchemaRegistrySpec{URL: "http://sr:8085"}}
}

func TestSchemaRegistrySerdeAvroRoundTrip(t *testing.T) {
	port := &fakeSRPort{
		byID:   map[int]cluster.RawSchema{5: {Schema: avroUserSchema, SchemaType: "AVRO"}},
		latest: cluster.SchemaVersion{ID: 5, Schema: avroUserSchema, SchemaType: "AVRO"},
	}
	s := infraserde.NewSchemaRegistrySerde(port, srDef())

	bytes, err := s.Serialize("t", domainserde.TargetValue, `{"name":"alice","city":"NYC"}`)
	require.NoError(t, err)
	require.Equal(t, byte(0x00), bytes[0])                           // confluent magic
	require.Equal(t, uint32(5), binary.BigEndian.Uint32(bytes[1:5])) // schema id
	require.Equal(t, "t-value", port.lastVerSubject)                 // TopicNameStrategy subject

	text, err := s.Deserialize("t", domainserde.TargetValue, bytes)
	require.NoError(t, err)
	require.JSONEq(t, `{"name":"alice","city":"NYC"}`, text)
}

func TestSchemaRegistrySerdeKeyUsesKeySubject(t *testing.T) {
	port := &fakeSRPort{latest: cluster.SchemaVersion{ID: 1, Schema: avroUserSchema, SchemaType: "AVRO"}}
	s := infraserde.NewSchemaRegistrySerde(port, srDef())
	_, err := s.Serialize("t", domainserde.TargetKey, `{"name":"a","city":"b"}`)
	require.NoError(t, err)
	require.Equal(t, "t-key", port.lastVerSubject)
}

func TestSchemaRegistrySerdeJSONSchemaStripsHeader(t *testing.T) {
	port := &fakeSRPort{byID: map[int]cluster.RawSchema{9: {Schema: `{"type":"object"}`, SchemaType: "JSON"}}}
	s := infraserde.NewSchemaRegistrySerde(port, srDef())

	header := []byte{0x00, 0x00, 0x00, 0x00, 0x09}
	data := append(header, []byte(`{"hello":"world"}`)...)
	text, err := s.Deserialize("t", domainserde.TargetValue, data)
	require.NoError(t, err)
	require.JSONEq(t, `{"hello":"world"}`, text)
}

func TestSchemaRegistrySerdeDeserializeErrors(t *testing.T) {
	cases := []struct {
		name string
		port *fakeSRPort
		data []byte
	}{
		{"too short", &fakeSRPort{}, []byte{0x00, 0x00}},
		{"bad magic", &fakeSRPort{}, []byte{0x01, 0x00, 0x00, 0x00, 0x05, 0x42}},
		{"schema fetch fails", &fakeSRPort{byIDErr: fmt.Errorf("404")}, []byte{0x00, 0x00, 0x00, 0x00, 0x05, 0x42}},
		{"protobuf unsupported", &fakeSRPort{byID: map[int]cluster.RawSchema{5: {Schema: "x", SchemaType: "PROTOBUF"}}}, []byte{0x00, 0x00, 0x00, 0x00, 0x05, 0x42}},
		{"unparseable avro schema", &fakeSRPort{byID: map[int]cluster.RawSchema{5: {Schema: "{{not avro", SchemaType: "AVRO"}}}, []byte{0x00, 0x00, 0x00, 0x00, 0x05, 0x42}},
		{"truncated avro body", &fakeSRPort{byID: map[int]cluster.RawSchema{5: {Schema: avroUserSchema, SchemaType: "AVRO"}}}, []byte{0x00, 0x00, 0x00, 0x00, 0x05, 0x28}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := infraserde.NewSchemaRegistrySerde(tc.port, srDef())
			_, err := s.Deserialize("t", domainserde.TargetValue, tc.data)
			require.Error(t, err) // decodeField (message engine) falls back to raw bytes on any error
		})
	}
}

func TestSchemaRegistrySerdeIdentity(t *testing.T) {
	s := infraserde.NewSchemaRegistrySerde(&fakeSRPort{}, srDef())
	require.Equal(t, "SchemaRegistry", s.Name())
	require.NotEmpty(t, s.Description())
	require.True(t, s.CanDeserialize("t", domainserde.TargetValue))
	require.True(t, s.CanSerialize("t", domainserde.TargetValue))
}

func TestSchemaRegistrySerdeSchemaReportsLatest(t *testing.T) {
	port := &fakeSRPort{latest: cluster.SchemaVersion{ID: 5, Schema: avroUserSchema, SchemaType: "AVRO"}}
	s := infraserde.NewSchemaRegistrySerde(port, srDef())
	schema, ok := s.Schema("t", domainserde.TargetValue)
	require.True(t, ok)
	require.Equal(t, avroUserSchema, schema)
	require.Equal(t, "t-value", port.lastVerSubject)
}

func TestSchemaRegistrySerdeSchemaAbsentSubject(t *testing.T) {
	port := &fakeSRPort{latestErr: fmt.Errorf("subject not found")}
	s := infraserde.NewSchemaRegistrySerde(port, srDef())
	_, ok := s.Schema("t", domainserde.TargetValue)
	require.False(t, ok) // missing subject / fetch error → not a schema, never errors out of Suggest
}

// --- Provider dynamic candidate (Task 6) ---

func serdeNames(descs []domainserde.Description) []string {
	out := make([]string, len(descs))
	for i, d := range descs {
		out[i] = d.Name
	}
	return out
}

func srPreferred(t *testing.T, descs []domainserde.Description) string {
	t.Helper()
	var found string
	count := 0
	for _, d := range descs {
		if d.Preferred {
			found = d.Name
			count++
		}
	}
	require.Equal(t, 1, count, "exactly one candidate must be preferred")
	return found
}

func TestProviderSuggestAddsSchemaRegistryWhenConfigured(t *testing.T) {
	port := &fakeSRPort{latestErr: fmt.Errorf("no subject")} // Schema() false → still a candidate, just not preferred
	prov := infraserde.NewProviderWithSchemaRegistry(infraserde.NewRegistry(), port)

	got := prov.Suggest(srDef(), "orders", domainserde.UsageDeserialize)
	require.Len(t, got.Value, 9) // 8 built-ins + SchemaRegistry
	require.Contains(t, serdeNames(got.Value), "SchemaRegistry")
	require.Contains(t, serdeNames(got.Key), "SchemaRegistry")
}

func TestProviderSuggestNoSchemaRegistryZeroRegression(t *testing.T) {
	port := &fakeSRPort{} // SR port present, but the cluster has no SchemaRegistry URL
	prov := infraserde.NewProviderWithSchemaRegistry(infraserde.NewRegistry(), port)

	got := prov.Suggest(cluster.Definition{Name: "nosr"}, "orders", domainserde.UsageDeserialize)
	require.Len(t, got.Value, 8)
	require.NotContains(t, serdeNames(got.Value), "SchemaRegistry")
}

func TestProviderSuggestPrefersSchemaRegistryWhenSubjectExists(t *testing.T) {
	port := &fakeSRPort{
		latest:       cluster.SchemaVersion{ID: 5, Schema: avroUserSchema, SchemaType: "AVRO"},
		versionCalls: map[string]int{},
	} // Schema() true
	prov := infraserde.NewProviderWithSchemaRegistry(infraserde.NewRegistry(), port)

	got := prov.Suggest(srDef(), "orders", domainserde.UsageDeserialize)
	require.Equal(t, "SchemaRegistry", srPreferred(t, got.Value))
	require.Equal(t, 1, port.versionCalls["orders-key"])
	require.Equal(t, 1, port.versionCalls["orders-value"])
}

func TestProviderSuggestSchemaRegistryNotPreferredWithoutSubject(t *testing.T) {
	port := &fakeSRPort{latestErr: fmt.Errorf("subject not found")} // Schema() false
	prov := infraserde.NewProviderWithSchemaRegistry(infraserde.NewRegistry(), port)

	got := prov.Suggest(srDef(), "orders", domainserde.UsageDeserialize)
	require.Equal(t, "String", srPreferred(t, got.Value)) // falls back to the built-in default
}

func TestProviderLookupSchemaRegistry(t *testing.T) {
	prov := infraserde.NewProviderWithSchemaRegistry(infraserde.NewRegistry(), &fakeSRPort{})

	sd, ok := prov.Lookup(srDef(), "SchemaRegistry")
	require.True(t, ok)
	require.Equal(t, "SchemaRegistry", sd.Name())

	_, ok = prov.Lookup(cluster.Definition{Name: "nosr"}, "SchemaRegistry")
	require.False(t, ok) // unconfigured cluster has no SR serde
}
