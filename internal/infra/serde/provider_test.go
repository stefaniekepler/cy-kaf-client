package serde

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/cy-kaf/cy-kaf-client/internal/domain/cluster"
	domainserde "github.com/cy-kaf/cy-kaf-client/internal/domain/serde"
)

// preferredName asserts descs has exactly one Preferred==true entry (every
// other candidate must be false -- Suggest never leaves more than one
// preferred, nor zero) and returns that entry's Name.
func preferredName(t *testing.T, descs []domainserde.Description) string {
	t.Helper()
	var found string
	count := 0
	for _, d := range descs {
		if d.Preferred {
			found = d.Name
			count++
		}
	}
	require.Equal(t, 1, count, "expected exactly one preferred candidate, got descs=%+v", descs)
	return found
}

// TestProviderSuggest covers the brief's three provider.Suggest cases: no
// config falls back to "String" preferred (case 1), a matching SerdeConfigs
// pattern wins for value (case 2), and DefaultKeySerde wins for key when no
// pattern matches (case 3).
func TestProviderSuggest(t *testing.T) {
	prov := NewProvider(NewRegistry())

	t.Run("no serde config: value defaults to String preferred, all 8 built-ins candidates", func(t *testing.T) {
		got := prov.Suggest(cluster.Definition{}, "any-topic", domainserde.UsageDeserialize)
		require.Len(t, got.Value, 8)
		require.Equal(t, "String", preferredName(t, got.Value))
		require.Len(t, got.Key, 8)
		require.Equal(t, "String", preferredName(t, got.Key))
	})

	t.Run("SerdeConfigs TopicValuesPattern match wins for value", func(t *testing.T) {
		def := cluster.Definition{
			SerdeConfigs: []cluster.SerdeConfig{{Name: "Int64", TopicValuesPattern: "orders-.*"}},
		}
		got := prov.Suggest(def, "orders-1", domainserde.UsageDeserialize)
		require.Equal(t, "Int64", preferredName(t, got.Value))
	})

	t.Run("DefaultKeySerde wins for key when no SerdeConfigs pattern matches", func(t *testing.T) {
		def := cluster.Definition{DefaultKeySerde: "Hex"}
		got := prov.Suggest(def, "any-topic", domainserde.UsageSerialize)
		require.Equal(t, "Hex", preferredName(t, got.Key))
	})

	// A SerdeConfigs entry may bind a *custom*, non-built-in serde name
	// (Task 1's config supports className/filePath serde bindings) that this
	// built-in-only registry can't honor yet. That name must NOT be marked
	// preferred (it isn't a candidate), but the "exactly one preferred"
	// invariant must still hold -- it degrades to a real candidate (String
	// here, since no valid default is configured), never zero preferred.
	t.Run("SerdeConfigs match on a non-built-in name degrades to String, never zero preferred", func(t *testing.T) {
		def := cluster.Definition{
			SerdeConfigs: []cluster.SerdeConfig{{Name: "MySerde", TopicValuesPattern: ".*-value"}},
		}
		got := prov.Suggest(def, "foo-value", domainserde.UsageDeserialize)
		require.Equal(t, "String", preferredName(t, got.Value)) // preferredName asserts exactly one preferred
	})

	// Same degradation for a Default*Serde naming a non-built-in serde (or a
	// typo): skipped, degrading to String, never leaving zero preferred.
	t.Run("DefaultKeySerde naming a non-built-in serde degrades to String, never zero preferred", func(t *testing.T) {
		def := cluster.Definition{DefaultKeySerde: "Typo"}
		got := prov.Suggest(def, "any-topic", domainserde.UsageSerialize)
		require.Equal(t, "String", preferredName(t, got.Key))
	})

	// Ordering: when a SerdeConfigs regex matches AND a Default*Serde is also
	// configured on the same target (both valid built-ins), the regex match
	// wins -- pins "config pattern beats cluster default" (review Minor #1).
	t.Run("SerdeConfigs pattern match beats DefaultValueSerde on the same target", func(t *testing.T) {
		def := cluster.Definition{
			SerdeConfigs:      []cluster.SerdeConfig{{Name: "Int64", TopicValuesPattern: ".*-value"}},
			DefaultValueSerde: "Hex",
		}
		got := prov.Suggest(def, "foo-value", domainserde.UsageDeserialize)
		require.Equal(t, "Int64", preferredName(t, got.Value))
	})
}

// TestProviderSuggestPopulatesSchemaWhenSerdeHasOne exercises Suggest's
// Schema-pointer mapping branch, which none of the 8 real built-in codecs
// ever take (their Schema always reports ("", false) -- see serde.Serde's
// doc comment) -- a minimal fake registered directly into a Registry literal
// (same package, so the unexported byName/order fields are reachable) drives
// it instead.
func TestProviderSuggestPopulatesSchemaWhenSerdeHasOne(t *testing.T) {
	reg := &Registry{
		byName: map[string]domainserde.Serde{"Fake": fakeSchemaSerde{}},
		order:  []domainserde.Serde{fakeSchemaSerde{}},
	}
	prov := NewProvider(reg)
	got := prov.Suggest(cluster.Definition{}, "any-topic", domainserde.UsageDeserialize)
	require.Len(t, got.Value, 1)
	require.NotNil(t, got.Value[0].Schema)
	require.Equal(t, "fake-schema", *got.Value[0].Schema)
}

// TestProviderLookup covers Provider.Lookup delegating straight to the
// underlying Registry.Get, both for a registered and an unregistered name.
func TestProviderLookup(t *testing.T) {
	prov := NewProvider(NewRegistry())

	s, ok := prov.Lookup(cluster.Definition{}, "String")
	require.True(t, ok)
	require.Equal(t, "String", s.Name())

	_, ok = prov.Lookup(cluster.Definition{}, "nope")
	require.False(t, ok)
}

// fakeSchemaSerde is a minimal domain serde.Serde stub used only to exercise
// Suggest's Schema-mapping branch.
type fakeSchemaSerde struct{}

func (fakeSchemaSerde) Name() string        { return "Fake" }
func (fakeSchemaSerde) Description() string { return "fake schema-bearing serde" }

func (fakeSchemaSerde) CanDeserialize(_ string, _ domainserde.Target) bool { return true }
func (fakeSchemaSerde) CanSerialize(_ string, _ domainserde.Target) bool   { return true }

func (fakeSchemaSerde) Schema(_ string, _ domainserde.Target) (string, bool) {
	return "fake-schema", true
}

func (fakeSchemaSerde) Serialize(_ string, _ domainserde.Target, input string) ([]byte, error) {
	return []byte(input), nil
}

func (fakeSchemaSerde) Deserialize(_ string, _ domainserde.Target, data []byte) (string, error) {
	return string(data), nil
}
