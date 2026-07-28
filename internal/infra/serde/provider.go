package serde

import (
	"regexp"

	"github.com/cy-kaf/cy-kaf-client/internal/domain/cluster"
	"github.com/cy-kaf/cy-kaf-client/internal/domain/serde"
)

// Provider is the built-in-registry-backed serde.Provider: every suggestion
// candidate comes from Registry.All (the 8 built-in codecs, in their fixed
// display order), plus — when the Provider was given a SchemaRegistryPort and
// the cluster declares a Schema Registry URL — a dynamically-constructed
// "SchemaRegistry" candidate (Task 6). Which one is "preferred" is computed
// from cluster.Definition's own config plus, for SR-configured clusters,
// whether the topic actually carries a registered schema — see
// preferredSerdeName's doc comment.
type Provider struct {
	reg *Registry
	// sr is the Schema Registry port used to build per-cluster SchemaRegistry
	// serde candidates; nil in the built-in-only configuration (every existing
	// caller/test), in which case no "SchemaRegistry" candidate is ever offered
	// and behaviour is identical to before Task 6.
	sr cluster.SchemaRegistryPort
}

// NewProvider builds a built-in-only Provider backed by reg (typically
// NewRegistry()'s fixed 8-codec set) — no SchemaRegistry candidate is offered.
func NewProvider(reg *Registry) *Provider {
	return &Provider{reg: reg}
}

// NewProviderWithSchemaRegistry builds a Provider that additionally offers a
// per-cluster "SchemaRegistry" serde candidate (backed by sr) for any cluster
// whose Definition declares a Schema Registry URL. Clusters without one behave
// exactly as under NewProvider (zero regression).
func NewProviderWithSchemaRegistry(reg *Registry, sr cluster.SchemaRegistryPort) *Provider {
	return &Provider{reg: reg, sr: sr}
}

var _ serde.Provider = (*Provider)(nil)

// Suggest reports every registered serde as a candidate for topic's key and
// its value separately (same 8 candidates both times -- built-in serdes are
// bidirectional, so `use` doesn't filter the candidate set, only
// preferredSerdeName's per-target config lookup differs between key and
// value).
func (p *Provider) Suggest(def cluster.Definition, topic string, use serde.Usage) serde.Suggestion {
	return serde.Suggestion{
		Key:   p.candidates(def, topic, serde.TargetKey),
		Value: p.candidates(def, topic, serde.TargetValue),
	}
}

// candidates builds one target's (key or value's) full candidate list: every
// registered built-in serde in Registry.All order, plus the per-cluster
// "SchemaRegistry" serde when this cluster has one. The subject schema is
// fetched once and reused both for preference selection and its Description;
// every candidate is marked against the single preferred name.
func (p *Provider) candidates(def cluster.Definition, topic string, target serde.Target) []serde.Description {
	all := p.reg.All()
	srSerde, hasSR := p.schemaRegistrySerde(def)
	var (
		srSchema         string
		hasSubjectSchema bool
	)
	if hasSR {
		srSchema, hasSubjectSchema = srSerde.Schema(topic, target)
	}
	preferred := p.preferredSerdeName(def, topic, target, hasSR, hasSubjectSchema)
	out := make([]serde.Description, 0, len(all)+1)
	for _, s := range all {
		out = append(out, describeCandidate(s, topic, target, s.Name() == preferred))
	}
	if hasSR {
		desc := serde.Description{
			Name:        srSerde.Name(),
			Description: srSerde.Description(),
			Preferred:   srSerde.Name() == preferred,
		}
		if hasSubjectSchema {
			desc.Schema = &srSchema
		}
		out = append(out, desc)
	}
	return out
}

// schemaRegistrySerde returns this cluster's SchemaRegistry serde and true when
// the Provider was given an SR port and the cluster declares a Schema Registry
// URL; otherwise (nil, false) — the built-in-only path, where no SR candidate
// is offered.
func (p *Provider) schemaRegistrySerde(def cluster.Definition) (serde.Serde, bool) {
	if p.sr == nil || def.SchemaRegistry.URL == "" {
		return nil, false
	}
	return NewSchemaRegistrySerde(p.sr, def), true
}

// describeCandidate maps one registered serde onto its Description for
// topic/target: Name/Description come straight from the serde, Preferred is
// the caller's own precomputed verdict, Schema is only set (as a non-nil
// pointer) when the serde actually has one for this topic/target -- built-in
// codecs never do (Serde.Schema's doc comment: always ("", false)), so this
// is always nil for the 8 built-ins today. Params stays nil: built-in serdes
// take no configurable parameters.
func describeCandidate(s serde.Serde, topic string, target serde.Target, preferred bool) serde.Description {
	d := serde.Description{Name: s.Name(), Description: s.Description(), Preferred: preferred}
	if schema, ok := s.Schema(topic, target); ok {
		d.Schema = &schema
	}
	return d
}

// preferredSerdeName computes which single candidate (by Name) is preferred
// for topic's target, four tiers in priority order:
//  1. the first configured SerdeConfigs entry whose TopicKeysPattern (target
//     == TargetKey) or TopicValuesPattern (target == TargetValue) regex
//     matches topic AND whose Name is an actually-offered candidate;
//  2. else, for an SR-configured cluster, "SchemaRegistry" when topic's
//     <topic>-key/-value subject actually carries a registered schema (Task 6:
//     auto-prefer the SR serde for topics that have schemas, so the vendored
//     frontend defaults the Browse view to it — matching upstream kafka-ui);
//  3. else def.DefaultKeySerde (key) / def.DefaultValueSerde (value), when it
//     too names an offered candidate;
//  4. else the fixed fallback "String".
//
// Every tier is gated on candidacy (isCandidate: a registered built-in, or
// "SchemaRegistry" when this cluster has one) because the returned name MUST
// be one of the candidates candidates() marks Preferred against -- a name no
// offered serde carries would leave the whole list Preferred=false, breaking
// the "exactly one preferred" invariant the vendored frontend's
// getDefaultValues relies on (it seeds its default serde form from the sole
// preferred entry; zero preferred crashes it). Task 1's config can bind a
// *custom*, non-built-in serde by name this registry can't honor yet; such a
// name is skipped, degrading to the next honorable tier rather than zeroing
// out the preferred flag. (An empty Default*Serde is likewise skipped.)
//
// A SerdeConfigs entry whose pattern fails to compile as a regexp is treated
// as a non-match (skipped) rather than panicking or erroring the whole
// suggestion call.
func (p *Provider) preferredSerdeName(def cluster.Definition, topic string, target serde.Target, hasSR, hasSubjectSchema bool) string {
	for _, sc := range def.SerdeConfigs {
		pattern := sc.TopicValuesPattern
		if target == serde.TargetKey {
			pattern = sc.TopicKeysPattern
		}
		if pattern == "" {
			continue
		}
		re, err := regexp.Compile(pattern)
		if err != nil {
			continue
		}
		if re.MatchString(topic) {
			if p.isCandidate(def, sc.Name, hasSR) {
				return sc.Name
			}
		}
	}
	if hasSR && hasSubjectSchema {
		return SchemaRegistryName
	}
	dflt := def.DefaultValueSerde
	if target == serde.TargetKey {
		dflt = def.DefaultKeySerde
	}
	if p.isCandidate(def, dflt, hasSR) {
		return dflt
	}
	return "String"
}

// isCandidate reports whether name is an offered candidate for def: a
// registered built-in, or "SchemaRegistry" when this cluster has an SR serde.
func (p *Provider) isCandidate(def cluster.Definition, name string, hasSR bool) bool {
	if _, ok := p.reg.Get(name); ok {
		return true
	}
	return hasSR && name == SchemaRegistryName
}

// Lookup finds an offered serde by name: a registered built-in, or the
// per-cluster "SchemaRegistry" serde when def has one. ok is false when name
// isn't offered for def (e.g. "SchemaRegistry" on a cluster with no Schema
// Registry configured, or an unknown built-in name).
func (p *Provider) Lookup(def cluster.Definition, name string) (serde.Serde, bool) {
	if name == SchemaRegistryName {
		return p.schemaRegistrySerde(def)
	}
	return p.reg.Get(name)
}
