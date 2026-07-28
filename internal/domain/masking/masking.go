// Package masking implements data-masking (REMOVE/MASK/REPLACE), the domain
// logic behind spec §7.8: a message's key/value text, already turned into
// text by a serde.Serde, gets masked here before it ever reaches the UI.
// Zero third-party imports -- enforced by lint (depguard's domain-purity
// rule), same convention as internal/domain/serde and internal/domain/filter.
// This package does import internal/domain/cluster (for MaskingRule/
// MaskingType) -- domain->domain, same layer, allowed by depguard's
// domain-purity rule.
package masking

import (
	"bytes"
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
	"unicode"

	"github.com/cy-kaf/cy-kaf-client/internal/domain/cluster"
)

// Target selects which part of a Kafka record (already deserialized into
// text by a serde.Serde) Apply is masking -- mirrors domain/serde.Target's
// key/value split, but stays its own type since masking rules bind to
// key/value via two independent topic patterns (TopicKeysPattern/
// TopicValuesPattern) rather than sharing one selector.
type Target int

const (
	TargetKey Target = iota
	TargetValue
)

// defaultMaskChars is MASK's fallback character-class replacement table when
// a rule's MaskingCharsReplacement is empty: index 0 = uppercase letter, 1 =
// lowercase letter, 2 = digit, 3 = everything else (mirrors upstream
// kafka-ui's masking package default).
var defaultMaskChars = []string{"X", "x", "n", "-"}

// compiledRule pairs one cluster.MaskingRule with its precompiled regexes
// (nil when the corresponding pattern field was empty in config -- see New's
// doc comment for what an empty pattern means) and an exact-name lookup set
// built once from Fields, so Apply's hot path never re-compiles a regex or
// re-scans a slice per record.
type compiledRule struct {
	rule cluster.MaskingRule

	fieldsNameRe  *regexp.Regexp
	topicKeysRe   *regexp.Regexp
	topicValuesRe *regexp.Regexp

	fields map[string]struct{}
}

// Masker applies a cluster's configured masking rules to already-
// deserialized message text, at the point spec §7.8 calls out: after a
// serde.Serde has turned raw bytes into text, before that text reaches the
// UI. It holds only the rules' precompiled form -- no I/O, no state Apply
// mutates beyond the input it's given -- so one Masker is safe for
// concurrent use across a cluster's message stream.
type Masker struct {
	rules []compiledRule
}

// New precompiles every rule's FieldsNamePattern/TopicKeysPattern/
// TopicValuesPattern regex. An empty pattern field is left uncompiled (nil)
// rather than defaulting to "match everything": per this task's brief, an
// empty TopicKeysPattern/TopicValuesPattern means the rule simply isn't
// scoped to that target at all (applicableRules below treats a nil pattern
// as "never applicable" for that target) -- callers who want a rule to fire
// for every topic's key or value must configure that pattern explicitly
// (e.g. ".*"). This is the brief's documented "空 pattern = 该 target 不适用"
// reading of upstream's Mask semantics, flagged there for final-review
// cross-check against upstream's actual source; not re-litigated here.
//
// A malformed regex in any of the three fields fails the whole call with the
// offending pattern named in the error -- never a panic, never a partially-
// built Masker.
//
// Separately, a rule that leaves *both* Fields and FieldsNamePattern empty
// (New still compiles that to a nil fieldsNameRe / empty fields set) is not
// "select nothing": fieldMatches treats that combination as "no field
// selector configured at all" and matches every field, mirroring upstream
// kafka-ui's FieldsSelector.create() default -- the documented way to
// configure "mask the whole message" rather than naming individual fields.
func New(rules []cluster.MaskingRule) (*Masker, error) {
	compiled := make([]compiledRule, 0, len(rules))
	for _, r := range rules {
		cr := compiledRule{rule: r}

		var err error
		if cr.fieldsNameRe, err = compileIfSet(r.FieldsNamePattern); err != nil {
			return nil, fmt.Errorf("masking: invalid fieldsNamePattern %q: %w", r.FieldsNamePattern, err)
		}
		if cr.topicKeysRe, err = compileIfSet(r.TopicKeysPattern); err != nil {
			return nil, fmt.Errorf("masking: invalid topicKeysPattern %q: %w", r.TopicKeysPattern, err)
		}
		if cr.topicValuesRe, err = compileIfSet(r.TopicValuesPattern); err != nil {
			return nil, fmt.Errorf("masking: invalid topicValuesPattern %q: %w", r.TopicValuesPattern, err)
		}

		if len(r.Fields) > 0 {
			cr.fields = make(map[string]struct{}, len(r.Fields))
			for _, f := range r.Fields {
				cr.fields[f] = struct{}{}
			}
		}

		compiled = append(compiled, cr)
	}
	return &Masker{rules: compiled}, nil
}

// compileIfSet compiles pattern, or reports (nil, nil) when pattern is empty
// -- the "not scoped to this dimension" case New's doc comment describes.
func compileIfSet(pattern string) (*regexp.Regexp, error) {
	if pattern == "" {
		return nil, nil
	}
	return regexp.Compile(pattern)
}

// Apply masks topic's already-deserialized t-side text per every rule
// applicable to (topic, t) -- see applicableRules for the selector -- in
// configuration order, each rule's output feeding the next. With no
// applicable rule, value is returned unchanged.
//
// Per applicable rule: value is first tried as a JSON object (decodeObject:
// json.Decoder into map[string]any). If that succeeds, the rule's
// strategy is applied to every field matching Fields (exact name) or
// FieldsNamePattern (regex), recursing into nested objects for fields that
// don't match at the current level, and the result is re-marshaled to JSON
// text. Anything that isn't a JSON object at the top level -- a bare
// scalar/text value, a JSON array, a top-level JSON null, or invalid JSON --
// is instead treated as one opaque unit: the whole string is transformed by
// the rule's strategy.
//
// A field no applicable rule's selector matches is passed through the
// decode/re-marshal round trip unmodified in both value and text form: large
// integers stay exact (decodeObject uses json.Number, never float64, so no
// precision loss / scientific-notation rewrite) and special characters like
// '<', '>', '&' inside string fields stay literal (marshalNoEscape, not
// json.Marshal's default HTML-escaping). Key order within a re-marshaled
// object is not preserved -- Go map iteration plus encoding/json's
// alphabetical key sort means Apply's JSON-object output can reorder keys
// relative to the input even when nothing in that object was masked; this is
// a known cosmetic limitation (fixing it needs a hand-written JSON
// tokenizer/writer, which is out of scope here).
func (m *Masker) Apply(topic string, t Target, value string) (string, error) {
	out := value
	for _, cr := range m.applicableRules(topic, t) {
		obj, ok := decodeObject(out)
		if !ok {
			out = applyScalar(cr, out)
			continue
		}
		maskObject(obj, cr)
		b, err := marshalNoEscape(obj)
		if err != nil {
			return "", fmt.Errorf("masking: re-marshal after masking: %w", err)
		}
		out = b
	}
	return out, nil
}

// marshalNoEscape re-encodes v the same way json.Marshal would, except it
// never HTML-escapes '<', '>', '&' (json.Marshal's default behavior, meant
// for embedding JSON inside HTML <script> tags -- not a fit for Kafka
// message text). Apply's re-marshal step must not silently mangle a field's
// text just because that field happened to sit in the same JSON object as
// one a rule matched: plain json.Marshal(obj) would turn a field's "a<b"
// into "a\u003cb" even though that field was never touched by any rule,
// corrupting text the masking config never asked to change. Encoder.Encode
// appends a trailing newline that Marshal doesn't, so that's trimmed off to
// keep this a drop-in replacement for string(json.Marshal(v)).
func marshalNoEscape(v any) (string, error) {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(v); err != nil {
		return "", err
	}
	return string(bytes.TrimRight(buf.Bytes(), "\n")), nil
}

// applicableRules reports, in m's configuration order, every rule scoped to
// (topic, t): for t==TargetKey that means a non-empty TopicKeysPattern
// matching topic; for t==TargetValue, TopicValuesPattern. A rule with the
// relevant pattern left empty never matches either target (see New).
func (m *Masker) applicableRules(topic string, t Target) []compiledRule {
	applicable := make([]compiledRule, 0, len(m.rules))
	for _, cr := range m.rules {
		re := cr.topicValuesRe
		if t == TargetKey {
			re = cr.topicKeysRe
		}
		if re != nil && re.MatchString(topic) {
			applicable = append(applicable, cr)
		}
	}
	return applicable
}

// decodeObject reports value's JSON-object decoding when it has one: valid
// JSON whose top level is an object. A JSON "null" successfully unmarshals
// into a nil map with no error, which would otherwise look like an object to
// range over -- excluded here so top-level "null" falls through to Apply's
// scalar branch like any other non-object value, per the brief's
// object-vs-scalar split.
//
// Decoding uses a json.Decoder with UseNumber (not json.Unmarshal), so every
// number anywhere in obj -- top level or nested, matched by a rule or not --
// comes back as a json.Number (the original decimal text) instead of a
// float64. That matters for fields no rule touches: float64 can't represent
// every int64 exactly, and re-marshaling a large integer that round-tripped
// through float64 can silently corrupt it (lost precision, or rewritten into
// scientific notation) -- the same class of bug flagged in
// internal/api/handlers_topic.go's coerceConfigValue for the same reason.
// dec.More() after the one Decode call reproduces json.Unmarshal's "reject
// trailing non-whitespace data" behavior, which a bare dec.Decode alone does
// not enforce (it stops after the first complete JSON value and ignores
// whatever follows).
func decodeObject(value string) (map[string]any, bool) {
	dec := json.NewDecoder(strings.NewReader(value))
	dec.UseNumber()

	var obj map[string]any
	if err := dec.Decode(&obj); err != nil || obj == nil {
		return nil, false
	}
	if dec.More() {
		return nil, false
	}
	return obj, true
}

// maskObject applies cr's strategy, in place, to every field of obj matching
// cr's selector (fieldMatches), recursing into nested objects for fields
// that don't match at this level -- so a matching field anywhere in the
// (possibly nested) object gets masked, but a matching field's own value is
// never descended into (the strategy applies to that whole subtree, not
// selectively within it).
func maskObject(obj map[string]any, cr compiledRule) {
	for k, v := range obj {
		if !fieldMatches(cr, k) {
			if nested, ok := v.(map[string]any); ok {
				maskObject(nested, cr)
			}
			continue
		}
		switch cr.rule.Type {
		case cluster.MaskRemove:
			delete(obj, k)
		case cluster.MaskMask:
			obj[k] = maskValue(cr, v)
		case cluster.MaskReplace:
			obj[k] = replacementString(cr.rule)
		}
	}
}

// fieldMatches reports whether name is selected by cr: an exact hit in
// Fields, a FieldsNamePattern regex match, or -- when cr has neither Fields
// nor FieldsNamePattern configured (len(cr.fields)==0 && cr.fieldsNameRe==
// nil) -- every field name unconditionally. That last case is not an
// oversight: upstream kafka-ui's FieldsSelector.create() treats "no field
// selector configured" as "select all fields", the documented way to
// configure a rule that masks an entire JSON object rather than naming
// individual fields. Without this default, a rule meant to mask a whole
// message would instead silently mask nothing on any JSON-object target
// (fail-open data leak) while still working correctly on scalar targets
// (which never call fieldMatches at all).
func fieldMatches(cr compiledRule, name string) bool {
	if len(cr.fields) == 0 && cr.fieldsNameRe == nil {
		return true
	}
	if _, ok := cr.fields[name]; ok {
		return true
	}
	return cr.fieldsNameRe != nil && cr.fieldsNameRe.MatchString(name)
}

// applyScalar applies cr's strategy to a whole non-object value (top-level
// scalar text, JSON array, top-level JSON null, or invalid-JSON text) --
// REMOVE reduces it to an empty string, MASK char-class-replaces it in
// place, REPLACE substitutes cr's Replacement (or its "***" default)
// wholesale.
func applyScalar(cr compiledRule, value string) string {
	switch cr.rule.Type {
	case cluster.MaskRemove:
		return ""
	case cluster.MaskMask:
		return charClassMask(cr, value)
	case cluster.MaskReplace:
		return replacementString(cr.rule)
	default:
		return value
	}
}

// maskValue applies MASK's char-class replacement to v. Strings (the shape
// every one of the brief's test vectors uses) are masked directly; any
// other decoded JSON leaf/container (number, bool, null, nested array/
// object) is first rendered to its canonical JSON text and that text is
// masked -- so a MASK rule can never leave a matched non-string field
// looking untouched, at the cost of the result always coming back as a JSON
// string rather than preserving the original type. This is an assumption
// this task's report flags for upstream cross-check, since the brief's test
// vectors don't exercise non-string fields.
func maskValue(cr compiledRule, v any) any {
	if s, ok := v.(string); ok {
		return charClassMask(cr, s)
	}
	b, _ := json.Marshal(v) // v was itself decoded from JSON, so this cannot fail
	return charClassMask(cr, string(b))
}

// replacementString is REPLACE's substitution text: rule.Replacement, or its
// documented "***" default when that's empty.
func replacementString(rule cluster.MaskingRule) string {
	if rule.Replacement == "" {
		return "***"
	}
	return rule.Replacement
}

// charClassMask replaces every character of s with cr's class-appropriate
// replacement string, preserving s's rune count (so length/structure survive
// as long as config uses single-character replacement entries, per the
// brief's examples): uppercase letter -> class 0, lowercase letter -> class
// 1, digit -> class 2, everything else -> class 3. classPick resolves each
// class index against MaskingCharsReplacement (falling back to
// defaultMaskChars when that's empty, and to the *last* configured element
// when the list is shorter than the index needs -- e.g. a single-element
// list masks every class with that one element).
func charClassMask(cr compiledRule, s string) string {
	var b strings.Builder
	b.Grow(len(s))
	for _, r := range s {
		b.WriteString(classPick(cr.rule.MaskingCharsReplacement, charClass(r)))
	}
	return b.String()
}

// charClass buckets r into MASK's 4 replacement classes: 0 = uppercase
// letter, 1 = lowercase letter, 2 = digit, 3 = everything else.
func charClass(r rune) int {
	switch {
	case unicode.IsUpper(r):
		return 0
	case unicode.IsLower(r):
		return 1
	case unicode.IsDigit(r):
		return 2
	default:
		return 3
	}
}

// classPick resolves class against replacement (a rule's
// MaskingCharsReplacement): replacement[class] when the list reaches that
// far, replacement's last element when it's shorter, and
// defaultMaskChars[class] when replacement is empty altogether.
func classPick(replacement []string, class int) string {
	if len(replacement) == 0 {
		return defaultMaskChars[class]
	}
	if class < len(replacement) {
		return replacement[class]
	}
	return replacement[len(replacement)-1]
}
