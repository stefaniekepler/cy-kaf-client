package masking_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/cy-kaf/cy-kaf-client/internal/domain/cluster"
	"github.com/cy-kaf/cy-kaf-client/internal/domain/masking"
)

// TestApplyThreeStrategiesOverJSONObject pins the brief's cases 1-3: each of
// REMOVE/MASK/REPLACE acting on one matched field of a JSON object value,
// scoped in by a TopicValuesPattern that matches every topic.
func TestApplyThreeStrategiesOverJSONObject(t *testing.T) {
	cases := []struct {
		name  string
		rules []cluster.MaskingRule
		value string
		want  string
	}{
		{
			name: "case 1: REMOVE deletes the matched key entirely",
			rules: []cluster.MaskingRule{
				{Type: cluster.MaskRemove, Fields: []string{"secret"}, TopicValuesPattern: ".*"},
			},
			value: `{"a":1,"secret":"x"}`,
			want:  `{"a":1}`,
		},
		{
			name: "case 2: MASK preserves length, replaces every char by class",
			rules: []cluster.MaskingRule{
				{Type: cluster.MaskMask, FieldsNamePattern: "pass.*", MaskingCharsReplacement: []string{"X"}, TopicValuesPattern: ".*"},
			},
			value: `{"password":"Ab3"}`,
			want:  `{"password":"XXX"}`,
		},
		{
			name: "case 3: REPLACE substitutes the whole field value",
			rules: []cluster.MaskingRule{
				{Type: cluster.MaskReplace, Fields: []string{"ssn"}, Replacement: "***", TopicValuesPattern: ".*"},
			},
			value: `{"ssn":"123-45"}`,
			want:  `{"ssn":"***"}`,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			m, err := masking.New(tc.rules)
			require.NoError(t, err)

			got, err := m.Apply("any-topic", masking.TargetValue, tc.value)
			require.NoError(t, err)
			require.JSONEq(t, tc.want, got)
		})
	}
}

// TestApplySelectorScopesByTargetAndTopic pins the brief's case 4: a rule
// bound only via TopicValuesPattern applies to the matching topic's value,
// not to a non-matching topic's value, and not to the matching topic's key
// (whose TopicKeysPattern is left empty -- "not scoped to this target" per
// New's doc comment).
func TestApplySelectorScopesByTargetAndTopic(t *testing.T) {
	rules := []cluster.MaskingRule{
		{Type: cluster.MaskReplace, Replacement: "MASKED", TopicValuesPattern: "pii-.*"},
	}
	m, err := masking.New(rules)
	require.NoError(t, err)

	got, err := m.Apply("pii-users", masking.TargetValue, "hello")
	require.NoError(t, err)
	require.Equal(t, "MASKED", got, "matching topic's value must be masked")

	got, err = m.Apply("orders", masking.TargetValue, "hello")
	require.NoError(t, err)
	require.Equal(t, "hello", got, "non-matching topic's value must pass through unchanged")

	got, err = m.Apply("pii-users", masking.TargetKey, "hello")
	require.NoError(t, err)
	require.Equal(t, "hello", got, "empty TopicKeysPattern means the rule never applies to keys")
}

// TestApplyScalarValueReducedToEmptyOnRemove pins the brief's case 5: a
// value that isn't a JSON object (bare text, not quoted -- invalid JSON) is
// treated as one opaque scalar unit, so REMOVE empties it wholesale rather
// than trying (and failing) to find a JSON field inside it.
func TestApplyScalarValueReducedToEmptyOnRemove(t *testing.T) {
	rules := []cluster.MaskingRule{
		{Type: cluster.MaskRemove, TopicValuesPattern: ".*"},
	}
	m, err := masking.New(rules)
	require.NoError(t, err)

	got, err := m.Apply("any-topic", masking.TargetValue, "plaintext")
	require.NoError(t, err)
	require.Equal(t, "", got)
}

// TestApplyWithNoRulesPassesThrough pins the brief's case 6: a Masker built
// from a nil rule set (the "cluster has no masking configured" case) leaves
// every value untouched.
func TestApplyWithNoRulesPassesThrough(t *testing.T) {
	m, err := masking.New(nil)
	require.NoError(t, err)

	got, err := m.Apply("any-topic", masking.TargetValue, `{"a":1}`)
	require.NoError(t, err)
	require.Equal(t, `{"a":1}`, got)
}

// TestNewRejectsInvalidRegex pins the brief's case 7: a malformed regex in
// any of the three pattern fields fails New with an error (never a panic),
// and never returns a partially-built Masker.
func TestNewRejectsInvalidRegex(t *testing.T) {
	cases := []struct {
		name string
		rule cluster.MaskingRule
	}{
		{name: "invalid FieldsNamePattern", rule: cluster.MaskingRule{FieldsNamePattern: "["}},
		{name: "invalid TopicKeysPattern", rule: cluster.MaskingRule{TopicKeysPattern: "["}},
		{name: "invalid TopicValuesPattern", rule: cluster.MaskingRule{TopicValuesPattern: "["}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			m, err := masking.New([]cluster.MaskingRule{tc.rule})
			require.Error(t, err)
			require.Nil(t, m)
		})
	}
}

// TestMaskDefaultCharsReplacement covers MASK's default character-class
// table (X/x/n/-) when a rule leaves MaskingCharsReplacement empty --
// exercising all 4 classes (upper, lower, digit, other) in one string.
func TestMaskDefaultCharsReplacement(t *testing.T) {
	rules := []cluster.MaskingRule{
		{Type: cluster.MaskMask, TopicValuesPattern: ".*"},
	}
	m, err := masking.New(rules)
	require.NoError(t, err)

	got, err := m.Apply("any-topic", masking.TargetValue, "Ab3-")
	require.NoError(t, err)
	require.Equal(t, "Xxn-", got)
}

// TestMaskCharsReplacementShorterThanClassFallsBackToLastElement covers the
// brief's fallback rule: when MaskingCharsReplacement has fewer entries than
// the class index needs, the *last* configured element is reused -- proven
// here with a 2-element list where both the digit (class 2) and other
// (class 3) classes fall back to element[1].
func TestMaskCharsReplacementShorterThanClassFallsBackToLastElement(t *testing.T) {
	rules := []cluster.MaskingRule{
		{Type: cluster.MaskMask, MaskingCharsReplacement: []string{"U", "L"}, TopicValuesPattern: ".*"},
	}
	m, err := masking.New(rules)
	require.NoError(t, err)

	got, err := m.Apply("any-topic", masking.TargetValue, "A1!")
	require.NoError(t, err)
	require.Equal(t, "ULL", got)
}

// TestApplyRecursesIntoNestedObjectsForUnmatchedFields extends case 1 to a
// nested JSON object: a field that doesn't match at the top level is
// descended into (per "递归嵌套 object" in the brief), so a match two levels
// down still gets masked while sibling fields at every level survive.
func TestApplyRecursesIntoNestedObjectsForUnmatchedFields(t *testing.T) {
	rules := []cluster.MaskingRule{
		{Type: cluster.MaskRemove, Fields: []string{"ssn"}, TopicValuesPattern: ".*"},
	}
	m, err := masking.New(rules)
	require.NoError(t, err)

	got, err := m.Apply("any-topic", masking.TargetValue, `{"user":{"ssn":"123","name":"bob"},"other":1}`)
	require.NoError(t, err)
	require.JSONEq(t, `{"user":{"name":"bob"},"other":1}`, got)
}

// TestApplyMatchedFieldStrategyAppliesToWholeSubtreeNotRecursed proves the
// converse of the recursion test above: once a field name matches at a
// given level, its whole value is handed to the strategy as one unit --
// masking does not also separately recurse into a matched field's own
// nested object looking for more matches underneath it.
func TestApplyMatchedFieldStrategyAppliesToWholeSubtreeNotRecursed(t *testing.T) {
	rules := []cluster.MaskingRule{
		{Type: cluster.MaskRemove, Fields: []string{"user"}, TopicValuesPattern: ".*"},
	}
	m, err := masking.New(rules)
	require.NoError(t, err)

	got, err := m.Apply("any-topic", masking.TargetValue, `{"user":{"ssn":"123"},"other":1}`)
	require.NoError(t, err)
	require.JSONEq(t, `{"other":1}`, got)
}

// TestApplyMaskOnNonStringFieldMasksItsJSONTextForm covers maskValue's
// non-string branch: a matched field whose decoded value isn't a Go string
// (here, a JSON number) is rendered to its canonical JSON text and that text
// is masked, rather than being left untouched -- documented as an assumption
// in this task's report (the brief's own test vectors only use string
// fields).
func TestApplyMaskOnNonStringFieldMasksItsJSONTextForm(t *testing.T) {
	rules := []cluster.MaskingRule{
		{Type: cluster.MaskMask, Fields: []string{"age"}, TopicValuesPattern: ".*"},
	}
	m, err := masking.New(rules)
	require.NoError(t, err)

	got, err := m.Apply("any-topic", masking.TargetValue, `{"age":42}`)
	require.NoError(t, err)
	require.JSONEq(t, `{"age":"nn"}`, got)
}

// TestApplyScalarJSONArrayTreatedAsOpaqueUnit covers the "JSON array" leg of
// the brief's "value 非 JSON object（标量文本/数组）" case: a top-level JSON
// array is never descended into for field matching, it's masked as one
// whole unit like any other non-object value.
func TestApplyScalarJSONArrayTreatedAsOpaqueUnit(t *testing.T) {
	rules := []cluster.MaskingRule{
		{Type: cluster.MaskRemove, TopicValuesPattern: ".*"},
	}
	m, err := masking.New(rules)
	require.NoError(t, err)

	got, err := m.Apply("any-topic", masking.TargetValue, `[1,2,3]`)
	require.NoError(t, err)
	require.Equal(t, "", got)
}

// TestApplyTopLevelJSONNullTreatedAsScalar covers decodeObject's nil guard:
// a bare JSON "null" unmarshals into a nil map with no error, which must not
// be mistaken for an (empty) object -- it falls through to the scalar path
// like any other non-object value.
func TestApplyTopLevelJSONNullTreatedAsScalar(t *testing.T) {
	rules := []cluster.MaskingRule{
		{Type: cluster.MaskReplace, Replacement: "GONE", TopicValuesPattern: ".*"},
	}
	m, err := masking.New(rules)
	require.NoError(t, err)

	got, err := m.Apply("any-topic", masking.TargetValue, `null`)
	require.NoError(t, err)
	require.Equal(t, "GONE", got)
}

// TestApplyKeyTargetMaskedWhenTopicKeysPatternMatches covers applicableRules'
// TargetKey branch (the mirror of TargetValue, exercised by every other test
// in this file): a rule scoped via TopicKeysPattern applies to a matching
// topic's key.
func TestApplyKeyTargetMaskedWhenTopicKeysPatternMatches(t *testing.T) {
	rules := []cluster.MaskingRule{
		{Type: cluster.MaskRemove, Fields: []string{"id"}, TopicKeysPattern: "^k-.*"},
	}
	m, err := masking.New(rules)
	require.NoError(t, err)

	got, err := m.Apply("k-topic", masking.TargetKey, `{"id":5,"other":1}`)
	require.NoError(t, err)
	require.JSONEq(t, `{"other":1}`, got)

	got, err = m.Apply("other-topic", masking.TargetKey, `{"id":5,"other":1}`)
	require.NoError(t, err)
	require.JSONEq(t, `{"id":5,"other":1}`, got, "non-matching topic's key must pass through unchanged")
}

// TestApplyReplaceDefaultsToTripleAsterisk covers REPLACE's documented
// default substitution text when a rule leaves Replacement empty.
func TestApplyReplaceDefaultsToTripleAsterisk(t *testing.T) {
	rules := []cluster.MaskingRule{
		{Type: cluster.MaskReplace, Fields: []string{"ssn"}, TopicValuesPattern: ".*"},
	}
	m, err := masking.New(rules)
	require.NoError(t, err)

	got, err := m.Apply("any-topic", masking.TargetValue, `{"ssn":"123-45"}`)
	require.NoError(t, err)
	require.JSONEq(t, `{"ssn":"***"}`, got)
}

// TestApplyChainsMultipleApplicableRulesInOrder covers Apply's multi-rule
// loop: two rules both scoped to the same (topic, target) are applied in
// configuration order, each seeing the previous rule's output.
func TestApplyChainsMultipleApplicableRulesInOrder(t *testing.T) {
	rules := []cluster.MaskingRule{
		{Type: cluster.MaskRemove, Fields: []string{"secret"}, TopicValuesPattern: ".*"},
		{Type: cluster.MaskReplace, Fields: []string{"ssn"}, Replacement: "***", TopicValuesPattern: ".*"},
	}
	m, err := masking.New(rules)
	require.NoError(t, err)

	got, err := m.Apply("any-topic", masking.TargetValue, `{"secret":"x","ssn":"123-45","a":1}`)
	require.NoError(t, err)
	require.JSONEq(t, `{"ssn":"***","a":1}`, got)
}

// TestApplyPreservesLargeIntegersOnUnmatchedFields pins Q-C1: a JSON object
// field no applicable rule's selector matches must survive Apply's
// decode/mask/re-marshal round trip with its numeric value exact.
// decodeObject decoding into map[string]any without json.Decoder's
// UseNumber would silently route every number through float64, which cannot
// represent every int64 exactly -- 9007199254740993 (2^53+1) is the classic
// example: float64 rounds it down to 9007199254740992 and json.Marshal may
// additionally render it in scientific notation. Neither must happen to a
// field the masking config never asked to touch. require.JSONEq is
// deliberately NOT used for the numeric assertion below: JSONEq itself
// decodes both sides as float64 before comparing, so it would silently pass
// even if the implementation lost precision -- it can't catch this bug.
func TestApplyPreservesLargeIntegersOnUnmatchedFields(t *testing.T) {
	rules := []cluster.MaskingRule{
		{Type: cluster.MaskRemove, Fields: []string{"secret"}, TopicValuesPattern: ".*"},
	}
	m, err := masking.New(rules)
	require.NoError(t, err)

	got, err := m.Apply("any-topic", masking.TargetValue, `{"id":9007199254740993,"keep":"v","secret":"x"}`)
	require.NoError(t, err)
	require.Contains(t, got, "9007199254740993", "unmatched large-integer field must keep its exact decimal text")
	require.NotContains(t, got, "9.0", "must not have been rewritten in scientific notation")
	require.NotContains(t, got, "e+", "must not have been rewritten in scientific notation")
	require.NotContains(t, got, `"secret"`, "matched field must still be removed")
}

// TestApplyPreservesSpecialCharactersOnUnmatchedFields pins Q-C1's other
// half: a JSON object field no applicable rule's selector matches must keep
// '<', '>', '&' inside its string value literal. Re-marshaling with plain
// json.Marshal (its default SetEscapeHTML(true)) would silently rewrite
// those into their </&/> escapes, corrupting a field the
// masking config never touched.
func TestApplyPreservesSpecialCharactersOnUnmatchedFields(t *testing.T) {
	rules := []cluster.MaskingRule{
		{Type: cluster.MaskRemove, Fields: []string{"secret"}, TopicValuesPattern: ".*"},
	}
	m, err := masking.New(rules)
	require.NoError(t, err)

	got, err := m.Apply("any-topic", masking.TargetValue, `{"note":"a<b&c>d","secret":"x"}`)
	require.NoError(t, err)
	require.Contains(t, got, "a<b&c>d", "unmatched field's special characters must stay literal")
	require.NotContains(t, got, `\u003c`, "must not have been HTML-escaped")
	require.NotContains(t, got, `"secret"`, "matched field must still be removed")
}

// TestApplySelectorlessRuleMatchesAllFieldsOnObject pins S-C1: a rule with
// neither Fields nor FieldsNamePattern configured is upstream kafka-ui's
// documented "mask the whole message" configuration (FieldsSelector.create's
// "no selector = select all fields" default) -- it must match every field of
// a JSON object target, not silently match none of them (which is what
// fieldMatches's exact-name/regex checks alone would do when both are
// empty, and would be a fail-open data leak for anyone relying on that
// upstream-documented configuration).
func TestApplySelectorlessRuleMatchesAllFieldsOnObject(t *testing.T) {
	rules := []cluster.MaskingRule{
		{Type: cluster.MaskRemove, TopicValuesPattern: ".*"},
	}
	m, err := masking.New(rules)
	require.NoError(t, err)

	got, err := m.Apply("any-topic", masking.TargetValue, `{"a":1,"b":2}`)
	require.NoError(t, err)
	require.JSONEq(t, `{}`, got)
}

// TestApplySelectorlessMaskRuleMasksAllStringFieldsOnObject extends the
// REMOVE case above to MASK, with two fields, to prove every field gets the
// strategy applied (not just an arbitrary first one, and not none).
func TestApplySelectorlessMaskRuleMasksAllStringFieldsOnObject(t *testing.T) {
	rules := []cluster.MaskingRule{
		{Type: cluster.MaskMask, MaskingCharsReplacement: []string{"X"}, TopicValuesPattern: ".*"},
	}
	m, err := masking.New(rules)
	require.NoError(t, err)

	got, err := m.Apply("any-topic", masking.TargetValue, `{"a":"hi","b":"yo"}`)
	require.NoError(t, err)
	require.JSONEq(t, `{"a":"XX","b":"XX"}`, got)
}
