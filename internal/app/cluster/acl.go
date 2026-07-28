package cluster

import (
	"context"
	"encoding/csv"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/cy-kaf/cy-kaf-client/internal/domain/cluster"
)

// ErrBadAclCSV wraps every parse/validation failure of an ACL CSV payload
// (wrong column count, unparseable enum, bad header) so the api layer can
// errors.Is it and return 400 -- distinct from a backend (ListAcls/CreateAcls/
// DeleteAcls) failure during sync, which propagates raw and becomes a 500.
var ErrBadAclCSV = errors.New("invalid acl csv")

// ErrBadAclRequest marks a syntactically decoded ACL write that is unsafe or
// semantically empty. The API maps it to 400; infra repeats the validation as
// defense in depth for non-HTTP callers.
var ErrBadAclRequest = errors.New("invalid acl request")

// AclService performs ACL read/write/expand operations by resolving a cluster
// name to its Definition and delegating to AclAdminPort. Principal-substring /
// fts filtering, stable ordering, the three helper-endpoint expansions, and
// CSV parse/format/sync all live here (pure logic, fully unit-testable with a
// fake port); infra only does the kadm round-trip. CSV format+parse both live
// here (app owns the single source of the CSV shape; api's rowsToCsv is not
// reused, since app must not depend back on api).
type AclService struct {
	res       *Resolver
	port      cluster.AclAdminPort
	syncLocks keyedLocker[string]
}

func NewAclService(res *Resolver, port cluster.AclAdminPort) *AclService {
	return &AclService{res: res, port: port}
}

// ConsumerAclSpec/ProducerAclSpec/StreamAppAclSpec are the app-layer request
// shapes the api layer maps generated.Create*Acl onto (Task 6).
type ConsumerAclSpec struct {
	Principal, Host      string
	Topics               []string
	TopicsPrefix         string
	ConsumerGroups       []string
	ConsumerGroupsPrefix string
}
type ProducerAclSpec struct {
	Principal, Host      string
	Topics               []string
	TopicsPrefix         string
	TransactionalID      string
	TransactionsIDPrefix string
	Idempotent           bool
}
type StreamAppAclSpec struct {
	Principal, Host           string
	InputTopics, OutputTopics []string
	ApplicationID             string
}

// AclSortKey is the stable sort key (mirrors upstream AclBinding::toString
// ordering intent: a deterministic total order over all seven fields).
// Exported so tests can assert sortedness.
func AclSortKey(b cluster.AclBinding) string {
	fields := aclFields(b)
	var key strings.Builder
	for _, field := range fields {
		key.WriteString(strconv.Itoa(len(field)))
		key.WriteByte(':')
		key.WriteString(field)
	}
	return key.String()
}

// List resolves name, describes ACLs (resource dimension pushed down by the
// port), then applies a local ResourceType/ResourceName backstop (infra's
// ListAcls can degrade a type-only or name-only filter into a broad
// AnyResource() query -- Task 2 review Minor#2 -- so this narrows both
// dimensions locally regardless of what the port actually pushed down),
// principal-substring (or fts full-field) filtering, and a stable sort.
func (s *AclService) List(ctx context.Context, name string, filter cluster.AclFilter) ([]cluster.AclBinding, error) {
	def, err := s.res.Lookup(name)
	if err != nil {
		return nil, err
	}
	all, err := s.port.ListAcls(ctx, def, filter)
	if err != nil {
		return nil, err
	}
	out := make([]cluster.AclBinding, 0, len(all))
	for _, b := range all {
		if !matchesAclResource(b, filter) {
			continue
		}
		if !matchesSearch(b, filter) {
			continue
		}
		out = append(out, b)
	}
	sort.SliceStable(out, func(i, j int) bool { return aclBindingLess(out[i], out[j]) })
	return out, nil
}

func matchesAclResource(binding cluster.AclBinding, filter cluster.AclFilter) bool {
	if filter.ResourceType != "" && binding.ResourceType != filter.ResourceType {
		return false
	}
	if filter.PatternType == "MATCH" {
		switch binding.PatternType {
		case "LITERAL":
			if filter.ResourceName == "" {
				return true
			}
			return binding.ResourceName == filter.ResourceName || binding.ResourceName == "*"
		case "PREFIXED":
			if filter.ResourceName == "" {
				return true
			}
			return strings.HasPrefix(filter.ResourceName, binding.ResourceName)
		default:
			return false
		}
	}
	if filter.PatternType != "" && binding.PatternType != filter.PatternType {
		return false
	}
	return filter.ResourceName == "" || binding.ResourceName == filter.ResourceName
}

func matchesSearch(b cluster.AclBinding, f cluster.AclFilter) bool {
	if f.Search == "" {
		return true
	}
	if f.Fts {
		hay := strings.Join([]string{b.Principal, b.Host, b.ResourceName, b.ResourceType, b.PatternType, b.Operation, b.Permission}, " ")
		return strings.Contains(strings.ToLower(hay), strings.ToLower(f.Search))
	}
	return strings.Contains(strings.ToLower(b.Principal), strings.ToLower(f.Search))
}

func aclFields(binding cluster.AclBinding) []string {
	return []string{binding.Principal, binding.ResourceType, binding.PatternType, binding.ResourceName, binding.Operation, binding.Permission, binding.Host}
}

func aclBindingLess(left, right cluster.AclBinding) bool {
	leftFields, rightFields := aclFields(left), aclFields(right)
	for i := range leftFields {
		if leftFields[i] != rightFields[i] {
			return leftFields[i] < rightFields[i]
		}
	}
	return false
}

// CreateAcl resolves name then creates one binding.
func (s *AclService) CreateAcl(ctx context.Context, name string, binding cluster.AclBinding) error {
	if err := validateAclBinding(binding, false); err != nil {
		return err
	}
	def, err := s.res.Lookup(name)
	if err != nil {
		return err
	}
	return s.port.CreateAcls(ctx, def, []cluster.AclBinding{binding})
}

// DeleteAcl resolves name then deletes one binding, returning the match count
// (0 -> caller 404s).
func (s *AclService) DeleteAcl(ctx context.Context, name string, binding cluster.AclBinding) (int, error) {
	if err := validateAclBinding(binding, true); err != nil {
		return 0, err
	}
	def, err := s.res.Lookup(name)
	if err != nil {
		return 0, err
	}
	return s.port.DeleteAcls(ctx, def, binding)
}

func (s *AclService) CreateConsumerAcl(ctx context.Context, name string, spec ConsumerAclSpec) error {
	bindings, err := validateHelperBindings(spec.Principal, spec.Host, expandConsumer(spec))
	if err != nil {
		return err
	}
	def, err := s.res.Lookup(name)
	if err != nil {
		return err
	}
	return s.port.CreateAcls(ctx, def, bindings)
}

func (s *AclService) CreateProducerAcl(ctx context.Context, name string, spec ProducerAclSpec) error {
	bindings, err := validateHelperBindings(spec.Principal, spec.Host, expandProducer(spec))
	if err != nil {
		return err
	}
	def, err := s.res.Lookup(name)
	if err != nil {
		return err
	}
	return s.port.CreateAcls(ctx, def, bindings)
}

func (s *AclService) CreateStreamAppAcl(ctx context.Context, name string, spec StreamAppAclSpec) error {
	bindings, err := validateHelperBindings(spec.Principal, spec.Host, expandStreamApp(spec))
	if err != nil {
		return err
	}
	def, err := s.res.Lookup(name)
	if err != nil {
		return err
	}
	return s.port.CreateAcls(ctx, def, bindings)
}

// allow builds one ALLOW binding.
func allow(principal, host, resourceType, resourceName, pattern, op string) cluster.AclBinding {
	return cluster.AclBinding{
		Principal: principal, Host: host, ResourceType: resourceType, ResourceName: resourceName,
		PatternType: pattern, Operation: op, Permission: "ALLOW",
	}
}

// resourcesFor yields (name, pattern) pairs from a literal name list plus an
// optional prefix (both may coexist: each name is LITERAL, the prefix is
// PREFIXED).
type aclResource struct {
	name    string
	pattern string
}

func resourcesFor(names []string, prefix string) []aclResource {
	var out []aclResource
	seen := map[aclResource]struct{}{}
	for _, rawName := range names {
		n := strings.TrimSpace(rawName)
		resource := aclResource{n, "LITERAL"}
		if n == "" {
			continue
		}
		if _, duplicate := seen[resource]; duplicate {
			continue
		}
		seen[resource] = struct{}{}
		out = append(out, resource)
	}
	prefix = strings.TrimSpace(prefix)
	if prefix != "" {
		resource := aclResource{prefix, "PREFIXED"}
		if _, duplicate := seen[resource]; !duplicate {
			out = append(out, resource)
		}
	}
	return out
}

// expandConsumer: TOPIC{READ,DESCRIBE} + GROUP{READ,DESCRIBE} (upstream
// createAllowBindings consumer).
func expandConsumer(s ConsumerAclSpec) []cluster.AclBinding {
	var out []cluster.AclBinding
	for _, r := range resourcesFor(s.Topics, s.TopicsPrefix) {
		out = append(out, allow(s.Principal, s.Host, "TOPIC", r.name, r.pattern, "READ"))
		out = append(out, allow(s.Principal, s.Host, "TOPIC", r.name, r.pattern, "DESCRIBE"))
	}
	for _, r := range resourcesFor(s.ConsumerGroups, s.ConsumerGroupsPrefix) {
		out = append(out, allow(s.Principal, s.Host, "GROUP", r.name, r.pattern, "READ"))
		out = append(out, allow(s.Principal, s.Host, "GROUP", r.name, r.pattern, "DESCRIBE"))
	}
	return out
}

// expandProducer: TOPIC{WRITE,DESCRIBE,CREATE} + TRANSACTIONAL_ID{WRITE,
// DESCRIBE} + (idempotent) CLUSTER{IDEMPOTENT_WRITE} on "kafka-cluster".
func expandProducer(s ProducerAclSpec) []cluster.AclBinding {
	var out []cluster.AclBinding
	for _, r := range resourcesFor(s.Topics, s.TopicsPrefix) {
		out = append(out, allow(s.Principal, s.Host, "TOPIC", r.name, r.pattern, "WRITE"))
		out = append(out, allow(s.Principal, s.Host, "TOPIC", r.name, r.pattern, "DESCRIBE"))
		out = append(out, allow(s.Principal, s.Host, "TOPIC", r.name, r.pattern, "CREATE"))
	}
	for _, r := range resourcesFor(txnNames(s.TransactionalID), s.TransactionsIDPrefix) {
		out = append(out, allow(s.Principal, s.Host, "TRANSACTIONAL_ID", r.name, r.pattern, "WRITE"))
		out = append(out, allow(s.Principal, s.Host, "TRANSACTIONAL_ID", r.name, r.pattern, "DESCRIBE"))
	}
	if s.Idempotent {
		out = append(out, allow(s.Principal, s.Host, "CLUSTER", "kafka-cluster", "LITERAL", "IDEMPOTENT_WRITE"))
	}
	return out
}

func txnNames(id string) []string {
	if id == "" {
		return nil
	}
	return []string{id}
}

// expandStreamApp: input TOPIC{READ} LITERAL + output TOPIC{WRITE} LITERAL +
// applicationId GROUP{ALL} + TOPIC{ALL} PREFIXED.
func expandStreamApp(s StreamAppAclSpec) []cluster.AclBinding {
	var out []cluster.AclBinding
	for _, in := range resourcesFor(s.InputTopics, "") {
		out = append(out, allow(s.Principal, s.Host, "TOPIC", in.name, in.pattern, "READ"))
	}
	for _, output := range resourcesFor(s.OutputTopics, "") {
		out = append(out, allow(s.Principal, s.Host, "TOPIC", output.name, output.pattern, "WRITE"))
	}
	applicationID := strings.TrimSpace(s.ApplicationID)
	if applicationID != "" {
		out = append(out, allow(s.Principal, s.Host, "GROUP", applicationID, "PREFIXED", "ALL"))
		out = append(out, allow(s.Principal, s.Host, "TOPIC", applicationID, "PREFIXED", "ALL"))
	}
	return out
}

// aclCSVHeader is the fixed 7-column order (P2c-D4, verbatim upstream AclCsv).
var aclCSVHeader = []string{"Principal", "ResourceType", "PatternType", "ResourceName", "Operation", "PermissionType", "Host"}

// Enum column value sets (contract/openapi.yaml KafkaAclResourceType /
// KafkaAclNamePatternType / KafkaAcl.operation / KafkaAcl.permission), used
// by ParseAclCSV to reject bad enum values at parse time (upstream
// AclCsv.parseCsvLine does the same) rather than letting them fall through to
// an unwrapped infra error during SyncCSV's port.CreateAcls. USER is a valid
// ResourceType here -- kadm's lack of support for it is an infra sync-time
// concern (see acl_admin.go), not a parse-time rejection.
var validAclResourceType = map[string]struct{}{
	"UNKNOWN": {}, "TOPIC": {}, "GROUP": {}, "CLUSTER": {}, "TRANSACTIONAL_ID": {}, "DELEGATION_TOKEN": {}, "USER": {},
}
var supportedAclResourceType = map[string]struct{}{
	"TOPIC": {}, "GROUP": {}, "CLUSTER": {}, "TRANSACTIONAL_ID": {}, "DELEGATION_TOKEN": {},
}
var validAclPatternType = map[string]struct{}{
	"MATCH": {}, "LITERAL": {}, "PREFIXED": {},
}
var validAclOperation = map[string]struct{}{
	"UNKNOWN": {}, "ALL": {}, "READ": {}, "WRITE": {}, "CREATE": {}, "DELETE": {}, "ALTER": {}, "DESCRIBE": {},
	"CLUSTER_ACTION": {}, "DESCRIBE_CONFIGS": {}, "ALTER_CONFIGS": {}, "IDEMPOTENT_WRITE": {}, "CREATE_TOKENS": {}, "DESCRIBE_TOKENS": {},
}
var validAclPermission = map[string]struct{}{
	"ALLOW": {}, "DENY": {},
}

func validateAclBinding(binding cluster.AclBinding, allowMatch bool) error {
	if err := validateAclActor(binding.Principal, binding.Host); err != nil {
		return err
	}
	if strings.TrimSpace(binding.ResourceName) == "" {
		return fmt.Errorf("%w: resourceName must not be blank", ErrBadAclRequest)
	}
	if _, ok := supportedAclResourceType[binding.ResourceType]; !ok {
		return fmt.Errorf("%w: unsupported resourceType %q", ErrBadAclRequest, binding.ResourceType)
	}
	if _, ok := validAclOperation[binding.Operation]; !ok || binding.Operation == "UNKNOWN" {
		return fmt.Errorf("%w: invalid operation %q", ErrBadAclRequest, binding.Operation)
	}
	if _, ok := validAclPermission[binding.Permission]; !ok {
		return fmt.Errorf("%w: invalid permission %q", ErrBadAclRequest, binding.Permission)
	}
	validPattern := binding.PatternType == "LITERAL" || binding.PatternType == "PREFIXED" || allowMatch && binding.PatternType == "MATCH"
	if !validPattern {
		return fmt.Errorf("%w: invalid patternType %q", ErrBadAclRequest, binding.PatternType)
	}
	if binding.ResourceType == "CLUSTER" && (binding.ResourceName != "kafka-cluster" || binding.PatternType != "LITERAL") {
		return fmt.Errorf("%w: CLUSTER requires kafka-cluster with LITERAL pattern", ErrBadAclRequest)
	}
	return nil
}

func validateAclActor(principal, host string) error {
	if strings.TrimSpace(principal) == "" {
		return fmt.Errorf("%w: principal must not be blank", ErrBadAclRequest)
	}
	separator := strings.IndexByte(principal, ':')
	if separator <= 0 || separator == len(principal)-1 || strings.TrimSpace(principal[:separator]) == "" || strings.TrimSpace(principal[separator+1:]) == "" {
		return fmt.Errorf("%w: principal must use type:name form", ErrBadAclRequest)
	}
	if strings.TrimSpace(host) == "" {
		return fmt.Errorf("%w: host must not be blank", ErrBadAclRequest)
	}
	return nil
}

func validateHelperBindings(principal, host string, bindings []cluster.AclBinding) ([]cluster.AclBinding, error) {
	principal = strings.TrimSpace(principal)
	host = strings.TrimSpace(host)
	if err := validateAclActor(principal, host); err != nil {
		return nil, err
	}
	out := make([]cluster.AclBinding, 0, len(bindings))
	seen := make(map[cluster.AclBinding]struct{}, len(bindings))
	for _, binding := range bindings {
		binding.Principal = principal
		binding.Host = host
		if err := validateAclBinding(binding, false); err != nil {
			return nil, err
		}
		if _, duplicate := seen[binding]; duplicate {
			continue
		}
		seen[binding] = struct{}{}
		out = append(out, binding)
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("%w: helper request expands to no ACL bindings", ErrBadAclRequest)
	}
	return out, nil
}

// FormatAclCSV writes bindings as the fixed 7-column CSV (header + one row
// each), in the seven-field column order. It's a method (not a package func)
// so it can sit on the AclServicer interface the api layer consumes; the
// receiver is unused (pure over its argument). Returns any csv.Writer error.
func (s *AclService) FormatAclCSV(bindings []cluster.AclBinding) (string, error) {
	var buf strings.Builder
	w := csv.NewWriter(&buf)
	if err := w.Write(aclCSVHeader); err != nil {
		return "", err
	}
	for _, b := range bindings {
		if err := w.Write([]string{b.Principal, b.ResourceType, b.PatternType, b.ResourceName, b.Operation, b.Permission, b.Host}); err != nil {
			return "", err
		}
	}
	w.Flush()
	if err := w.Error(); err != nil {
		return "", err
	}
	return buf.String(), nil
}

// ParseAclCSV parses the fixed 7-column CSV into bindings. The first non-empty
// record is the header (validated == aclCSVHeader); every data record must have
// exactly 7 fields, and the four enum columns (ResourceType/PatternType/
// Operation/PermissionType) must each be one of the contract's legal enum
// values -- otherwise a bad row (e.g. ResourceType="BOGUS_TYPE") would parse
// clean and only blow up as an unwrapped infra error inside SyncCSV's
// port.CreateAcls (api 500), instead of the ErrBadAclCSV 400 it should be.
// Duplicate rows (identical across all seven fields) are deduped, keeping the
// first occurrence and preserving otherwise-stable order (mirrors upstream
// AclCsv.parseCsvLine collecting into a HashSet). Every parse/validation
// failure wraps ErrBadAclCSV (api 400), keeping it distinct from a backend
// failure during sync.
func ParseAclCSV(text string) ([]cluster.AclBinding, error) {
	r := csv.NewReader(strings.NewReader(text))
	r.FieldsPerRecord = -1 // validate width ourselves for a precise error
	records, err := r.ReadAll()
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrBadAclCSV, err)
	}
	seen := map[cluster.AclBinding]struct{}{}
	var out []cluster.AclBinding
	headerSeen := false
	for i, rec := range records {
		if len(rec) == 1 && strings.TrimSpace(rec[0]) == "" {
			continue // leading/inter-record physical blank line
		}
		if !headerSeen {
			if len(rec) != len(aclCSVHeader) || !equalCSVHeader(rec) {
				return nil, fmt.Errorf("%w: first non-empty record must be the exact ACL header", ErrBadAclCSV)
			}
			headerSeen = true
			continue // skip header
		}
		if len(rec) != 7 {
			return nil, fmt.Errorf("%w: row %d must have 7 columns, got %d", ErrBadAclCSV, i+1, len(rec))
		}
		for column, value := range rec {
			rec[column] = strings.TrimSpace(value)
			if rec[column] == "" {
				return nil, fmt.Errorf("%w: row %d column %d must not be blank", ErrBadAclCSV, i+1, column+1)
			}
		}
		if _, ok := validAclResourceType[rec[1]]; !ok {
			return nil, fmt.Errorf("%w: row %d: invalid ResourceType %q", ErrBadAclCSV, i+1, rec[1])
		}
		if _, ok := validAclPatternType[rec[2]]; !ok {
			return nil, fmt.Errorf("%w: row %d: invalid PatternType %q", ErrBadAclCSV, i+1, rec[2])
		}
		if _, ok := validAclOperation[rec[4]]; !ok {
			return nil, fmt.Errorf("%w: row %d: invalid Operation %q", ErrBadAclCSV, i+1, rec[4])
		}
		if _, ok := validAclPermission[rec[5]]; !ok {
			return nil, fmt.Errorf("%w: row %d: invalid PermissionType %q", ErrBadAclCSV, i+1, rec[5])
		}
		b := cluster.AclBinding{
			Principal: rec[0], ResourceType: rec[1], PatternType: rec[2], ResourceName: rec[3],
			Operation: rec[4], Permission: rec[5], Host: rec[6],
		}
		if _, dup := seen[b]; dup {
			continue
		}
		seen[b] = struct{}{}
		out = append(out, b)
	}
	if !headerSeen {
		return nil, fmt.Errorf("%w: missing ACL header", ErrBadAclCSV)
	}
	return out, nil
}

func equalCSVHeader(record []string) bool {
	for i, value := range aclCSVHeader {
		if record[i] != value {
			return false
		}
	}
	return true
}

// SyncCSV resolves name, describes current ACLs, parses the desired set from
// csvText, then applies create-before-delete (P2c-D4: toAdd first so no
// permission vacuum): toAdd = desired - current, toDelete = current - desired.
func (s *AclService) SyncCSV(ctx context.Context, name string, csvText string) error {
	desired, err := ParseAclCSV(csvText)
	if err != nil {
		return err
	}
	def, err := s.res.Lookup(name)
	if err != nil {
		return err
	}
	unlock := s.syncLocks.lock(name)
	defer unlock()

	current, err := s.port.ListAcls(ctx, def, cluster.AclFilter{})
	if err != nil {
		return err
	}
	currentSet := map[cluster.AclBinding]struct{}{}
	for _, b := range current {
		currentSet[b] = struct{}{}
	}
	desiredSet := map[cluster.AclBinding]struct{}{}
	var toAdd []cluster.AclBinding
	for _, b := range desired {
		desiredSet[b] = struct{}{}
		if _, ok := currentSet[b]; !ok {
			toAdd = append(toAdd, b)
		}
	}
	var toDelete []cluster.AclBinding
	for b := range currentSet {
		if _, ok := desiredSet[b]; !ok {
			toDelete = append(toDelete, b)
		}
	}
	for _, binding := range toAdd {
		if err := validateAclBinding(binding, false); err != nil {
			return err
		}
	}
	for _, binding := range toDelete {
		if err := validateAclBinding(binding, true); err != nil {
			return err
		}
	}
	if len(toAdd) > 0 {
		if err := s.port.CreateAcls(ctx, def, toAdd); err != nil {
			return err
		}
	}
	sort.SliceStable(toDelete, func(i, j int) bool { return aclBindingLess(toDelete[i], toDelete[j]) })
	for _, b := range toDelete {
		if _, err := s.port.DeleteAcls(ctx, def, b); err != nil {
			return err
		}
	}
	return nil
}
