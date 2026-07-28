package kafka

// acls.go implements cluster.AclAdminPort (P2c Task 2) against the pooled
// kadm client. Read side (ListAcls) reuses P1b's kadm->contract enum mappers
// (topics.go's aclXxxToContract) after a broad DescribeACLs; write side
// (CreateAcls/DeleteAcls) needs the reverse contract-text->kadm mapping added
// here, since the ACLBuilder is fed kadm enum values, not contract strings.
//
// CreateAcls issues one CreateACLs call per binding, each with a single-valued
// builder: kadm's ACLBuilder multiplies principals*hosts*resources*operations,
// so packing heterogeneous bindings (e.g. TOPIC{READ} + GROUP{ALL} with
// different patterns) into one builder would create the wrong cross product.
// The app layer's helper expansion (acl.go) already flattens everything to a
// list of atomic bindings, so per-binding creation is both correct and simple.

import (
	"context"
	"fmt"
	"strings"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/cy-kaf/cy-kaf-client/internal/domain/cluster"
)

var _ cluster.AclAdminPort = (*Pool)(nil)

// aclPatternFromContract maps the contract's KafkaAclNamePatternType text onto
// kadm's ACLPattern. Only an unset list filter maps to ACLPatternAny; unknown
// values are rejected so a typo cannot broaden a describe/delete operation.
func aclPatternFromContract(pt string) (kadm.ACLPattern, error) {
	switch pt {
	case "LITERAL":
		return kadm.ACLPatternLiteral, nil
	case "PREFIXED":
		return kadm.ACLPatternPrefixed, nil
	case "MATCH":
		return kadm.ACLPatternMatch, nil
	case "":
		return kadm.ACLPatternAny, nil
	default:
		return 0, fmt.Errorf("invalid acl pattern type %q", pt)
	}
}

// aclOperationFromContract maps the contract's KafkaAclOperation text onto
// kadm's ACLOperation via kmsg.ParseACLOperation (normalizes by stripping
// dots/underscores/dashes and lowercasing, so "IDEMPOTENT_WRITE" ->
// idempotentwrite -> ACLOperationIdempotentWrite). An unparseable value is an
// error (createAcl 400 upstream).
func aclOperationFromContract(op string) (kadm.ACLOperation, error) {
	parsed, err := kmsg.ParseACLOperation(op)
	if err != nil {
		return 0, fmt.Errorf("unknown acl operation %q: %w", op, err)
	}
	return parsed, nil
}

// aclResourceTypeRecognized reports whether resourceType is either "" (unset,
// meaning "any resource type") or one of the contract's KafkaAclResourceType
// values that kadm's ACLBuilder actually has a typed setter for. USER,
// UNKNOWN, and any other unrecognized value are not recognized -- see
// applyAclResource's doc comment for why. Exported at package level (rather
// than folded into applyAclResource) so aclFilterToBuilder's type-only branch
// (no concrete resourceName to hand a typed setter) can still validate the
// type up front, before falling back to AnyResource().
func aclResourceTypeRecognized(resourceType string) bool {
	switch resourceType {
	case "", "TOPIC", "GROUP", "CLUSTER", "TRANSACTIONAL_ID", "DELEGATION_TOKEN":
		return true
	default:
		return false
	}
}

// unsupportedAclResourceTypeErr builds the error returned for a resourceType
// aclResourceTypeRecognized rejects, shared so CreateAcls/DeleteAcls (via
// applyAclResource) and ListAcls (via aclFilterToBuilder's type-only branch)
// report the same message.
func unsupportedAclResourceTypeErr(resourceType string) error {
	return fmt.Errorf("acl resource type %q not supported: kadm ACLBuilder has no setter for it", resourceType)
}

// applyAclResource sets the ACL's resource dimension on b by resource type,
// returning an error when resourceType names a resource kadm's ACLBuilder has
// no setter for. CLUSTER ignores name (Kafka's cluster ACL resource is the
// singleton "kafka-cluster"; kadm's Clusters() takes no name). "" (unset,
// list only) matches any resource type via AnyResource.
//
// Capability gap: USER is a real KafkaAclResourceType contract value, but
// kadm's ACLBuilder has no Users() setter (confirmed against kadm source --
// the type dimension only covers Topic/Group/Cluster/TransactionalID/
// DelegationToken). This function used to silently fall back to
// AnyResource() for USER (and UNKNOWN, and anything else unrecognized),
// which made CreateAcls(USER) a silent no-op success and made
// DeleteAcls/ListAcls(USER) quietly degrade into ANY-resource operations
// (over-broad delete/list). It now rejects USER/UNKNOWN/any unrecognized
// value with an explicit error instead. ADR-0009 records this kadm
// capability gap.
func applyAclResource(b *kadm.ACLBuilder, resourceType, resourceName string) error {
	if !aclResourceTypeRecognized(resourceType) {
		return unsupportedAclResourceTypeErr(resourceType)
	}
	switch resourceType {
	case "TOPIC":
		b.Topics(resourceName)
	case "GROUP":
		b.Groups(resourceName)
	case "CLUSTER":
		b.Clusters()
	case "TRANSACTIONAL_ID":
		b.TransactionalIDs(resourceName)
	case "DELEGATION_TOKEN":
		b.DelegationTokens(resourceName)
	default:
		// "" (unset): matches any resource type.
		b.AnyResource()
	}
	return nil
}

// aclBuilderForBinding builds a single-ACL builder for one fully-specified
// binding (create/delete). Permission folds into Allow/Deny (kadm's model).
func aclBuilderForBinding(binding cluster.AclBinding, allowMatch bool) (*kadm.ACLBuilder, error) {
	if err := validateInfraAclBinding(binding, allowMatch); err != nil {
		return nil, err
	}
	op, err := aclOperationFromContract(binding.Operation)
	if err != nil {
		return nil, err
	}
	pattern, err := aclPatternFromContract(binding.PatternType)
	if err != nil {
		return nil, err
	}
	b := kadm.NewACLs().ResourcePatternType(pattern)
	if err := applyAclResource(b, binding.ResourceType, binding.ResourceName); err != nil {
		return nil, err
	}
	b.Operations(op)
	if binding.Permission == "DENY" {
		b.Deny(binding.Principal).DenyHosts(binding.Host)
	} else {
		b.Allow(binding.Principal).AllowHosts(binding.Host)
	}
	return b, nil
}

func validateInfraAclBinding(binding cluster.AclBinding, allowMatch bool) error {
	separator := strings.IndexByte(binding.Principal, ':')
	if separator <= 0 || separator == len(binding.Principal)-1 || strings.TrimSpace(binding.Principal[:separator]) == "" || strings.TrimSpace(binding.Principal[separator+1:]) == "" {
		return fmt.Errorf("invalid acl principal %q: expected type:name", binding.Principal)
	}
	if strings.TrimSpace(binding.Host) == "" || strings.TrimSpace(binding.ResourceName) == "" {
		return fmt.Errorf("invalid acl binding: host and resourceName must not be blank")
	}
	if !aclResourceTypeRecognized(binding.ResourceType) || binding.ResourceType == "" {
		return unsupportedAclResourceTypeErr(binding.ResourceType)
	}
	patternValid := binding.PatternType == "LITERAL" || binding.PatternType == "PREFIXED" || allowMatch && binding.PatternType == "MATCH"
	if !patternValid {
		return fmt.Errorf("invalid acl pattern type %q", binding.PatternType)
	}
	if binding.Permission != "ALLOW" && binding.Permission != "DENY" {
		return fmt.Errorf("invalid acl permission %q", binding.Permission)
	}
	if !infraAclOperationRecognized(binding.Operation) {
		return fmt.Errorf("invalid acl operation %q", binding.Operation)
	}
	if binding.ResourceType == "CLUSTER" && (binding.ResourceName != "kafka-cluster" || binding.PatternType != "LITERAL") {
		return fmt.Errorf("invalid CLUSTER acl: requires kafka-cluster with LITERAL pattern")
	}
	return nil
}

func infraAclOperationRecognized(operation string) bool {
	switch operation {
	case "ALL", "READ", "WRITE", "CREATE", "DELETE", "ALTER", "DESCRIBE",
		"CLUSTER_ACTION", "DESCRIBE_CONFIGS", "ALTER_CONFIGS", "IDEMPOTENT_WRITE",
		"CREATE_TOKENS", "DESCRIBE_TOKENS":
		return true
	default:
		return false
	}
}

func aclBuildersForCreate(bindings []cluster.AclBinding) ([]*kadm.ACLBuilder, error) {
	builders := make([]*kadm.ACLBuilder, 0, len(bindings))
	for _, binding := range bindings {
		builder, err := aclBuilderForBinding(binding, false)
		if err != nil {
			return nil, err
		}
		builders = append(builders, builder)
	}
	return builders, nil
}

// aclFilterToBuilder builds a broad describe filter from an AclFilter: pattern
// type pushed down (ANY when unset), resource pushed down only when both type
// AND name are present. That "both present" gate is a limitation of this
// implementation, not of kadm: kadm's Topics()/Groups()/etc are variadic, and
// a bare zero-arg call (e.g. Topics()) sets anyTopic=true internally, i.e. it
// *does* match every resource of that type -- kadm itself can express "this
// type, any name". applyAclResource's resourceName parameter is a single
// string (not variadic) so it can't express that shape today; when a filter
// gives a type but no name, we fall back to AnyResource() here and rely on
// the app layer (AclService.List, Task 4) to locally re-filter by
// ResourceType/ResourceName as a backstop. Pushing the "type + any name"
// filter down to the server properly is a possible follow-up, not a
// correctness gap, since the app-layer backstop already narrows results.
//
// Even in that type-only (name absent) branch, resourceType is still
// validated up front via aclResourceTypeRecognized, so USER/UNKNOWN fails
// loudly here too instead of silently degrading into an ANY-resource
// describe (see applyAclResource's doc comment for the underlying kadm
// capability gap). Principal/operation/permission are left wide
// (Allow/AllowHosts/Deny/DenyHosts/Operations with no args = "any"), the same
// broad-describe shape P1b's TopicAcls uses.
func aclFilterToBuilder(f cluster.AclFilter) (*kadm.ACLBuilder, error) {
	if !aclResourceTypeRecognized(f.ResourceType) {
		return nil, unsupportedAclResourceTypeErr(f.ResourceType)
	}
	pattern, err := aclPatternFromContract(f.PatternType)
	if err != nil {
		return nil, err
	}
	b := kadm.NewACLs().ResourcePatternType(pattern)
	if f.ResourceType != "" && f.ResourceName != "" {
		if err := applyAclResource(b, f.ResourceType, f.ResourceName); err != nil {
			return nil, err
		}
	} else {
		b.AnyResource()
	}
	return b.Allow().AllowHosts().Deny().DenyHosts().Operations(), nil
}

// ListAcls broad-describes ACLs matching filter's resource dimension, mapping
// each described ACL back to a contract-text AclBinding. Principal-substring /
// fts filtering and stable sort are the app layer's job (AclService.List) --
// this method only pushes down the resource/pattern dimension and translates
// kadm enums to contract text.
func (p *Pool) ListAcls(ctx context.Context, def cluster.Definition, filter cluster.AclFilter) ([]cluster.AclBinding, error) {
	c, err := p.clientFor(def)
	if err != nil {
		return nil, err
	}
	builder, err := aclFilterToBuilder(filter)
	if err != nil {
		return nil, err
	}
	results, err := c.adm.DescribeACLs(ctx, builder)
	if err != nil {
		return nil, fmt.Errorf("describe acls: %w", err)
	}
	var out []cluster.AclBinding
	for _, r := range results {
		if r.Err != nil {
			return nil, r.Err
		}
		for _, acl := range r.Described {
			out = append(out, cluster.AclBinding{
				Principal:    acl.Principal,
				Host:         acl.Host,
				ResourceName: acl.Name,
				ResourceType: aclResourceTypeToContract(acl.Type),
				PatternType:  aclPatternTypeToContract(acl.Pattern),
				Operation:    aclOperationToContract(acl.Operation),
				Permission:   aclPermissionToContract(acl.Permission),
			})
		}
	}
	return out, nil
}

// CreateAcls creates each binding via its own single-valued builder (see file
// doc comment on why not one batched builder). First error aborts.
func (p *Pool) CreateAcls(ctx context.Context, def cluster.Definition, bindings []cluster.AclBinding) error {
	builders, err := aclBuildersForCreate(bindings)
	if err != nil {
		return err
	}
	c, err := p.clientFor(def)
	if err != nil {
		return err
	}
	for _, builder := range builders {
		results, err := c.adm.CreateACLs(ctx, builder)
		if err != nil {
			return fmt.Errorf("create acls: %w", err)
		}
		for _, res := range results {
			if res.Err != nil {
				return res.Err
			}
		}
	}
	return nil
}

// DeleteAcls deletes ACLs matching binding exactly, returning how many were
// actually removed (0 -> caller 404s). The delete filter is the same single-
// valued builder create uses, so it matches exactly the one binding.
func (p *Pool) DeleteAcls(ctx context.Context, def cluster.Definition, binding cluster.AclBinding) (int, error) {
	c, err := p.clientFor(def)
	if err != nil {
		return 0, err
	}
	b, err := aclBuilderForBinding(binding, true)
	if err != nil {
		return 0, err
	}
	results, err := c.adm.DeleteACLs(ctx, b)
	if err != nil {
		return 0, fmt.Errorf("delete acls: %w", err)
	}
	deleted := 0
	for _, res := range results {
		if res.Err != nil {
			return 0, res.Err
		}
		deleted += len(res.Deleted)
	}
	return deleted, nil
}
