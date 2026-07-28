package kafka

// quotas.go implements cluster.QuotaPort (P2c Task 3) against the pooled kadm
// client. ListQuotas describes all client quotas (no filter, strict=false);
// UpsertQuotas does a full-replace (P2c-D5): the request's quotas map is the
// complete desired state, so any key currently on the entity but absent from
// the request is Removed, not merged.

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"

	"github.com/cy-kaf/cy-kaf-client/internal/domain/cluster"
)

var _ cluster.QuotaPort = (*Pool)(nil)

const (
	quotaStateVisibilityTimeout = 30 * time.Second
	quotaStatePollInterval      = 100 * time.Millisecond
)

type clientQuotaAdmin interface {
	DescribeClientQuotas(context.Context, bool, []kadm.DescribeClientQuotaComponent) (kadm.DescribedClientQuotas, error)
	AlterClientQuotas(context.Context, []kadm.AlterClientQuotaEntry) (kadm.AlteredClientQuotas, error)
}

type quotaComponentKey struct {
	componentType string
	hasName       bool
	name          string
}

type quotaEntityKey struct {
	count      int
	components [3]quotaComponentKey
}

type quotaLockKey struct {
	cluster string
	entity  quotaEntityKey
}

func newQuotaEntityKey(entity kadm.ClientQuotaEntity) quotaEntityKey {
	components := make([]quotaComponentKey, 0, len(entity))
	for _, component := range entity {
		key := quotaComponentKey{componentType: component.Type, hasName: component.Name != nil}
		if component.Name != nil {
			key.name = *component.Name
		}
		components = append(components, key)
	}
	sort.Slice(components, func(i, j int) bool {
		if components[i].componentType != components[j].componentType {
			return components[i].componentType < components[j].componentType
		}
		if components[i].hasName != components[j].hasName {
			return !components[i].hasName
		}
		return components[i].name < components[j].name
	})
	key := quotaEntityKey{count: len(components)}
	copy(key.components[:], components)
	return key
}

// quotaEntityFrom builds a kadm ClientQuotaEntity from a domain ClientQuota's
// three dimensions, in a stable order (user, client-id, ip); an empty
// dimension is omitted (a two-component entity like user+client-id is valid).
func quotaEntityFrom(q cluster.ClientQuota) kadm.ClientQuotaEntity {
	var e kadm.ClientQuotaEntity
	if q.User != "" {
		name := q.User
		e = append(e, kadm.ClientQuotaEntityComponent{Type: "user", Name: &name})
	}
	if q.ClientID != "" {
		name := q.ClientID
		e = append(e, kadm.ClientQuotaEntityComponent{Type: "client-id", Name: &name})
	}
	if q.IP != "" {
		name := q.IP
		e = append(e, kadm.ClientQuotaEntityComponent{Type: "ip", Name: &name})
	}
	return e
}

// clientQuotaFromEntity maps a described kadm entity+values back to a domain
// ClientQuota. A nil component Name (the "default" entity) degrades to "" --
// the contract's ClientQuotas has no notion of a default entity, so a defaulted
// dimension is reported as unset rather than invented.
func clientQuotaFromEntity(entity kadm.ClientQuotaEntity, values kadm.ClientQuotaValues) cluster.ClientQuota {
	out := cluster.ClientQuota{Quotas: map[string]float64{}}
	for _, comp := range entity {
		name := ""
		if comp.Name != nil {
			name = *comp.Name
		}
		switch comp.Type {
		case "user":
			out.User = name
		case "client-id":
			out.ClientID = name
		case "ip":
			out.IP = name
		}
	}
	for _, v := range values {
		out.Quotas[v.Key] = v.Value
	}
	return out
}

// quotaEntitiesEqual compares entities as typed component multisets. Kafka
// does not assign identity to component order; pointer addresses are likewise
// irrelevant, while type, nil/default, pointed-to value, and duplicate count
// all remain significant.
func quotaEntitiesEqual(left, right kadm.ClientQuotaEntity) bool {
	if len(left) != len(right) {
		return false
	}
	matched := make([]bool, len(right))
	for _, leftComponent := range left {
		found := false
		for i, rightComponent := range right {
			if matched[i] || !quotaEntityComponentsEqual(leftComponent, rightComponent) {
				continue
			}
			matched[i] = true
			found = true
			break
		}
		if !found {
			return false
		}
	}
	return true
}

func quotaEntityComponentsEqual(left, right kadm.ClientQuotaEntityComponent) bool {
	if left.Type != right.Type || (left.Name == nil) != (right.Name == nil) {
		return false
	}
	return left.Name == nil || *left.Name == *right.Name
}

// ListQuotas describes every client quota (strict=false, no components => all).
func (p *Pool) ListQuotas(ctx context.Context, def cluster.Definition) ([]cluster.ClientQuota, error) {
	c, err := p.clientFor(def)
	if err != nil {
		return nil, err
	}
	described, err := c.adm.DescribeClientQuotas(ctx, false, nil)
	if err != nil {
		return nil, fmt.Errorf("describe client quotas: %w", err)
	}
	out := make([]cluster.ClientQuota, 0, len(described))
	for _, dq := range described {
		out = append(out, clientQuotaFromEntity(dq.Entity, dq.Values))
	}
	return out, nil
}

// UpsertQuotas full-replaces the entity's quotas: Set every requested key,
// Remove every currently-stored key the request omits (P2c-D5).
func (p *Pool) UpsertQuotas(ctx context.Context, def cluster.Definition, quota cluster.ClientQuota) error {
	c, err := p.clientFor(def)
	if err != nil {
		return err
	}
	entity := quotaEntityFrom(quota)
	return upsertQuotaEntityLocked(ctx, &p.quotaLocks, def.Name, c.adm, entity, quota.Quotas)
}

func upsertQuotaEntityLocked(
	ctx context.Context,
	locks *keyedLocker[quotaLockKey],
	clusterName string,
	admin clientQuotaAdmin,
	entity kadm.ClientQuotaEntity,
	desired map[string]float64,
) error {
	unlock := locks.lock(quotaLockKey{cluster: clusterName, entity: newQuotaEntityKey(entity)})
	defer unlock()
	return upsertQuotaEntity(ctx, admin, entity, desired)
}

func upsertQuotaEntity(ctx context.Context, admin clientQuotaAdmin, entity kadm.ClientQuotaEntity, desired map[string]float64) error {
	existing, _, err := describeQuotaEntity(ctx, admin, entity)
	if err != nil {
		return err
	}

	desiredKeys := make([]string, 0, len(desired))
	for key := range desired {
		desiredKeys = append(desiredKeys, key)
	}
	sort.Strings(desiredKeys)
	removeKeys := make([]string, 0, len(existing))
	for key := range existing {
		if _, keep := desired[key]; !keep {
			removeKeys = append(removeKeys, key)
		}
	}
	sort.Strings(removeKeys)

	ops := make([]kadm.AlterClientQuotaOp, 0, len(desiredKeys)+len(removeKeys))
	for _, key := range desiredKeys {
		ops = append(ops, kadm.AlterClientQuotaOp{Key: key, Value: desired[key]})
	}
	for _, key := range removeKeys {
		ops = append(ops, kadm.AlterClientQuotaOp{Key: key, Remove: true})
	}
	if len(ops) == 0 {
		return nil // the only no-op is an already-empty desired entity
	}
	altered, err := admin.AlterClientQuotas(ctx, []kadm.AlterClientQuotaEntry{{Entity: entity, Ops: ops}})
	if err != nil {
		return fmt.Errorf("alter client quotas: %w", err)
	}
	for _, a := range altered {
		if a.Err != nil {
			return a.Err
		}
	}
	return waitForExactQuotaState(ctx, admin, entity, desired)
}

func describeQuotaEntity(ctx context.Context, admin clientQuotaAdmin, entity kadm.ClientQuotaEntity) (map[string]float64, bool, error) {
	described, err := admin.DescribeClientQuotas(ctx, false, nil)
	if err != nil {
		return nil, false, fmt.Errorf("describe client quotas: %w", err)
	}
	for _, quota := range described {
		if !quotaEntitiesEqual(quota.Entity, entity) {
			continue
		}
		values := make(map[string]float64, len(quota.Values))
		for _, value := range quota.Values {
			values[value.Key] = value.Value
		}
		return values, true, nil
	}
	return map[string]float64{}, false, nil
}

func waitForExactQuotaState(ctx context.Context, admin clientQuotaAdmin, entity kadm.ClientQuotaEntity, desired map[string]float64) error {
	waitCtx, cancel := context.WithTimeout(ctx, quotaStateVisibilityTimeout)
	defer cancel()
	for {
		current, _, err := describeQuotaEntity(waitCtx, admin, entity)
		if err != nil {
			return err
		}
		if quotaValuesEqual(current, desired) {
			return nil
		}
		timer := time.NewTimer(quotaStatePollInterval)
		select {
		case <-waitCtx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return fmt.Errorf("wait for exact client quota state: %w", waitCtx.Err())
		case <-timer.C:
		}
	}
}

func quotaValuesEqual(left, right map[string]float64) bool {
	if len(left) != len(right) {
		return false
	}
	for key, value := range left {
		if rightValue, ok := right[key]; !ok || rightValue != value {
			return false
		}
	}
	return true
}
