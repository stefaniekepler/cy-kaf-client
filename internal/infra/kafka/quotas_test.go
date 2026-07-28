package kafka

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kadm"

	"github.com/cy-kaf/cy-kaf-client/internal/domain/cluster"
)

func TestQuotaEntityFromAllDimensionsSet(t *testing.T) {
	e := quotaEntityFrom(cluster.ClientQuota{User: "alice", ClientID: "app1", IP: "10.0.0.1"})
	require.Len(t, e, 3)
	require.Equal(t, "user", e[0].Type)
	require.Equal(t, "alice", *e[0].Name)
	require.Equal(t, "client-id", e[1].Type)
	require.Equal(t, "app1", *e[1].Name)
	require.Equal(t, "ip", e[2].Type)
	require.Equal(t, "10.0.0.1", *e[2].Name)
}

func TestQuotaEntityFromOmitsEmptyDimensions(t *testing.T) {
	e := quotaEntityFrom(cluster.ClientQuota{User: "alice"})
	require.Len(t, e, 1)
	require.Equal(t, "user", e[0].Type)
	require.Equal(t, "alice", *e[0].Name)
}

func TestQuotaEntityFromAllEmptyYieldsEmptyEntity(t *testing.T) {
	e := quotaEntityFrom(cluster.ClientQuota{})
	require.Empty(t, e)
}

func TestClientQuotaFromEntityMapsAllDimensionsAndValues(t *testing.T) {
	user, clientID, ip := "alice", "app1", "10.0.0.1"
	entity := kadm.ClientQuotaEntity{
		{Type: "user", Name: &user},
		{Type: "client-id", Name: &clientID},
		{Type: "ip", Name: &ip},
	}
	values := kadm.ClientQuotaValues{
		{Key: "producer_byte_rate", Value: 1024},
		{Key: "consumer_byte_rate", Value: 2048},
	}
	got := clientQuotaFromEntity(entity, values)
	require.Equal(t, cluster.ClientQuota{
		User: "alice", ClientID: "app1", IP: "10.0.0.1",
		Quotas: map[string]float64{"producer_byte_rate": 1024, "consumer_byte_rate": 2048},
	}, got)
}

// TestClientQuotaFromEntityNilNameDegradesToEmpty proves a "default" entity
// component (nil Name, kadm's representation of the wildcard default) maps to
// "" rather than a literal "<default>" or similar -- the contract's
// ClientQuota has no notion of a default entity.
func TestClientQuotaFromEntityNilNameDegradesToEmpty(t *testing.T) {
	entity := kadm.ClientQuotaEntity{{Type: "user", Name: nil}}
	got := clientQuotaFromEntity(entity, nil)
	require.Equal(t, "", got.User)
	require.Empty(t, got.Quotas)
}

func TestClientQuotaFromEntityUnknownComponentTypeIgnored(t *testing.T) {
	name := "x"
	entity := kadm.ClientQuotaEntity{{Type: "not-a-real-dimension", Name: &name}}
	got := clientQuotaFromEntity(entity, nil)
	require.Equal(t, cluster.ClientQuota{Quotas: map[string]float64{}}, got)
}

func TestQuotaEntitiesEqualUsesPointerValues(t *testing.T) {
	user, clientID := "alice", "app1"
	a := kadm.ClientQuotaEntity{{Type: "user", Name: &user}, {Type: "client-id", Name: &clientID}}
	userCopy, clientIDCopy := user, clientID
	b := kadm.ClientQuotaEntity{{Type: "user", Name: &userCopy}, {Type: "client-id", Name: &clientIDCopy}}
	require.True(t, quotaEntitiesEqual(a, b))
}

func TestQuotaEntitiesEqualIgnoresComponentOrder(t *testing.T) {
	user, clientID := "alice", "app1"
	a := kadm.ClientQuotaEntity{{Type: "user", Name: &user}, {Type: "client-id", Name: &clientID}}
	b := kadm.ClientQuotaEntity{{Type: "client-id", Name: &clientID}, {Type: "user", Name: &user}}
	require.True(t, quotaEntitiesEqual(a, b))
}

func TestQuotaEntitiesEqualPreservesDuplicateMultiplicity(t *testing.T) {
	alice, bob := "alice", "bob"
	left := kadm.ClientQuotaEntity{{Type: "user", Name: &alice}, {Type: "user", Name: &alice}}
	right := kadm.ClientQuotaEntity{{Type: "user", Name: &alice}, {Type: "user", Name: &bob}}
	require.False(t, quotaEntitiesEqual(left, right))
}

func TestQuotaEntitiesEqualDistinguishesDefaultFromExplicitEmpty(t *testing.T) {
	empty := ""
	defaultUser := kadm.ClientQuotaEntity{{Type: "user", Name: nil}}
	explicitEmptyUser := kadm.ClientQuotaEntity{{Type: "user", Name: &empty}}
	require.False(t, quotaEntitiesEqual(defaultUser, explicitEmptyUser))
}

func TestQuotaEntitiesEqualCannotCollideOnDelimiters(t *testing.T) {
	compound, clientID, user := "b;user=a", "b", "a"
	oneComponent := kadm.ClientQuotaEntity{{Type: "client-id", Name: &compound}}
	twoComponents := kadm.ClientQuotaEntity{{Type: "client-id", Name: &clientID}, {Type: "user", Name: &user}}
	require.False(t, quotaEntitiesEqual(oneComponent, twoComponents))
}

type statefulQuotaAdmin struct {
	mu              sync.Mutex
	entity          kadm.ClientQuotaEntity
	values          map[string]float64
	describeCalls   int
	alterCalls      int
	blockFirstTwo   bool
	describeEntered chan int
	releaseDescribe chan struct{}
}

func (a *statefulQuotaAdmin) DescribeClientQuotas(context.Context, bool, []kadm.DescribeClientQuotaComponent) (kadm.DescribedClientQuotas, error) {
	a.mu.Lock()
	a.describeCalls++
	call := a.describeCalls
	values := make(kadm.ClientQuotaValues, 0, len(a.values))
	for key, value := range a.values {
		values = append(values, kadm.ClientQuotaValue{Key: key, Value: value})
	}
	a.mu.Unlock()

	if a.blockFirstTwo && call <= 2 {
		a.describeEntered <- call
		<-a.releaseDescribe
	}
	if len(values) == 0 {
		return nil, nil
	}
	return kadm.DescribedClientQuotas{{Entity: a.entity, Values: values}}, nil
}

func (a *statefulQuotaAdmin) AlterClientQuotas(_ context.Context, entries []kadm.AlterClientQuotaEntry) (kadm.AlteredClientQuotas, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.alterCalls++
	for _, operation := range entries[0].Ops {
		if operation.Remove {
			delete(a.values, operation.Key)
		} else {
			a.values[operation.Key] = operation.Value
		}
	}
	return kadm.AlteredClientQuotas{{Entity: entries[0].Entity}}, nil
}

func (a *statefulQuotaAdmin) snapshot() map[string]float64 {
	a.mu.Lock()
	defer a.mu.Unlock()
	out := make(map[string]float64, len(a.values))
	for key, value := range a.values {
		out[key] = value
	}
	return out
}

func TestUpsertQuotaEntitySerializesSameEntityThroughVisibility(t *testing.T) {
	user := "alice"
	entity := kadm.ClientQuotaEntity{{Type: "user", Name: &user}}
	admin := &statefulQuotaAdmin{
		entity: entity, values: map[string]float64{}, blockFirstTwo: true,
		describeEntered: make(chan int, 8), releaseDescribe: make(chan struct{}),
	}
	var locks keyedLocker[quotaLockKey]
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	errCh := make(chan error, 2)
	go func() {
		errCh <- upsertQuotaEntityLocked(ctx, &locks, "c1", admin, entity, map[string]float64{"producer_byte_rate": 1})
	}()
	require.Equal(t, 1, <-admin.describeEntered)
	go func() {
		errCh <- upsertQuotaEntityLocked(ctx, &locks, "c1", admin, entity, map[string]float64{"consumer_byte_rate": 2})
	}()

	select {
	case call := <-admin.describeEntered:
		require.Failf(t, "same entity was not serialized", "describe call %d entered before the first replacement completed", call)
	case <-time.After(100 * time.Millisecond):
	}
	close(admin.releaseDescribe)
	require.NoError(t, <-errCh)
	require.NoError(t, <-errCh)

	final := admin.snapshot()
	require.Len(t, final, 1, "serialized replacements must finish as A or B, never their union")
	_, hasProducer := final["producer_byte_rate"]
	_, hasConsumer := final["consumer_byte_rate"]
	require.NotEqual(t, hasProducer, hasConsumer)
	require.Empty(t, locks.entries, "idle keyed locks must be reclaimed")
}

type scriptedQuotaAdmin struct {
	describes     []kadm.DescribedClientQuotas
	describeCalls int
	alterCalls    int
}

func (a *scriptedQuotaAdmin) DescribeClientQuotas(context.Context, bool, []kadm.DescribeClientQuotaComponent) (kadm.DescribedClientQuotas, error) {
	index := a.describeCalls
	a.describeCalls++
	if index >= len(a.describes) {
		index = len(a.describes) - 1
	}
	return a.describes[index], nil
}

func (a *scriptedQuotaAdmin) AlterClientQuotas(_ context.Context, entries []kadm.AlterClientQuotaEntry) (kadm.AlteredClientQuotas, error) {
	a.alterCalls++
	return kadm.AlteredClientQuotas{{Entity: entries[0].Entity}}, nil
}

func describedQuota(entity kadm.ClientQuotaEntity, values map[string]float64) kadm.DescribedClientQuotas {
	quotaValues := make(kadm.ClientQuotaValues, 0, len(values))
	for key, value := range values {
		quotaValues = append(quotaValues, kadm.ClientQuotaValue{Key: key, Value: value})
	}
	if len(quotaValues) == 0 {
		return nil
	}
	return kadm.DescribedClientQuotas{{Entity: entity, Values: quotaValues}}
}

func TestUpsertQuotaEntityWaitsPastStaleAndPartialStatesUntilExact(t *testing.T) {
	user := "alice"
	entity := kadm.ClientQuotaEntity{{Type: "user", Name: &user}}
	admin := &scriptedQuotaAdmin{describes: []kadm.DescribedClientQuotas{
		describedQuota(entity, map[string]float64{"old": 1}),
		describedQuota(entity, map[string]float64{"old": 1}),
		describedQuota(entity, map[string]float64{"old": 1, "new": 2}),
		describedQuota(entity, map[string]float64{"new": 2}),
	}}
	var locks keyedLocker[quotaLockKey]

	require.NoError(t, upsertQuotaEntityLocked(
		context.Background(), &locks, "c1", admin, entity, map[string]float64{"new": 2},
	))
	require.Equal(t, 4, admin.describeCalls, "initial, stale, partial, then exact")
	require.Equal(t, 1, admin.alterCalls)
	require.Empty(t, locks.entries)
}

func TestWaitForExactQuotaStateHonorsCallerDeadline(t *testing.T) {
	user := "alice"
	entity := kadm.ClientQuotaEntity{{Type: "user", Name: &user}}
	admin := &scriptedQuotaAdmin{describes: []kadm.DescribedClientQuotas{
		describedQuota(entity, map[string]float64{"old": 1}),
	}}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()

	started := time.Now()
	err := waitForExactQuotaState(ctx, admin, entity, map[string]float64{"new": 2})
	require.Error(t, err)
	require.True(t, errors.Is(err, context.DeadlineExceeded))
	require.Less(t, time.Since(started), time.Second)
}

func TestUpsertQuotaEntityReleasesLockAfterVisibilityDeadline(t *testing.T) {
	user := "alice"
	entity := kadm.ClientQuotaEntity{{Type: "user", Name: &user}}
	admin := &scriptedQuotaAdmin{describes: []kadm.DescribedClientQuotas{
		describedQuota(entity, map[string]float64{"old": 1}),
	}}
	var locks keyedLocker[quotaLockKey]
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()

	err := upsertQuotaEntityLocked(ctx, &locks, "c1", admin, entity, map[string]float64{"new": 2})
	require.Error(t, err)
	require.True(t, errors.Is(err, context.DeadlineExceeded))
	require.Empty(t, locks.entries, "deadline must release and reclaim the keyed lock")
}

func TestUpsertQuotaEntityEmptyDesiredDeletesAndOnlyEmptyStateNoOps(t *testing.T) {
	user := "alice"
	entity := kadm.ClientQuotaEntity{{Type: "user", Name: &user}}
	admin := &statefulQuotaAdmin{entity: entity, values: map[string]float64{"producer_byte_rate": 1}}
	var locks keyedLocker[quotaLockKey]

	require.NoError(t, upsertQuotaEntityLocked(context.Background(), &locks, "c1", admin, entity, map[string]float64{}))
	require.Empty(t, admin.snapshot())
	require.Equal(t, 1, admin.alterCalls, "non-empty current state must be deleted")

	require.NoError(t, upsertQuotaEntityLocked(context.Background(), &locks, "c1", admin, entity, map[string]float64{}))
	require.Equal(t, 1, admin.alterCalls, "already-empty desired state is the only no-op")

	exactAdmin := &statefulQuotaAdmin{entity: entity, values: map[string]float64{"producer_byte_rate": 1}}
	require.NoError(t, upsertQuotaEntityLocked(context.Background(), &locks, "c1", exactAdmin, entity, map[string]float64{"producer_byte_rate": 1}))
	require.Equal(t, 1, exactAdmin.alterCalls, "a non-empty desired map is still sent even when already exact")
	require.Empty(t, locks.entries)
}

func TestQuotaEntityKeyIsOrderIndependentAndTyped(t *testing.T) {
	user, clientID, empty := "alice", "app", ""
	ordered := kadm.ClientQuotaEntity{{Type: "user", Name: &user}, {Type: "client-id", Name: &clientID}}
	reversed := kadm.ClientQuotaEntity{{Type: "client-id", Name: &clientID}, {Type: "user", Name: &user}}
	require.Equal(t, newQuotaEntityKey(ordered), newQuotaEntityKey(reversed))
	require.NotEqual(t,
		newQuotaEntityKey(kadm.ClientQuotaEntity{{Type: "user", Name: nil}}),
		newQuotaEntityKey(kadm.ClientQuotaEntity{{Type: "user", Name: &empty}}),
	)
}

// --- Pool.ListQuotas/UpsertQuotas error paths (no live broker needed; same
// pattern as acls_test.go for the Pool write/read methods that need a live
// kadm client to exercise their happy path -- the real success path is locked
// down by the integration soul test, per the task brief) ---

func TestListQuotasRejectsUnsupportedSecurityEagerly(t *testing.T) {
	p := NewPool()
	defer p.Close()
	qs, err := p.ListQuotas(context.Background(), unsupportedSecurityDef())
	require.ErrorIs(t, err, ErrUnsupportedSecurity)
	require.Nil(t, qs)
}

func TestListQuotasReturnsErrorWhenBrokerUnreachable(t *testing.T) {
	ctx, cancel, def := unreachableDef(t)
	defer cancel()
	p := NewPool()
	defer p.Close()
	qs, err := p.ListQuotas(ctx, def)
	require.ErrorContains(t, err, "describe client quotas")
	require.Nil(t, qs)
}

func TestUpsertQuotasRejectsUnsupportedSecurityEagerly(t *testing.T) {
	p := NewPool()
	defer p.Close()
	err := p.UpsertQuotas(context.Background(), unsupportedSecurityDef(), cluster.ClientQuota{User: "alice"})
	require.ErrorIs(t, err, ErrUnsupportedSecurity)
}

func TestUpsertQuotasReturnsErrorWhenBrokerUnreachable(t *testing.T) {
	ctx, cancel, def := unreachableDef(t)
	defer cancel()
	p := NewPool()
	defer p.Close()
	err := p.UpsertQuotas(ctx, def, cluster.ClientQuota{User: "alice", Quotas: map[string]float64{"producer_byte_rate": 1024}})
	require.ErrorContains(t, err, "describe client quotas")
}
