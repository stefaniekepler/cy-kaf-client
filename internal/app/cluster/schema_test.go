package cluster_test

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	appcluster "github.com/cy-kaf/cy-kaf-client/internal/app/cluster"
	"github.com/cy-kaf/cy-kaf-client/internal/domain/cluster"
)

// fakeSchemaPort implements cluster.SchemaRegistryPort for SchemaService
// tests — canned per-subject/per-version results plus atomic call counters,
// same call-recording shape as group_test.go's fakeGroupAdmin. Built to be
// reused across P2a Task 3 (reads), Task 4 (writes) and Task 5 (compat), so
// it carries fields for methods later tasks exercise even though Task 3 only
// drives Subjects/SchemaByVersion/Versions.
type fakeSchemaPort struct {
	calls atomic.Int32

	subjects    []string
	subjectsErr error

	byVersion      map[string]cluster.SchemaVersion // key: subjectVerKey(subject, version)
	byVersionErr   map[string]error
	byVersionCalls atomic.Int32
	byVersionArgs  []string

	byID    map[int]cluster.RawSchema
	byIDErr error

	versions      map[string][]int
	versionsErr   map[string]error
	versionsCalls atomic.Int32

	registerID   map[string]int
	registerErr  map[string]error
	lastRegister map[string]cluster.NewSchema

	deleteSubjectResult map[string][]int
	deleteSubjectErr    map[string]error
	deleteSubjectPerm   map[string]bool

	deleteVersionResult map[string]int
	deleteVersionErr    map[string]error

	globalCompat    string
	globalCompatErr error
	setGlobalErr    error
	lastSetGlobal   string

	subjectCompat    map[string]string
	subjectCompatErr map[string]error
	setSubjectErr    map[string]error
	lastSetSubject   map[string]string

	checkCompat    map[string]bool
	checkCompatErr map[string]error
	lastCheck      map[string]cluster.NewSchema
}

func newFakeSchemaPort() *fakeSchemaPort {
	return &fakeSchemaPort{
		byVersion: map[string]cluster.SchemaVersion{}, byVersionErr: map[string]error{},
		byID:     map[int]cluster.RawSchema{},
		versions: map[string][]int{}, versionsErr: map[string]error{},
		registerID: map[string]int{}, registerErr: map[string]error{}, lastRegister: map[string]cluster.NewSchema{},
		deleteSubjectResult: map[string][]int{}, deleteSubjectErr: map[string]error{}, deleteSubjectPerm: map[string]bool{},
		deleteVersionResult: map[string]int{}, deleteVersionErr: map[string]error{},
		subjectCompat: map[string]string{}, subjectCompatErr: map[string]error{}, setSubjectErr: map[string]error{}, lastSetSubject: map[string]string{},
		checkCompat: map[string]bool{}, checkCompatErr: map[string]error{}, lastCheck: map[string]cluster.NewSchema{},
	}
}

func subjectVerKey(subject, version string) string { return subject + "@" + version }

func (f *fakeSchemaPort) Subjects(context.Context, cluster.Definition) ([]string, error) {
	f.calls.Add(1)
	if f.subjectsErr != nil {
		return nil, f.subjectsErr
	}
	return f.subjects, nil
}

func (f *fakeSchemaPort) SchemaByVersion(_ context.Context, _ cluster.Definition, subject, version string) (cluster.SchemaVersion, error) {
	f.calls.Add(1)
	f.byVersionCalls.Add(1)
	f.byVersionArgs = append(f.byVersionArgs, version)
	k := subjectVerKey(subject, version)
	if err, ok := f.byVersionErr[k]; ok {
		return cluster.SchemaVersion{}, err
	}
	sv, ok := f.byVersion[k]
	if !ok {
		return cluster.SchemaVersion{}, fmt.Errorf("fakeSchemaPort: no schema for %s", k)
	}
	return sv, nil
}

func (f *fakeSchemaPort) SchemaByID(_ context.Context, _ cluster.Definition, id int) (cluster.RawSchema, error) {
	f.calls.Add(1)
	if f.byIDErr != nil {
		return cluster.RawSchema{}, f.byIDErr
	}
	rs, ok := f.byID[id]
	if !ok {
		return cluster.RawSchema{}, fmt.Errorf("fakeSchemaPort: no schema id %d", id)
	}
	return rs, nil
}

func (f *fakeSchemaPort) Versions(_ context.Context, _ cluster.Definition, subject string) ([]int, error) {
	f.calls.Add(1)
	f.versionsCalls.Add(1)
	if err, ok := f.versionsErr[subject]; ok {
		return nil, err
	}
	return f.versions[subject], nil
}

func (f *fakeSchemaPort) Register(_ context.Context, _ cluster.Definition, subject string, s cluster.NewSchema) (int, error) {
	f.calls.Add(1)
	f.lastRegister[subject] = s
	if err, ok := f.registerErr[subject]; ok {
		return 0, err
	}
	return f.registerID[subject], nil
}

func (f *fakeSchemaPort) DeleteSubject(_ context.Context, _ cluster.Definition, subject string, permanent bool) ([]int, error) {
	f.calls.Add(1)
	f.deleteSubjectPerm[subject] = permanent
	if err, ok := f.deleteSubjectErr[subject]; ok {
		return nil, err
	}
	return f.deleteSubjectResult[subject], nil
}

func (f *fakeSchemaPort) DeleteVersion(_ context.Context, _ cluster.Definition, subject, version string, _ bool) (int, error) {
	f.calls.Add(1)
	k := subjectVerKey(subject, version)
	if err, ok := f.deleteVersionErr[k]; ok {
		return 0, err
	}
	return f.deleteVersionResult[k], nil
}

func (f *fakeSchemaPort) GlobalCompat(context.Context, cluster.Definition) (string, error) {
	f.calls.Add(1)
	return f.globalCompat, f.globalCompatErr
}

func (f *fakeSchemaPort) SetGlobalCompat(_ context.Context, _ cluster.Definition, level string) error {
	f.calls.Add(1)
	f.lastSetGlobal = level
	return f.setGlobalErr
}

func (f *fakeSchemaPort) SubjectCompat(_ context.Context, _ cluster.Definition, subject string) (string, error) {
	f.calls.Add(1)
	if err, ok := f.subjectCompatErr[subject]; ok {
		return "", err
	}
	return f.subjectCompat[subject], nil
}

func (f *fakeSchemaPort) SetSubjectCompat(_ context.Context, _ cluster.Definition, subject, level string) error {
	f.calls.Add(1)
	f.lastSetSubject[subject] = level
	return f.setSubjectErr[subject]
}

func (f *fakeSchemaPort) CheckCompat(_ context.Context, _ cluster.Definition, subject string, s cluster.NewSchema) (bool, error) {
	f.calls.Add(1)
	f.lastCheck[subject] = s
	if err, ok := f.checkCompatErr[subject]; ok {
		return false, err
	}
	return f.checkCompat[subject], nil
}

var _ cluster.SchemaRegistryPort = (*fakeSchemaPort)(nil)

func newSchemaServiceForTest(def cluster.Definition, port cluster.SchemaRegistryPort) *appcluster.SchemaService {
	// Existing service tests exercise a configured registry unless they provide
	// an explicit non-empty (including whitespace-only) URL themselves.
	if def.SchemaRegistry.URL == "" {
		def.SchemaRegistry.URL = "https://schema.test"
	}
	res := appcluster.NewResolver([]cluster.Definition{def})
	return appcluster.NewSchemaService(res, port)
}

func TestSchemaServiceEmptyRegistryRejectsListWithoutCallingPort(t *testing.T) {
	port := newFakeSchemaPort()
	res := appcluster.NewResolver([]cluster.Definition{{Name: "prod"}})
	svc := appcluster.NewSchemaService(res, port)

	_, err := svc.ListSchemas(context.Background(), "prod", appcluster.SchemaListQuery{})
	require.ErrorIs(t, err, appcluster.ErrSchemaRegistryNotConfigured)
	require.Zero(t, port.calls.Load(), "empty registry URL must not call its port")
}

func TestSchemaServiceUnconfiguredRegistryRejectsEveryOperationWithoutCallingPort(t *testing.T) {
	port := newFakeSchemaPort()
	svc := newSchemaServiceForTest(cluster.Definition{
		Name:           "prod",
		SchemaRegistry: cluster.SchemaRegistrySpec{URL: " \t "},
	}, port)
	ctx := context.Background()
	newSchema := cluster.NewSchema{Schema: `{"type":"string"}`, SchemaType: "AVRO"}

	tests := []struct {
		name string
		run  func() error
	}{
		{name: "list", run: func() error {
			_, err := svc.ListSchemas(ctx, "prod", appcluster.SchemaListQuery{})
			return err
		}},
		{name: "latest", run: func() error {
			_, err := svc.LatestSchema(ctx, "prod", "subject")
			return err
		}},
		{name: "by version", run: func() error {
			_, err := svc.SchemaByVersion(ctx, "prod", "subject", "1")
			return err
		}},
		{name: "global compatibility", run: func() error {
			_, err := svc.GlobalCompat(ctx, "prod")
			return err
		}},
		{name: "set global compatibility", run: func() error {
			return svc.SetGlobalCompat(ctx, "prod", "BACKWARD")
		}},
		{name: "set subject compatibility", run: func() error {
			return svc.SetSubjectCompat(ctx, "prod", "subject", "BACKWARD")
		}},
		{name: "check compatibility", run: func() error {
			_, err := svc.CheckCompat(ctx, "prod", "subject", newSchema)
			return err
		}},
		{name: "register", run: func() error {
			_, err := svc.Register(ctx, "prod", "subject", newSchema)
			return err
		}},
		{name: "delete subject", run: func() error {
			_, err := svc.DeleteSubject(ctx, "prod", "subject")
			return err
		}},
		{name: "delete version", run: func() error {
			_, err := svc.DeleteVersion(ctx, "prod", "subject", "1")
			return err
		}},
		{name: "all versions", run: func() error {
			_, err := svc.AllVersions(ctx, "prod", "subject")
			return err
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.ErrorIs(t, tt.run(), appcluster.ErrSchemaRegistryNotConfigured)
			require.Zero(t, port.calls.Load(), "unconfigured registry must not call its port")
		})
	}
}

func TestSchemaServiceListSchemasPaginates(t *testing.T) {
	port := newFakeSchemaPort()
	port.subjects = []string{"s1", "s2", "s3", "s4", "s5", "s6"}
	for _, s := range port.subjects {
		port.byVersion[subjectVerKey(s, "latest")] = cluster.SchemaVersion{
			ID: 10, Subject: s, Version: 1, Schema: `{"type":"string"}`, SchemaType: "AVRO", CompatLevel: "BACKWARD",
		}
	}
	svc := newSchemaServiceForTest(cluster.Definition{Name: "prod"}, port)

	page, err := svc.ListSchemas(context.Background(), "prod", appcluster.SchemaListQuery{Page: 1, PerPage: 3})
	require.NoError(t, err)
	require.Len(t, page.Schemas, 3)
	require.Equal(t, 2, page.PageCount)
	require.Equal(t, []string{"s1", "s2", "s3"}, []string{page.Schemas[0].Subject, page.Schemas[1].Subject, page.Schemas[2].Subject})
	// Only the page slice's latest schema is fetched, not all six subjects'.
	require.Equal(t, int32(3), port.byVersionCalls.Load())
}

func TestSchemaServiceListSchemasSecondPage(t *testing.T) {
	port := newFakeSchemaPort()
	port.subjects = []string{"a", "b", "c", "d", "e"}
	for _, s := range port.subjects {
		port.byVersion[subjectVerKey(s, "latest")] = cluster.SchemaVersion{Subject: s, Version: 1, SchemaType: "AVRO", CompatLevel: "NONE"}
	}
	svc := newSchemaServiceForTest(cluster.Definition{Name: "prod"}, port)

	page, err := svc.ListSchemas(context.Background(), "prod", appcluster.SchemaListQuery{Page: 2, PerPage: 2})
	require.NoError(t, err)
	require.Len(t, page.Schemas, 2)
	require.Equal(t, 3, page.PageCount)
	require.Equal(t, []string{"c", "d"}, []string{page.Schemas[0].Subject, page.Schemas[1].Subject})
}

func TestSchemaServiceListSchemasSearchFilters(t *testing.T) {
	port := newFakeSchemaPort()
	port.subjects = []string{"orders-value", "orders-key", "payments-value"}
	for _, s := range port.subjects {
		port.byVersion[subjectVerKey(s, "latest")] = cluster.SchemaVersion{Subject: s, Version: 1, SchemaType: "AVRO", CompatLevel: "NONE"}
	}
	svc := newSchemaServiceForTest(cluster.Definition{Name: "prod"}, port)

	page, err := svc.ListSchemas(context.Background(), "prod", appcluster.SchemaListQuery{Search: "ORDERS"})
	require.NoError(t, err)
	require.Len(t, page.Schemas, 2)
	require.Equal(t, 1, page.PageCount)
	require.Equal(t, []string{"orders-key", "orders-value"}, []string{page.Schemas[0].Subject, page.Schemas[1].Subject})
}

func TestSchemaServiceListSchemasEmptyPageCountIsZero(t *testing.T) {
	port := newFakeSchemaPort()
	svc := newSchemaServiceForTest(cluster.Definition{Name: "prod"}, port)

	page, err := svc.ListSchemas(context.Background(), "prod", appcluster.SchemaListQuery{})
	require.NoError(t, err)
	require.Empty(t, page.Schemas)
	require.Equal(t, 0, page.PageCount)
}

func TestSchemaServiceListSchemasUnknownClusterIsErrUnknownCluster(t *testing.T) {
	svc := newSchemaServiceForTest(cluster.Definition{Name: "prod"}, newFakeSchemaPort())
	_, err := svc.ListSchemas(context.Background(), "nope", appcluster.SchemaListQuery{})
	require.ErrorIs(t, err, appcluster.ErrUnknownCluster)
}

func TestSchemaServiceLatestSchemaPassesThrough(t *testing.T) {
	port := newFakeSchemaPort()
	port.byVersion[subjectVerKey("orders-value", "latest")] = cluster.SchemaVersion{
		ID: 7, Subject: "orders-value", Version: 3, Schema: `{"type":"record"}`, SchemaType: "AVRO", CompatLevel: "FULL",
	}
	svc := newSchemaServiceForTest(cluster.Definition{Name: "prod"}, port)

	sv, err := svc.LatestSchema(context.Background(), "prod", "orders-value")
	require.NoError(t, err)
	require.Equal(t, 7, sv.ID)
	require.Equal(t, 3, sv.Version)
	require.Equal(t, "FULL", sv.CompatLevel)
}

func TestSchemaServiceLatestSchemaUnknownClusterIsErrUnknownCluster(t *testing.T) {
	svc := newSchemaServiceForTest(cluster.Definition{Name: "prod"}, newFakeSchemaPort())
	_, err := svc.LatestSchema(context.Background(), "nope", "s")
	require.ErrorIs(t, err, appcluster.ErrUnknownCluster)
}

func TestSchemaServiceSchemaByVersionPassesThrough(t *testing.T) {
	port := newFakeSchemaPort()
	port.byVersion[subjectVerKey("orders-value", "2")] = cluster.SchemaVersion{ID: 5, Subject: "orders-value", Version: 2, SchemaType: "AVRO", CompatLevel: "BACKWARD"}
	svc := newSchemaServiceForTest(cluster.Definition{Name: "prod"}, port)

	sv, err := svc.SchemaByVersion(context.Background(), "prod", "orders-value", "2")
	require.NoError(t, err)
	require.Equal(t, 2, sv.Version)
	require.Equal(t, 5, sv.ID)
}

// getAllVersionsBySubject's contract 200 is an array of full SchemaSubject
// objects (not bare version numbers), so AllVersions materializes each
// registered version via SchemaByVersion — driven here by asserting one
// SchemaVersion per port version number, in version order.
func TestSchemaServiceAllVersionsReturnsEachVersionsSchema(t *testing.T) {
	port := newFakeSchemaPort()
	port.versions["orders-value"] = []int{1, 2}
	port.byVersion[subjectVerKey("orders-value", "1")] = cluster.SchemaVersion{Subject: "orders-value", Version: 1, ID: 5, SchemaType: "AVRO", CompatLevel: "BACKWARD"}
	port.byVersion[subjectVerKey("orders-value", "2")] = cluster.SchemaVersion{Subject: "orders-value", Version: 2, ID: 6, SchemaType: "AVRO", CompatLevel: "BACKWARD"}
	svc := newSchemaServiceForTest(cluster.Definition{Name: "prod"}, port)

	got, err := svc.AllVersions(context.Background(), "prod", "orders-value")
	require.NoError(t, err)
	require.Len(t, got, 2)
	require.Equal(t, 1, got[0].Version)
	require.Equal(t, 5, got[0].ID)
	require.Equal(t, 2, got[1].Version)
	require.Equal(t, 6, got[1].ID)
}

func TestSchemaServiceAllVersionsEmptyIsEmpty(t *testing.T) {
	port := newFakeSchemaPort()
	port.versions["orders-value"] = nil
	svc := newSchemaServiceForTest(cluster.Definition{Name: "prod"}, port)

	got, err := svc.AllVersions(context.Background(), "prod", "orders-value")
	require.NoError(t, err)
	require.Empty(t, got)
}

func TestSchemaServiceAllVersionsUnknownClusterIsErrUnknownCluster(t *testing.T) {
	svc := newSchemaServiceForTest(cluster.Definition{Name: "prod"}, newFakeSchemaPort())
	_, err := svc.AllVersions(context.Background(), "nope", "s")
	require.ErrorIs(t, err, appcluster.ErrUnknownCluster)
}

// --- Task 4: writes ---

func TestSchemaServiceRegisterReadsBackLatest(t *testing.T) {
	port := newFakeSchemaPort()
	port.registerID["orders-value"] = 9
	want := cluster.SchemaVersion{
		ID: 9, Subject: "orders-value", Version: 2, SchemaType: "AVRO", CompatLevel: "BACKWARD",
	}
	port.byVersion[subjectVerKey("orders-value", "latest")] = want
	svc := newSchemaServiceForTest(cluster.Definition{Name: "prod"}, port)

	ns := cluster.NewSchema{Schema: `{"type":"string"}`, SchemaType: "AVRO"}
	sv, err := svc.Register(context.Background(), "prod", "orders-value", ns)
	require.NoError(t, err)
	require.Equal(t, want, sv)
	require.Equal(t, ns, port.lastRegister["orders-value"])
	require.Equal(t, int32(0), port.versionsCalls.Load())
	require.Equal(t, int32(1), port.byVersionCalls.Load())
	require.Equal(t, []string{"latest"}, port.byVersionArgs)
}

func TestSchemaServiceRegisterReturnsOlderIdempotentVersion(t *testing.T) {
	port := newFakeSchemaPort()
	port.registerID["orders-value"] = 1
	port.byVersion[subjectVerKey("orders-value", "latest")] = cluster.SchemaVersion{
		ID: 2, Subject: "orders-value", Version: 2, Schema: `{"type":"long"}`, SchemaType: "AVRO",
	}
	port.versions["orders-value"] = []int{2, 1}
	want := cluster.SchemaVersion{
		ID: 1, Subject: "orders-value", Version: 1, Schema: `{"type":"string"}`,
		SchemaType: "AVRO", CompatLevel: "BACKWARD",
	}
	port.byVersion[subjectVerKey("orders-value", "1")] = want
	svc := newSchemaServiceForTest(cluster.Definition{Name: "prod"}, port)

	sv, err := svc.Register(
		context.Background(),
		"prod",
		"orders-value",
		cluster.NewSchema{Schema: `{"type":"string"}`, SchemaType: "AVRO"},
	)
	require.NoError(t, err)
	require.Equal(t, want, sv)
	require.Equal(t, int32(1), port.versionsCalls.Load())
	require.Equal(t, []string{"latest", "1"}, port.byVersionArgs)
}

func TestSchemaServiceRegisterReturnsExactVersionAfterConcurrentLatest(t *testing.T) {
	port := newFakeSchemaPort()
	port.registerID["orders-value"] = 9
	port.byVersion[subjectVerKey("orders-value", "latest")] = cluster.SchemaVersion{
		ID: 10, Subject: "orders-value", Version: 3, Schema: `{"type":"boolean"}`, SchemaType: "AVRO",
	}
	port.versions["orders-value"] = []int{1, 3, 2}
	want := cluster.SchemaVersion{
		ID: 9, Subject: "orders-value", Version: 2, Schema: `{"type":"string"}`,
		SchemaType: "AVRO", CompatLevel: "FULL",
	}
	port.byVersion[subjectVerKey("orders-value", "2")] = want
	svc := newSchemaServiceForTest(cluster.Definition{Name: "prod"}, port)

	sv, err := svc.Register(
		context.Background(),
		"prod",
		"orders-value",
		cluster.NewSchema{Schema: `{"type":"string"}`, SchemaType: "AVRO"},
	)
	require.NoError(t, err)
	require.Equal(t, want, sv)
	require.Equal(t, []string{"latest", "2"}, port.byVersionArgs)
}

func TestSchemaServiceRegisterRejectsWhenRegisteredIDIsAbsent(t *testing.T) {
	port := newFakeSchemaPort()
	port.registerID["orders-value"] = 9
	port.byVersion[subjectVerKey("orders-value", "latest")] = cluster.SchemaVersion{
		ID: 10, Subject: "orders-value", Version: 3, SchemaType: "AVRO",
	}
	port.versions["orders-value"] = []int{0, -2, 3, 2, 2}
	port.byVersion[subjectVerKey("orders-value", "2")] = cluster.SchemaVersion{
		ID: 8, Subject: "orders-value", Version: 2, SchemaType: "AVRO",
	}
	svc := newSchemaServiceForTest(cluster.Definition{Name: "prod"}, port)

	sv, err := svc.Register(
		context.Background(),
		"prod",
		"orders-value",
		cluster.NewSchema{Schema: `{"type":"string"}`, SchemaType: "AVRO"},
	)
	require.ErrorIs(t, err, appcluster.ErrSchemaRegistrationChanged)
	require.Equal(t, cluster.SchemaVersion{}, sv)
	require.Equal(t, int32(1), port.versionsCalls.Load())
	require.Equal(t, []string{"latest", "2"}, port.byVersionArgs)
}

func TestSchemaServiceRegisterChoosesHighestMatchingVersionDeterministically(t *testing.T) {
	port := newFakeSchemaPort()
	port.registerID["orders-value"] = 42
	port.byVersion[subjectVerKey("orders-value", "latest")] = cluster.SchemaVersion{
		ID: 99, Subject: "orders-value", Version: 5, SchemaType: "AVRO",
	}
	versions := []int{2, 4, 1, 4, 0, -1, 5, 3}
	port.versions["orders-value"] = versions
	port.byVersion[subjectVerKey("orders-value", "4")] = cluster.SchemaVersion{
		ID: 42, Subject: "orders-value", Version: 4, Schema: `{"version":4}`, SchemaType: "AVRO",
	}
	port.byVersion[subjectVerKey("orders-value", "2")] = cluster.SchemaVersion{
		ID: 42, Subject: "orders-value", Version: 2, Schema: `{"version":2}`, SchemaType: "AVRO",
	}
	svc := newSchemaServiceForTest(cluster.Definition{Name: "prod"}, port)

	sv, err := svc.Register(
		context.Background(),
		"prod",
		"orders-value",
		cluster.NewSchema{Schema: `{"type":"string"}`, SchemaType: "AVRO"},
	)
	require.NoError(t, err)
	require.Equal(t, 4, sv.Version)
	require.Equal(t, `{"version":4}`, sv.Schema)
	require.Equal(t, []string{"latest", "4"}, port.byVersionArgs)
	require.Equal(t, []int{2, 4, 1, 4, 0, -1, 5, 3}, versions)
}

func TestSchemaServiceRegisterRejectsOversizedRawVersionsBeforeFallbackRead(t *testing.T) {
	port := newFakeSchemaPort()
	port.registerID["orders-value"] = 9
	port.byVersion[subjectVerKey("orders-value", "latest")] = cluster.SchemaVersion{
		ID: 10, Subject: "orders-value", Version: 2, SchemaType: "AVRO",
	}
	for index := 0; index < 501; index++ {
		if index%2 == 0 {
			port.versions["orders-value"] = append(port.versions["orders-value"], -1)
			continue
		}
		port.versions["orders-value"] = append(port.versions["orders-value"], 1)
	}
	port.byVersion[subjectVerKey("orders-value", "1")] = cluster.SchemaVersion{
		ID: 9, Subject: "orders-value", Version: 1, SchemaType: "AVRO",
	}
	svc := newSchemaServiceForTest(cluster.Definition{Name: "prod"}, port)

	sv, err := svc.Register(
		context.Background(),
		"prod",
		"orders-value",
		cluster.NewSchema{Schema: `{"type":"string"}`, SchemaType: "AVRO"},
	)
	require.ErrorIs(t, err, appcluster.ErrSchemaRegistrationChanged)
	require.Equal(t, cluster.SchemaVersion{}, sv)
	require.Equal(t, int32(1), port.versionsCalls.Load())
	require.Equal(t, int32(1), port.byVersionCalls.Load())
	require.Equal(t, []string{"latest"}, port.byVersionArgs)
}

func TestSchemaServiceRegisterAcceptsRawVersionBoundaryWithinCallBudget(t *testing.T) {
	port := newFakeSchemaPort()
	port.registerID["orders-value"] = 9
	port.byVersion[subjectVerKey("orders-value", "latest")] = cluster.SchemaVersion{
		ID: 10, Subject: "orders-value", Version: 500, SchemaType: "AVRO",
	}
	for version := 1; version <= 500; version++ {
		port.versions["orders-value"] = append(port.versions["orders-value"], version)
		port.byVersion[subjectVerKey("orders-value", fmt.Sprint(version))] = cluster.SchemaVersion{
			ID: 10, Subject: "orders-value", Version: version, SchemaType: "AVRO",
		}
	}
	want := cluster.SchemaVersion{
		ID: 9, Subject: "orders-value", Version: 1, Schema: `{"version":1}`, SchemaType: "AVRO",
	}
	port.byVersion[subjectVerKey("orders-value", "1")] = want
	svc := newSchemaServiceForTest(cluster.Definition{Name: "prod"}, port)

	sv, err := svc.Register(
		context.Background(),
		"prod",
		"orders-value",
		cluster.NewSchema{Schema: `{"type":"string"}`, SchemaType: "AVRO"},
	)
	require.NoError(t, err)
	require.Equal(t, want, sv)
	require.Equal(t, int32(500), port.byVersionCalls.Load())
	require.Equal(t, "latest", port.byVersionArgs[0])
	require.Equal(t, "1", port.byVersionArgs[len(port.byVersionArgs)-1])
}

func TestSchemaServiceRegisterCapsTotalMaterializationCallsAt500(t *testing.T) {
	port := newFakeSchemaPort()
	port.registerID["orders-value"] = 9
	port.byVersion[subjectVerKey("orders-value", "latest")] = cluster.SchemaVersion{
		ID: 10, Subject: "orders-value", Version: 501, SchemaType: "AVRO",
	}
	for version := 1; version <= 500; version++ {
		port.versions["orders-value"] = append(port.versions["orders-value"], version)
		port.byVersion[subjectVerKey("orders-value", fmt.Sprint(version))] = cluster.SchemaVersion{
			ID: 10, Subject: "orders-value", Version: version, SchemaType: "AVRO",
		}
	}
	svc := newSchemaServiceForTest(cluster.Definition{Name: "prod"}, port)

	sv, err := svc.Register(
		context.Background(),
		"prod",
		"orders-value",
		cluster.NewSchema{Schema: `{"type":"string"}`, SchemaType: "AVRO"},
	)
	require.ErrorIs(t, err, appcluster.ErrSchemaRegistrationChanged)
	require.Equal(t, cluster.SchemaVersion{}, sv)
	require.Equal(t, int32(1), port.versionsCalls.Load())
	require.Equal(t, int32(500), port.byVersionCalls.Load())
	require.Equal(t, "2", port.byVersionArgs[len(port.byVersionArgs)-1])
}

func TestSchemaServiceRegisterRejectsMatchBeyondConcreteCallBudget(t *testing.T) {
	port := newFakeSchemaPort()
	port.registerID["orders-value"] = 9
	port.byVersion[subjectVerKey("orders-value", "latest")] = cluster.SchemaVersion{
		ID: 10, Subject: "orders-value", Version: 501, SchemaType: "AVRO",
	}
	for version := 1; version <= 500; version++ {
		port.versions["orders-value"] = append(port.versions["orders-value"], version)
		port.byVersion[subjectVerKey("orders-value", fmt.Sprint(version))] = cluster.SchemaVersion{
			ID: 10, Subject: "orders-value", Version: version, SchemaType: "AVRO",
		}
	}
	port.byVersion[subjectVerKey("orders-value", "1")] = cluster.SchemaVersion{
		ID: 9, Subject: "orders-value", Version: 1, SchemaType: "AVRO",
	}
	svc := newSchemaServiceForTest(cluster.Definition{Name: "prod"}, port)

	sv, err := svc.Register(
		context.Background(),
		"prod",
		"orders-value",
		cluster.NewSchema{Schema: `{"type":"string"}`, SchemaType: "AVRO"},
	)
	require.ErrorIs(t, err, appcluster.ErrSchemaRegistrationChanged)
	require.Equal(t, cluster.SchemaVersion{}, sv)
	require.Equal(t, int32(500), port.byVersionCalls.Load())
	require.Equal(t, "2", port.byVersionArgs[len(port.byVersionArgs)-1])
}

func TestSchemaServiceRegisterPropagatesError(t *testing.T) {
	port := newFakeSchemaPort()
	port.registerErr["orders-value"] = errBoom
	svc := newSchemaServiceForTest(cluster.Definition{Name: "prod"}, port)
	_, err := svc.Register(context.Background(), "prod", "orders-value", cluster.NewSchema{SchemaType: "AVRO"})
	require.ErrorIs(t, err, errBoom)
}

func TestSchemaServiceRegisterUnknownClusterIsErrUnknownCluster(t *testing.T) {
	svc := newSchemaServiceForTest(cluster.Definition{Name: "prod"}, newFakeSchemaPort())
	_, err := svc.Register(context.Background(), "nope", "s", cluster.NewSchema{SchemaType: "AVRO"})
	require.ErrorIs(t, err, appcluster.ErrUnknownCluster)
}

func TestSchemaServiceDeleteSubjectSoftDeletes(t *testing.T) {
	port := newFakeSchemaPort()
	port.deleteSubjectResult["orders-value"] = []int{1, 2}
	svc := newSchemaServiceForTest(cluster.Definition{Name: "prod"}, port)

	deleted, err := svc.DeleteSubject(context.Background(), "prod", "orders-value")
	require.NoError(t, err)
	require.Equal(t, []int{1, 2}, deleted)
	require.False(t, port.deleteSubjectPerm["orders-value"]) // the UI delete endpoints are always soft deletes
}

func TestSchemaServiceDeleteSubjectUnknownClusterIsErrUnknownCluster(t *testing.T) {
	svc := newSchemaServiceForTest(cluster.Definition{Name: "prod"}, newFakeSchemaPort())
	_, err := svc.DeleteSubject(context.Background(), "nope", "s")
	require.ErrorIs(t, err, appcluster.ErrUnknownCluster)
}

func TestSchemaServiceDeleteVersion(t *testing.T) {
	port := newFakeSchemaPort()
	port.deleteVersionResult[subjectVerKey("orders-value", "1")] = 1
	svc := newSchemaServiceForTest(cluster.Definition{Name: "prod"}, port)

	deleted, err := svc.DeleteVersion(context.Background(), "prod", "orders-value", "1")
	require.NoError(t, err)
	require.Equal(t, 1, deleted)
}

func TestSchemaServiceDeleteVersionLatestForwardsSentinel(t *testing.T) {
	port := newFakeSchemaPort()
	port.deleteVersionResult[subjectVerKey("orders-value", "latest")] = 3
	svc := newSchemaServiceForTest(cluster.Definition{Name: "prod"}, port)

	deleted, err := svc.DeleteVersion(context.Background(), "prod", "orders-value", "latest")
	require.NoError(t, err)
	require.Equal(t, 3, deleted) // "latest" is forwarded to the port, which resolves it to a concrete number
}

func TestSchemaServiceDeleteVersionUnknownClusterIsErrUnknownCluster(t *testing.T) {
	svc := newSchemaServiceForTest(cluster.Definition{Name: "prod"}, newFakeSchemaPort())
	_, err := svc.DeleteVersion(context.Background(), "nope", "s", "1")
	require.ErrorIs(t, err, appcluster.ErrUnknownCluster)
}

// --- Task 5: compatibility ---

func TestSchemaServiceGlobalCompatPassesThrough(t *testing.T) {
	port := newFakeSchemaPort()
	port.globalCompat = "FULL"
	svc := newSchemaServiceForTest(cluster.Definition{Name: "prod"}, port)

	got, err := svc.GlobalCompat(context.Background(), "prod")
	require.NoError(t, err)
	require.Equal(t, "FULL", got)
}

func TestSchemaServiceGlobalCompatUnknownClusterIsErrUnknownCluster(t *testing.T) {
	svc := newSchemaServiceForTest(cluster.Definition{Name: "prod"}, newFakeSchemaPort())
	_, err := svc.GlobalCompat(context.Background(), "nope")
	require.ErrorIs(t, err, appcluster.ErrUnknownCluster)
}

func TestSchemaServiceSetGlobalCompat(t *testing.T) {
	port := newFakeSchemaPort()
	svc := newSchemaServiceForTest(cluster.Definition{Name: "prod"}, port)

	require.NoError(t, svc.SetGlobalCompat(context.Background(), "prod", "BACKWARD"))
	require.Equal(t, "BACKWARD", port.lastSetGlobal)
}

func TestSchemaServiceSetGlobalCompatUnknownClusterIsErrUnknownCluster(t *testing.T) {
	svc := newSchemaServiceForTest(cluster.Definition{Name: "prod"}, newFakeSchemaPort())
	err := svc.SetGlobalCompat(context.Background(), "nope", "BACKWARD")
	require.ErrorIs(t, err, appcluster.ErrUnknownCluster)
}

func TestSchemaServiceSetSubjectCompat(t *testing.T) {
	port := newFakeSchemaPort()
	svc := newSchemaServiceForTest(cluster.Definition{Name: "prod"}, port)

	require.NoError(t, svc.SetSubjectCompat(context.Background(), "prod", "orders-value", "FORWARD"))
	require.Equal(t, "FORWARD", port.lastSetSubject["orders-value"])
}

func TestSchemaServiceSetSubjectCompatUnknownClusterIsErrUnknownCluster(t *testing.T) {
	svc := newSchemaServiceForTest(cluster.Definition{Name: "prod"}, newFakeSchemaPort())
	err := svc.SetSubjectCompat(context.Background(), "nope", "s", "FORWARD")
	require.ErrorIs(t, err, appcluster.ErrUnknownCluster)
}

func TestSchemaServiceCheckCompatPassesThrough(t *testing.T) {
	port := newFakeSchemaPort()
	port.checkCompat["orders-value"] = true
	svc := newSchemaServiceForTest(cluster.Definition{Name: "prod"}, port)

	ns := cluster.NewSchema{Schema: `{"type":"string"}`, SchemaType: "AVRO"}
	ok, err := svc.CheckCompat(context.Background(), "prod", "orders-value", ns)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, ns, port.lastCheck["orders-value"])
}

func TestSchemaServiceCheckCompatIncompatible(t *testing.T) {
	port := newFakeSchemaPort()
	port.checkCompat["orders-value"] = false
	svc := newSchemaServiceForTest(cluster.Definition{Name: "prod"}, port)

	ok, err := svc.CheckCompat(context.Background(), "prod", "orders-value", cluster.NewSchema{SchemaType: "AVRO"})
	require.NoError(t, err)
	require.False(t, ok)
}

func TestSchemaServiceCheckCompatUnknownClusterIsErrUnknownCluster(t *testing.T) {
	svc := newSchemaServiceForTest(cluster.Definition{Name: "prod"}, newFakeSchemaPort())
	_, err := svc.CheckCompat(context.Background(), "nope", "s", cluster.NewSchema{SchemaType: "AVRO"})
	require.ErrorIs(t, err, appcluster.ErrUnknownCluster)
}
