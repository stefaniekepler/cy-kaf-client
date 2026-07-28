package cluster

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	domain "github.com/cy-kaf/cy-kaf-client/internal/domain/cluster"
)

type fakeKsqlPort struct {
	mu               sync.Mutex
	listStreams      func(context.Context, domain.Definition) ([]domain.KsqlStreamDescription, error)
	listTables       func(context.Context, domain.Definition) ([]domain.KsqlTableDescription, error)
	execute          func(context.Context, domain.Definition, domain.KsqlCommand, func(domain.KsqlTable) error) error
	listStreamsCalls int
	listTablesCalls  int
	executeCalls     int
	lastDefinition   domain.Definition
	lastCommand      domain.KsqlCommand
}

func (f *fakeKsqlPort) ListStreams(ctx context.Context, def domain.Definition) ([]domain.KsqlStreamDescription, error) {
	f.mu.Lock()
	f.listStreamsCalls++
	f.mu.Unlock()
	if f.listStreams != nil {
		return f.listStreams(ctx, def)
	}
	return nil, nil
}

func (f *fakeKsqlPort) ListTables(ctx context.Context, def domain.Definition) ([]domain.KsqlTableDescription, error) {
	f.mu.Lock()
	f.listTablesCalls++
	f.mu.Unlock()
	if f.listTables != nil {
		return f.listTables(ctx, def)
	}
	return nil, nil
}

func (f *fakeKsqlPort) Execute(ctx context.Context, def domain.Definition, cmd domain.KsqlCommand, emit func(domain.KsqlTable) error) error {
	f.mu.Lock()
	f.executeCalls++
	f.lastDefinition = def
	f.lastCommand = cloneKsqlCommand(cmd)
	f.mu.Unlock()
	if f.execute != nil {
		return f.execute(ctx, def, cmd, emit)
	}
	return nil
}

func (f *fakeKsqlPort) calls() (int, domain.Definition, domain.KsqlCommand) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.executeCalls, f.lastDefinition, cloneKsqlCommand(f.lastCommand)
}

func (f *fakeKsqlPort) totalCalls() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.listStreamsCalls + f.listTablesCalls + f.executeCalls
}

type fakeClassifier struct {
	kind  domain.KsqlStatementKind
	err   error
	calls *atomic.Int32
}

func (f fakeClassifier) Classify(string) (domain.KsqlStatementKind, error) {
	if f.calls != nil {
		f.calls.Add(1)
	}
	return f.kind, f.err
}

func testDefinition(name string) domain.Definition {
	return domain.Definition{Name: name, KsqlURL: "http://" + name + ":8088"}
}

func testService(port domain.KsqlPort, classifier KsqlClassifier) *KsqlService {
	return NewKsqlService(NewResolver([]domain.Definition{
		testDefinition("local"),
		testDefinition("other"),
	}), port, classifier)
}

func TestKsqlPipeIsOneShot(t *testing.T) {
	calls := 0
	port := &fakeKsqlPort{execute: func(ctx context.Context, def domain.Definition,
		cmd domain.KsqlCommand, emit func(domain.KsqlTable) error) error {
		calls++
		return emit(domain.KsqlTable{Header: "Query Result"})
	}}
	svc := NewKsqlService(NewResolver([]domain.Definition{testDefinition("local")}), port,
		fakeClassifier{kind: domain.KsqlQuery})
	pipeID, err := svc.Register(context.Background(), "local", "SELECT 1;", nil)
	if err != nil {
		t.Fatalf("Register() error = %v", err)
	}
	if err := svc.Open(context.Background(), "local", pipeID, func(domain.KsqlTable) error { return nil }); err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if calls != 1 {
		t.Fatalf("Execute calls = %d, want 1", calls)
	}
	if err := svc.Open(context.Background(), "local", pipeID, func(domain.KsqlTable) error { return nil }); !errors.Is(err, ErrKsqlPipeNotFound) {
		t.Fatalf("second Open() error = %v, want ErrKsqlPipeNotFound", err)
	}
}

func TestKsqlOpenAuthorizedConsumesDeniedStatementWithoutCallingPort(t *testing.T) {
	denied := errors.New("writes disabled")
	port := &fakeKsqlPort{}
	svc := testService(port, fakeClassifier{kind: domain.KsqlStatement})
	pipeID, err := svc.Register(context.Background(), "local", "CREATE STREAM orders;", nil)
	if err != nil {
		t.Fatalf("Register() error = %v", err)
	}

	var authorizedKind domain.KsqlStatementKind
	err = svc.OpenAuthorized(
		context.Background(),
		"local",
		pipeID,
		func(kind domain.KsqlStatementKind) error {
			authorizedKind = kind
			return denied
		},
		func(domain.KsqlTable) error { return nil },
	)
	if !errors.Is(err, denied) {
		t.Fatalf("OpenAuthorized() error = %v, want denied sentinel", err)
	}
	if authorizedKind != domain.KsqlStatement {
		t.Fatalf("authorized kind = %q, want %q", authorizedKind, domain.KsqlStatement)
	}
	if calls := port.totalCalls(); calls != 0 {
		t.Fatalf("denied OpenAuthorized() reached port %d times", calls)
	}

	err = svc.OpenAuthorized(
		context.Background(),
		"local",
		pipeID,
		nil,
		func(domain.KsqlTable) error { return nil },
	)
	if !errors.Is(err, ErrKsqlPipeNotFound) {
		t.Fatalf("second OpenAuthorized() error = %v, want ErrKsqlPipeNotFound", err)
	}
}

func TestKsqlOpenAuthorizedAuthorizesQueryBeforePortExecution(t *testing.T) {
	order := 0
	port := &fakeKsqlPort{
		execute: func(
			_ context.Context,
			_ domain.Definition,
			command domain.KsqlCommand,
			_ func(domain.KsqlTable) error,
		) error {
			if order != 1 {
				return fmt.Errorf("execute order = %d, want authorization first", order)
			}
			if command.Kind != domain.KsqlQuery {
				return fmt.Errorf("command kind = %q, want %q", command.Kind, domain.KsqlQuery)
			}
			order = 2
			return nil
		},
	}
	svc := testService(port, fakeClassifier{kind: domain.KsqlQuery})
	pipeID, err := svc.Register(context.Background(), "local", "SELECT * FROM orders;", nil)
	if err != nil {
		t.Fatalf("Register() error = %v", err)
	}

	err = svc.OpenAuthorized(
		context.Background(),
		"local",
		pipeID,
		func(kind domain.KsqlStatementKind) error {
			if kind != domain.KsqlQuery {
				return fmt.Errorf("authorized kind = %q, want %q", kind, domain.KsqlQuery)
			}
			if order != 0 {
				return fmt.Errorf("authorization order = %d, want 0", order)
			}
			order = 1
			return nil
		},
		func(domain.KsqlTable) error { return nil },
	)
	if err != nil {
		t.Fatalf("OpenAuthorized() error = %v", err)
	}
	if order != 2 {
		t.Fatalf("final order = %d, want 2", order)
	}
}

func TestKsqlRegisterDoesNotExecute(t *testing.T) {
	port := &fakeKsqlPort{}
	svc := testService(port, fakeClassifier{kind: domain.KsqlStatement})
	if _, err := svc.Register(context.Background(), "local", "CREATE STREAM S;", map[string]string{"auto.offset.reset": "earliest"}); err != nil {
		t.Fatalf("Register() error = %v", err)
	}
	calls, _, _ := port.calls()
	if calls != 0 {
		t.Fatalf("Register() executed downstream %d times", calls)
	}
}

func TestKsqlRegisterCopiesStreamsProperties(t *testing.T) {
	port := &fakeKsqlPort{execute: func(context.Context, domain.Definition, domain.KsqlCommand, func(domain.KsqlTable) error) error {
		return nil
	}}
	svc := testService(port, fakeClassifier{kind: domain.KsqlQuery})
	properties := map[string]string{"auto.offset.reset": "earliest"}
	pipeID, err := svc.Register(context.Background(), "local", "SELECT 1", properties)
	if err != nil {
		t.Fatalf("Register() error = %v", err)
	}
	properties["auto.offset.reset"] = "latest"
	properties["new"] = "mutated"
	if err := svc.Open(context.Background(), "local", pipeID, func(domain.KsqlTable) error { return nil }); err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	_, _, command := port.calls()
	if command.StreamsProperties["auto.offset.reset"] != "earliest" {
		t.Fatalf("copied property = %q, want earliest", command.StreamsProperties["auto.offset.reset"])
	}
	if _, ok := command.StreamsProperties["new"]; ok {
		t.Fatal("mutating caller properties changed registered command")
	}
}

func TestKsqlPipeClusterMismatchIsNotFoundAndDoesNotConsume(t *testing.T) {
	port := &fakeKsqlPort{execute: func(context.Context, domain.Definition, domain.KsqlCommand, func(domain.KsqlTable) error) error {
		return nil
	}}
	svc := testService(port, fakeClassifier{kind: domain.KsqlQuery})
	pipeID, err := svc.Register(context.Background(), "local", "SELECT 1", nil)
	if err != nil {
		t.Fatalf("Register() error = %v", err)
	}
	if err := svc.Open(context.Background(), "other", pipeID, func(domain.KsqlTable) error { return nil }); !errors.Is(err, ErrKsqlPipeNotFound) {
		t.Fatalf("mismatched Open() error = %v, want ErrKsqlPipeNotFound", err)
	}
	if err := svc.Open(context.Background(), "local", pipeID, func(domain.KsqlTable) error { return nil }); err != nil {
		t.Fatalf("matching Open() after mismatch error = %v", err)
	}
	calls, _, _ := port.calls()
	if calls != 1 {
		t.Fatalf("Execute calls = %d, want 1", calls)
	}
}

func TestKsqlPipeExpiresAtTTL(t *testing.T) {
	now := time.Unix(100, 0)
	registry := newPipeRegistry(func() time.Time { return now }, time.Minute, 1024)
	command := domain.KsqlCommand{SQL: "SELECT 1", Kind: domain.KsqlQuery}
	pipeID, err := registry.register("local", command)
	if err != nil {
		t.Fatalf("register() error = %v", err)
	}
	now = now.Add(time.Minute)
	if _, ok := registry.claim("local", pipeID); ok {
		t.Fatal("expired pipe was claimable at TTL boundary")
	}
	if len(registry.entries) != 0 {
		t.Fatalf("expired entries = %d, want 0", len(registry.entries))
	}
}

func TestKsqlPipeCapacityAllows1024Rejects1025(t *testing.T) {
	now := time.Unix(200, 0)
	registry := newPipeRegistry(func() time.Time { return now }, time.Minute, 1024)
	command := domain.KsqlCommand{SQL: "SELECT 1", Kind: domain.KsqlQuery}
	for i := 0; i < 1024; i++ {
		if _, err := registry.register("local", command); err != nil {
			t.Fatalf("register #%d error = %v", i+1, err)
		}
	}
	if len(registry.entries) != 1024 {
		t.Fatalf("entries after 1024 registrations = %d", len(registry.entries))
	}
	if _, err := registry.register("local", command); err == nil {
		t.Fatal("1025th registration succeeded")
	}
}

func TestKsqlUnknownClusterReturnsNotFound(t *testing.T) {
	port := &fakeKsqlPort{}
	svc := testService(port, fakeClassifier{kind: domain.KsqlQuery})
	if _, err := svc.Register(context.Background(), "missing", "SELECT 1", nil); !errors.Is(err, ErrKsqlClusterNotFound) {
		t.Fatalf("Register() error = %v, want ErrKsqlClusterNotFound", err)
	}
	if err := svc.Open(context.Background(), "missing", "pipe", func(domain.KsqlTable) error { return nil }); !errors.Is(err, ErrKsqlClusterNotFound) {
		t.Fatalf("Open() error = %v, want ErrKsqlClusterNotFound", err)
	}
	if _, err := svc.ListStreams(context.Background(), "missing"); !errors.Is(err, ErrKsqlClusterNotFound) {
		t.Fatalf("ListStreams() error = %v, want ErrKsqlClusterNotFound", err)
	}
	if _, err := svc.ListTables(context.Background(), "missing"); !errors.Is(err, ErrKsqlClusterNotFound) {
		t.Fatalf("ListTables() error = %v, want ErrKsqlClusterNotFound", err)
	}
	calls, _, _ := port.calls()
	if calls != 0 {
		t.Fatalf("unknown cluster reached port %d times", calls)
	}
}

func TestKsqlNotConfiguredRejectsEveryOperationWithoutSideEffects(t *testing.T) {
	tests := []struct {
		name string
		run  func(*KsqlService) error
	}{
		{name: "register", run: func(svc *KsqlService) error {
			_, err := svc.Register(context.Background(), "local", "SELECT 1", nil)
			return err
		}},
		{name: "open", run: func(svc *KsqlService) error {
			return svc.Open(context.Background(), "local", "pipe", func(domain.KsqlTable) error { return nil })
		}},
		{name: "list streams", run: func(svc *KsqlService) error {
			_, err := svc.ListStreams(context.Background(), "local")
			return err
		}},
		{name: "list tables", run: func(svc *KsqlService) error {
			_, err := svc.ListTables(context.Background(), "local")
			return err
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			port := &fakeKsqlPort{}
			var classifierCalls atomic.Int32
			svc := NewKsqlService(
				NewResolver([]domain.Definition{{Name: "local", KsqlURL: " \t "}}),
				port,
				fakeClassifier{kind: domain.KsqlQuery, calls: &classifierCalls},
			)

			if err := tt.run(svc); !errors.Is(err, ErrKsqlNotConfigured) {
				t.Fatalf("%s error = %v, want ErrKsqlNotConfigured", tt.name, err)
			}
			if calls := classifierCalls.Load(); calls != 0 {
				t.Fatalf("%s classifier calls = %d, want 0", tt.name, calls)
			}
			if entries := len(svc.pipes.entries); entries != 0 {
				t.Fatalf("%s pipe entries = %d, want 0", tt.name, entries)
			}
			if calls := port.totalCalls(); calls != 0 {
				t.Fatalf("%s port calls = %d, want 0", tt.name, calls)
			}
		})
	}
}

func TestKsqlEmptyURLRejectsListWithoutCallingPort(t *testing.T) {
	port := &fakeKsqlPort{}
	svc := NewKsqlService(
		NewResolver([]domain.Definition{{Name: "local"}}),
		port,
		fakeClassifier{kind: domain.KsqlQuery},
	)

	if _, err := svc.ListStreams(context.Background(), "local"); !errors.Is(err, ErrKsqlNotConfigured) {
		t.Fatalf("ListStreams() error = %v, want ErrKsqlNotConfigured", err)
	}
	if calls := port.totalCalls(); calls != 0 {
		t.Fatalf("port calls = %d, want 0", calls)
	}
}

func TestKsqlRemovedConfigurationDoesNotConsumeRegisteredPipe(t *testing.T) {
	resolver := NewResolver([]domain.Definition{testDefinition("local")})
	port := &fakeKsqlPort{}
	svc := NewKsqlService(resolver, port, fakeClassifier{kind: domain.KsqlQuery})
	pipeID, err := svc.Register(context.Background(), "local", "SELECT 1", nil)
	if err != nil {
		t.Fatalf("Register() error = %v", err)
	}

	resolver.Replace([]domain.Definition{{Name: "local"}})
	if err := svc.Open(context.Background(), "local", pipeID, func(domain.KsqlTable) error { return nil }); !errors.Is(err, ErrKsqlNotConfigured) {
		t.Fatalf("Open() after removal error = %v, want ErrKsqlNotConfigured", err)
	}
	if calls := port.totalCalls(); calls != 0 {
		t.Fatalf("port calls after removal = %d, want 0", calls)
	}
	if entries := len(svc.pipes.entries); entries != 1 {
		t.Fatalf("pipe entries after removal = %d, want 1", entries)
	}

	resolver.Replace([]domain.Definition{testDefinition("local")})
	if err := svc.Open(context.Background(), "local", pipeID, func(domain.KsqlTable) error { return nil }); err != nil {
		t.Fatalf("Open() after restore error = %v", err)
	}
	if calls := port.totalCalls(); calls != 1 {
		t.Fatalf("port calls after restore = %d, want 1", calls)
	}
}

func TestKsqlClassifierErrorDoesNotCreatePipe(t *testing.T) {
	port := &fakeKsqlPort{}
	svc := testService(port, fakeClassifier{err: errors.New("syntax has diagnostic-marker")})
	if _, err := svc.Register(context.Background(), "local", "SELECT diagnostic-marker", nil); !errors.Is(err, ErrKsqlInvalidCommand) {
		t.Fatalf("Register() error = %v, want ErrKsqlInvalidCommand", err)
	} else if strings.Contains(err.Error(), "diagnostic-marker") {
		t.Fatalf("classifier error leaked SQL/diagnostic: %v", err)
	}
	if got := len(svc.pipes.entries); got != 0 {
		t.Fatalf("entries after classifier failure = %d, want 0", got)
	}
}

func TestKsqlInvalidClassifierKindDoesNotCreatePipe(t *testing.T) {
	svc := testService(&fakeKsqlPort{}, fakeClassifier{kind: domain.KsqlStatementKind("unexpected")})
	if _, err := svc.Register(context.Background(), "local", "SELECT 1", nil); !errors.Is(err, ErrKsqlInvalidCommand) {
		t.Fatalf("Register() error = %v, want ErrKsqlInvalidCommand", err)
	}
	if got := len(svc.pipes.entries); got != 0 {
		t.Fatalf("entries after invalid kind = %d, want 0", got)
	}
}

func TestKsqlCancellationReachesPort(t *testing.T) {
	started := make(chan struct{})
	port := &fakeKsqlPort{execute: func(ctx context.Context, _ domain.Definition, _ domain.KsqlCommand, _ func(domain.KsqlTable) error) error {
		close(started)
		<-ctx.Done()
		return ctx.Err()
	}}
	svc := testService(port, fakeClassifier{kind: domain.KsqlQuery})
	pipeID, err := svc.Register(context.Background(), "local", "SELECT 1", nil)
	if err != nil {
		t.Fatalf("Register() error = %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() {
		result <- svc.Open(ctx, "local", pipeID, func(domain.KsqlTable) error { return nil })
	}()
	<-started
	cancel()
	if err := <-result; !errors.Is(err, context.Canceled) {
		t.Fatalf("Open() error = %v, want context.Canceled", err)
	}
}

func TestKsqlListsResolveAndDelegate(t *testing.T) {
	wantStream := domain.KsqlStreamDescription{}
	wantTable := domain.KsqlTableDescription{}
	port := &fakeKsqlPort{
		listStreams: func(_ context.Context, def domain.Definition) ([]domain.KsqlStreamDescription, error) {
			if def.Name != "local" {
				return nil, fmt.Errorf("definition = %s", def.Name)
			}
			return []domain.KsqlStreamDescription{wantStream}, nil
		},
		listTables: func(_ context.Context, def domain.Definition) ([]domain.KsqlTableDescription, error) {
			if def.Name != "local" {
				return nil, fmt.Errorf("definition = %s", def.Name)
			}
			return []domain.KsqlTableDescription{wantTable}, nil
		},
	}
	svc := testService(port, fakeClassifier{kind: domain.KsqlQuery})
	streams, err := svc.ListStreams(context.Background(), "local")
	if err != nil || len(streams) != 1 {
		t.Fatalf("ListStreams() = %#v, %v", streams, err)
	}
	tables, err := svc.ListTables(context.Background(), "local")
	if err != nil || len(tables) != 1 {
		t.Fatalf("ListTables() = %#v, %v", tables, err)
	}
}

func TestKsqlPipeClaimsAtomically(t *testing.T) {
	registry := newPipeRegistry(time.Now, time.Minute, 1024)
	pipeID, err := registry.register("local", domain.KsqlCommand{SQL: "SELECT 1", Kind: domain.KsqlQuery})
	if err != nil {
		t.Fatalf("register() error = %v", err)
	}
	const workers = 16
	var wg sync.WaitGroup
	claims := make(chan bool, workers)
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, ok := registry.claim("local", pipeID)
			claims <- ok
		}()
	}
	wg.Wait()
	close(claims)
	count := 0
	for ok := range claims {
		if ok {
			count++
		}
	}
	if count != 1 {
		t.Fatalf("successful claims = %d, want 1", count)
	}
}
