MODULE      := github.com/cy-kaf/cy-kaf-client
BIN         := cy-kaf-client
VERSION     ?= 0.1.0-dev
COMMIT      := $(shell git rev-parse --short HEAD 2>/dev/null || echo unknown)
BUILDTIME   := $(shell date -u +%Y-%m-%dT%H:%M:%SZ)
LDFLAGS     := -s -w -X $(MODULE)/internal/version.version=$(VERSION) \
               -X $(MODULE)/internal/version.commit=$(COMMIT) \
               -X $(MODULE)/internal/version.buildTime=$(BUILDTIME)
PLATFORMS   := darwin/amd64 darwin/arm64 windows/amd64
E2E_COMPOSE := docker compose -f deploy/compose/e2e-tests.yaml $(shell [ -f deploy/compose/e2e-host.override.yaml ] && echo -f deploy/compose/e2e-host.override.yaml)
E2E_COMPOSE_AUTHZ := docker compose -f deploy/compose/e2e-tests.yaml -f deploy/compose/e2e-authz.override.yaml $(shell [ -f deploy/compose/e2e-host.override.yaml ] && echo -f deploy/compose/e2e-host.override.yaml)
TAURI_CLI_VERSION := 2.11.4
RUST_TARGET ?= $(shell rustc --print host-tuple)
DESKTOP_BUNDLES ?= app,dmg
# Rust tests inject sidecar I/O; skip only Tauri's ignored bundle artifact check.
DESKTOP_TEST_TAURI_CONFIG := {"bundle":{"externalBin":[]}}

.PHONY: test mcp-test mcp-settings-test lint build verify build-fe test-fe gen-go contract-check release-build vendor-upstream test-integration e2e-p1a e2e-p1b e2e-p2c e2e-p3-ksql desktop-tools desktop-sidecar desktop-test desktop-build desktop-package

test:
	go test ./... -race -covermode=atomic -coverprofile=coverage.out -coverpkg=./internal/...
	bash scripts/coverage-gate.sh coverage.out

mcp-test:
	go test ./internal/mcppolicy ./internal/mcpserver ./cmd/cy-kaf-client \
	  -run 'MCP|Mcp|Catalog|Policy' -race -count=1

mcp-settings-test:
	go test ./internal/mcppolicy ./internal/infra/mcpclient \
	  ./internal/app/mcpsettings ./internal/api ./cmd/cy-kaf-client -race -count=1
	cd frontend && pnpm exec jest --ci \
	  src/components/Settings/__tests__/SettingsModal.spec.tsx \
	  src/components/NavBar/__tests__/NavBar.spec.tsx
	$(MAKE) desktop-test

test-integration:
	go test -p=1 -tags integration ./internal/infra/... -v -timeout 10m

lint:
	golangci-lint run
	cd frontend && pnpm lint

build:
	CGO_ENABLED=0 go build -ldflags '$(LDFLAGS)' -o dist/$(BIN) ./cmd/cy-kaf-client

release-build: build-fe
	@for p in $(PLATFORMS); do \
	  os=$${p%/*}; arch=$${p#*/}; ext=$$( [ $$os = windows ] && echo .exe || echo "" ); \
	  echo "==> $$os/$$arch"; \
	  CGO_ENABLED=0 GOOS=$$os GOARCH=$$arch go build -ldflags '$(LDFLAGS)' \
	    -o dist/$(BIN)-$$os-$$arch$$ext ./cmd/cy-kaf-client || exit 1; \
	done

desktop-tools:
	cargo install tauri-cli --version $(TAURI_CLI_VERSION) --locked

desktop-sidecar: build-fe
	go run ./scripts/build-desktop-sidecar -target $(RUST_TARGET) \
	  -output-dir desktop/src-tauri/binaries -version $(VERSION) \
	  -commit $(COMMIT) -build-time $(BUILDTIME)

desktop-test:
	cd desktop/src-tauri && cargo fmt --check
	cd desktop/src-tauri && TAURI_CONFIG='$(DESKTOP_TEST_TAURI_CONFIG)' cargo test --locked
	cd desktop/src-tauri && TAURI_CONFIG='$(DESKTOP_TEST_TAURI_CONFIG)' cargo clippy --locked --all-targets -- -D warnings

desktop-build: desktop-sidecar
	cd desktop && cargo tauri build --target $(RUST_TARGET) --no-bundle

desktop-package: desktop-sidecar
	cd desktop && cargo tauri build --target $(RUST_TARGET) --bundles $(DESKTOP_BUNDLES)

build-fe:
	cd frontend && pnpm install --frozen-lockfile && pnpm build
	rm -rf webui/static && mkdir -p webui/static
	cp -R frontend/build/vite/static/. webui/static/
	touch webui/static/.gitkeep

test-fe:
	cd frontend && pnpm install --frozen-lockfile && pnpm exec jest --ci

gen-go:
	go run github.com/oapi-codegen/oapi-codegen/v2/cmd/oapi-codegen@v2.6.0 \
	  -config oapi-codegen.yaml contract/openapi.yaml
	go run ./scripts/genstub

contract-check: gen-go
	git diff --exit-code -- internal/api/generated/

vendor-upstream:
	bash scripts/vendor-upstream.sh

# e2e-p1a: one-shot upstream e2e entry point for the P1 surface implemented so
# far (Brokers + Clusters). Boots kafka0 from the vendored compose file (no
# override needed — kafka0 already publishes 9092 to the host and advertises
# PLAINTEXT_HOST://localhost:9092, see deploy/compose/e2e-tests.yaml), builds
# and starts our own binary in place of the vendored kafbat-ui service, waits
# for both Kafka and the cluster cache to be genuinely ready, then runs
# upstream Cucumber against just Brokers.feature. The whole body is one shell
# script (backslash-continued, like release-build's loop) with a trap so
# cleanup (kill our binary, compose down, rm temp files) always runs, even if
# an earlier readiness wait times out and exits 1 — a bare per-line recipe
# would abort before reaching a cleanup line written only at the end.
#
# Two readiness gates matter more than they look:
#  - Kafka: polls `docker inspect` Health.Status, not a bare `nc -z`. The
#    PLAINTEXT_HOST listener socket accepts TCP connections well before KRaft
#    finishes controller/broker startup, so nc -z reports "up" prematurely;
#    the container healthcheck actually round-trips kafka-broker-api-versions
#    and is the true readiness signal (confirmed empirically: nc -z succeeds
#    within ~1s of container start, Health.Status only flips to healthy after
#    ~20s).
#  - Cluster cache: polls our own /api/clusters for "status":"ONLINE". Our
#    StateCache's first refresh runs in the background (main doesn't block
#    startup on it — see internal/app/cluster/state.go Start), so
#    /actuator/health alone returns 200 well before the Kafka admin client has
#    completed its first successful scrape; without this second gate the
#    Brokers page can render against an empty/offline snapshot.
# --name scopes the run to Brokers.feature's 2 scenarios: cucumber.js's
# "default" profile sets paths=["src/features/"], and cucumber-js merges
# profile paths additively with any CLI path argument (see docs/provenance.md
# §3), so a bare feature-path argument alone still pulls in all 8
# features/32 scenarios — most of which exercise endpoints P1a hasn't
# implemented yet and would fail the target for reasons unrelated to Brokers.
e2e-p1a: build-fe build
	$(E2E_COMPOSE) up -d kafka0; \
	trap '[ -f /tmp/cy-kaf-e2e.pid ] && kill "$$(cat /tmp/cy-kaf-e2e.pid)" 2>/dev/null; $(E2E_COMPOSE) down; rm -f /tmp/cy-kaf-e2e.pid /tmp/cy-kaf-e2e.yaml' EXIT; \
	echo "waiting for kafka0 to be healthy"; \
	ok=""; \
	for i in $$(seq 1 60); do \
	  h=$$(docker inspect --format='{{.State.Health.Status}}' kafka0 2>/dev/null); \
	  if [ "$$h" = "healthy" ]; then ok=1; break; fi; \
	  sleep 2; \
	done; \
	if [ -z "$$ok" ]; then echo "kafka0 not healthy after 120s" >&2; exit 1; fi; \
	printf 'kafka:\n  clusters:\n    - name: local\n      bootstrapServers: localhost:9092\n' > /tmp/cy-kaf-e2e.yaml; \
	./dist/cy-kaf-client --no-browser --port 8080 --config /tmp/cy-kaf-e2e.yaml >/tmp/cy-kaf-e2e.log 2>&1 & \
	echo $$! > /tmp/cy-kaf-e2e.pid; \
	echo "waiting for cy-kaf-client health on :8080"; \
	ok=""; \
	for i in $$(seq 1 30); do \
	  if curl -sf http://127.0.0.1:8080/actuator/health >/dev/null 2>&1; then ok=1; break; fi; \
	  sleep 1; \
	done; \
	if [ -z "$$ok" ]; then echo "cy-kaf-client did not become healthy on :8080 after 30s" >&2; cat /tmp/cy-kaf-e2e.log >&2; exit 1; fi; \
	echo "waiting for cluster 'local' to report ONLINE"; \
	ok=""; \
	for i in $$(seq 1 60); do \
	  if curl -sf http://127.0.0.1:8080/api/clusters 2>/dev/null | grep -q '"status":"ONLINE"'; then ok=1; break; fi; \
	  sleep 1; \
	done; \
	if [ -z "$$ok" ]; then echo "cluster 'local' did not reach ONLINE after 60s" >&2; cat /tmp/cy-kaf-e2e.log >&2; exit 1; fi; \
	(cd e2e && ENV=prod FORCE_COLOR=0 npx cucumber-js --config config/cucumber.js --name "^Brokers visibility BrokerDetails visibility$$" src/features/Brokers.feature)

# e2e-p1b: upstream Cucumber against the P1b surface (Topics + Consumer
# Groups backends), exercised through navigation/Topics/TopicsActions, plus
# a Brokers.feature regression. Same skeleton as e2e-p1a above (compose
# boot, trap-based cleanup, dual Kafka/cluster readiness gates, --name
# anchoring against cucumber.js's additive `paths` default — see that
# target's own comment for why a bare feature-path CLI argument alone can't
# scope the run). Differences from e2e-p1a:
#
#  1. Seed data (Step 1 recon; Plan A chosen — the vendored
#     deploy/compose/e2e-tests.yaml kafka-init-topics service, unmodified;
#     Plan B, curl'ing our own createTopic API, wasn't needed): brought up
#     alongside kafka0. It depends_on kafka0 service_healthy, then creates
#     topics `users` (3 partitions) and `messages` (2 partitions) and pipes
#     data/message.json into `users` via kafka-console-producer.
#     Topics.feature's first scenario hardcodes a search for topic "users"
#     (P0 recon) and never asserts on message content/count, so this
#     one-shot seed already covers it (confirmed empirically: exits 0,
#     `kafka-topics --list` shows both topics). kafka-init-topics has no
#     container_name in the vendored compose file (unlike kafka0), so its
#     real container name is compose-project-derived (confirmed:
#     compose-kafka-init-topics-1) rather than the bare service name --
#     `$(E2E_COMPOSE) ps -a -q kafka-init-topics` resolves it either way, so
#     the gate below uses that instead of hardcoding a name. Topics.feature's
#     *second* scenario ALSO hardcodes a search for `__consumer_offsets`
#     (expected visible by default — frontend/src/components/Topics/List/
#     ListPage.tsx's `showInternal: !searchParams.has('hideInternal')`
#     confirms internal topics show by default) — a real broker only
#     lazily creates that internal topic on first consumer-group activity,
#     which this minimal kafka0-only bring-up never generates, so it's
#     seeded explicitly too, straight via kafka-topics on kafka0 (not a
#     kafka-init-topics/vendored-compose change).
#  2. A third readiness gate: TopicService.List (internal/app/cluster/
#     topic.go) reads the periodic StateCache, refreshed only every 30s
#     (cmd/cy-kaf-client/main.go's clusterStateRefreshInterval) — unlike
#     GetBrokerConfig etc., it's not a live per-request Kafka call. Starting
#     the binary only after kafka-init-topics has already exited (gate
#     above) means the binary's own first (startup) scrape should already
#     see `users` — confirmed empirically: the cluster-ONLINE gate (gate 2
#     below) only flips after that first scrape completes, so by the time
#     it passes the seed is normally already visible and this gate resolves
#     on its first check. It's still generously bounded (120s) as a safety
#     net in case that first scrape ever loses the race and a second 30s
#     cycle is needed.
#  3. The deep-route white-screen regression (Task 3's <base href> fix,
#     internal/api/static.go's loadIndex): curl a real deep client-side
#     route — /ui/clusters/local/all-topics, the Topics list's own URL
#     (frontend/src/lib/paths.ts's clusterTopicsPath) — and assert the
#     served HTML contains the injected <base href="/">. A regression here
#     is otherwise invisible to Cucumber (which only ever navigates via
#     clicks from `/`, never a hard reload of a deep route) and would only
#     show up as a silent white screen in a real browser.
#
# RESOLVED (Step 4 escalation — see .superpowers/sdd/task-8-report.md and
# progress.md's PLAN AMENDMENT #1-#3): reaching 12/12 surfaced three real,
# e2e-only-visible backend gaps behind "TopicCreate ui functions" / "Topic
# Delete", each fixed as its own reviewed task (never papered over by
# stubbing the backend or dropping scenarios from the --name anchor):
#   - Task 8a (getTopicConnectors, c3c788e): Topic.tsx unconditionally
#     suspense-queries GET .../topics/{name}/connectors; the 501 stub drove
#     every topic-detail route to /404. Now answers an empty [] (no cluster
#     in scope configures Kafka Connect, so that's the correct subset, not a
#     fake) — the detail route renders again.
#   - Task 8b (read-your-writes, 6db19f0): topic writes go to the live
#     cluster but the list reads a 30s StateCache, so a just-created topic
#     stayed invisible for up to 30s, racing the step's 30s timeout. Writes
#     now refresh that cluster's cache synchronously.
#   - Task 8c (messagesCount, ac4a26e): the Topics-list row left its
#     message-count column unpopulated (rendered "N/A"), so the vendored row
#     locator's hardcoded "... 0 0 Bytes" never matched. Now computed as
#     Σ(EndOffset-StartOffset) from the already-scraped partition offsets.
# All three land before this target's own commit; `make e2e-p1b` is 12/12.
#
# --name anchors exactly these 12 scenarios (of 32 across all 8 vendored
# feature files — cucumber.js's paths:["src/features/"] default means every
# feature is discovered regardless of which paths we pass on the CLI below;
# only --name actually scopes what runs, same additive-paths pitfall
# e2e-p1a's own comment documents):
#   navigation.feature    (4 of 7)  Navigate to Brokers / Navigate to Topics /
#     Navigate to Consumers / Navigate to all clusters. EXCLUDED: Navigate to
#     Schema Registry / Kafka Connect / KSQL DB — those three integrations
#     are never configured for the `local` cluster this target's config
#     writes below (no schema registry/connect/ksql URL), so
#     domain/cluster.Definition.Features() never reports those features and
#     the frontend never renders their nav links at all (P0 recon).
#   Topics.feature         (2 of 2)  Topics elements / Topics serchfield and
#     ShowInternalTopics (upstream's own spelling) — depend on the seeded
#     `users` topic (gate 2 above).
#   TopicsActions.feature  (4 of 4)  TopicCreate elemets visible / TopicCreate
#     ui functions / TopicCreate time to retain data functions / Topic Delete
#     (again upstream's own spelling) — the last drives a real deleteTopic,
#     needing the cluster's TOPIC_DELETION feature on; delete.topic.enable
#     defaults true on cp-kafka (same as confluent-local), and
#     internal/infra/kafka/state.go's topicDeletionEnabledFrom also defaults
#     true when the key is absent from a broker's config.
#   Brokers.feature        (2 of 2, same scenario name twice — unchanged
#     regression from e2e-p1a) Brokers visibility BrokerDetails visibility.
# Total: 4+2+4+2 = 12.
e2e-p1b: build-fe build
	$(E2E_COMPOSE) up -d kafka0 kafka-init-topics; \
	trap '[ -f /tmp/cy-kaf-e2e-p1b.pid ] && kill "$$(cat /tmp/cy-kaf-e2e-p1b.pid)" 2>/dev/null; $(E2E_COMPOSE) down; rm -f /tmp/cy-kaf-e2e-p1b.pid /tmp/cy-kaf-e2e-p1b.yaml' EXIT; \
	echo "waiting for kafka0 to be healthy"; \
	ok=""; \
	for i in $$(seq 1 60); do \
	  h=$$(docker inspect --format='{{.State.Health.Status}}' kafka0 2>/dev/null); \
	  if [ "$$h" = "healthy" ]; then ok=1; break; fi; \
	  sleep 2; \
	done; \
	if [ -z "$$ok" ]; then echo "kafka0 not healthy after 120s" >&2; exit 1; fi; \
	echo "waiting for kafka-init-topics seed job (users/messages topics + data/message.json) to finish"; \
	cid=$$($(E2E_COMPOSE) ps -a -q kafka-init-topics 2>/dev/null); \
	ok=""; \
	for i in $$(seq 1 120); do \
	  s=$$(docker inspect --format='{{.State.Status}}' "$$cid" 2>/dev/null); \
	  if [ "$$s" = "exited" ]; then ok=1; break; fi; \
	  sleep 1; \
	done; \
	if [ -z "$$ok" ]; then echo "kafka-init-topics did not finish after 120s" >&2; $(E2E_COMPOSE) logs kafka-init-topics >&2; exit 1; fi; \
	ec=$$(docker inspect --format='{{.State.ExitCode}}' "$$cid" 2>/dev/null); \
	if [ "$$ec" != "0" ]; then echo "kafka-init-topics exited $$ec (seed data not created)" >&2; $(E2E_COMPOSE) logs kafka-init-topics >&2; exit 1; fi; \
	echo "seeding __consumer_offsets (Topics.feature's second scenario hardcodes a search for it; a real Kafka cluster only lazily creates this internal topic on first consumer-group activity, and this target's minimal kafka0-only bring-up deliberately has none -- confirmed empirically, so we pre-create it directly, matching kafka-init-topics's own use of the plain kafka-topics CLI)"; \
	docker exec kafka0 kafka-topics --create --topic __consumer_offsets --partitions 1 --replication-factor 1 --if-not-exists --bootstrap-server localhost:9092 || { echo "failed to seed __consumer_offsets" >&2; exit 1; }; \
	printf 'kafka:\n  clusters:\n    - name: local\n      bootstrapServers: localhost:9092\n' > /tmp/cy-kaf-e2e-p1b.yaml; \
	./dist/cy-kaf-client --no-browser --port 8080 --config /tmp/cy-kaf-e2e-p1b.yaml >/tmp/cy-kaf-e2e-p1b.log 2>&1 & \
	echo $$! > /tmp/cy-kaf-e2e-p1b.pid; \
	echo "waiting for cy-kaf-client health on :8080"; \
	ok=""; \
	for i in $$(seq 1 30); do \
	  if curl -sf http://127.0.0.1:8080/actuator/health >/dev/null 2>&1; then ok=1; break; fi; \
	  sleep 1; \
	done; \
	if [ -z "$$ok" ]; then echo "cy-kaf-client did not become healthy on :8080 after 30s" >&2; cat /tmp/cy-kaf-e2e-p1b.log >&2; exit 1; fi; \
	echo "waiting for cluster 'local' to report ONLINE"; \
	ok=""; \
	for i in $$(seq 1 60); do \
	  if curl -sf http://127.0.0.1:8080/api/clusters 2>/dev/null | grep -q '"status":"ONLINE"'; then ok=1; break; fi; \
	  sleep 1; \
	done; \
	if [ -z "$$ok" ]; then echo "cluster 'local' did not reach ONLINE after 60s" >&2; cat /tmp/cy-kaf-e2e-p1b.log >&2; exit 1; fi; \
	echo "waiting for seeded 'users' topic via /api/clusters/local/topics (StateCache-backed; generously bounded past one 30s refresh cycle)"; \
	ok=""; \
	for i in $$(seq 1 60); do \
	  if curl -sf http://127.0.0.1:8080/api/clusters/local/topics 2>/dev/null | grep -q '"name":"users"'; then ok=1; break; fi; \
	  sleep 2; \
	done; \
	if [ -z "$$ok" ]; then echo "'users' topic did not appear after 120s" >&2; cat /tmp/cy-kaf-e2e-p1b.log >&2; exit 1; fi; \
	echo "asserting deep-route base href regression fix (internal/api/static.go)"; \
	curl -s http://127.0.0.1:8080/ui/clusters/local/all-topics | grep -q '<base href="/">' \
	  || { echo 'FATAL: deep-route base href missing' >&2; cat /tmp/cy-kaf-e2e-p1b.log >&2; exit 1; }; \
	(cd e2e && ENV=prod FORCE_COLOR=0 npx cucumber-js --config config/cucumber.js --name "^(Navigate to Brokers|Navigate to Topics|Navigate to Consumers|Navigate to all clusters|Topics elements|Topics serchfield and ShowInternalTopics|TopicCreate elemets visible|TopicCreate ui functions|TopicCreate time to retain data functions|Topic Delete|Brokers visibility BrokerDetails visibility)$$" src/features/navigation.feature src/features/Topics.feature src/features/TopicsActions.feature src/features/Brokers.feature)

e2e-p1c: build-fe build
	$(E2E_COMPOSE) up -d kafka0 kafka-init-topics; \
	trap '[ -f /tmp/cy-kaf-e2e-p1c.pid ] && kill "$$(cat /tmp/cy-kaf-e2e-p1c.pid)" 2>/dev/null; $(E2E_COMPOSE) down; rm -f /tmp/cy-kaf-e2e-p1c.pid /tmp/cy-kaf-e2e-p1c.yaml' EXIT; \
	echo "waiting for kafka0 to be healthy"; \
	ok=""; \
	for i in $$(seq 1 60); do \
	  h=$$(docker inspect --format='{{.State.Health.Status}}' kafka0 2>/dev/null); \
	  if [ "$$h" = "healthy" ]; then ok=1; break; fi; \
	  sleep 2; \
	done; \
	if [ -z "$$ok" ]; then echo "kafka0 not healthy after 120s" >&2; exit 1; fi; \
	echo "waiting for kafka-init-topics seed job (users/messages topics + data/message.json) to finish"; \
	cid=$$($(E2E_COMPOSE) ps -a -q kafka-init-topics 2>/dev/null); \
	ok=""; \
	for i in $$(seq 1 30); do \
	  s=$$(docker inspect --format='{{.State.Status}}' "$$cid" 2>/dev/null); \
	  if [ "$$s" = "exited" ]; then ok=1; break; fi; \
	  sleep 1; \
	done; \
	if [ -z "$$ok" ]; then echo "kafka-init-topics did not finish after 30s" >&2; $(E2E_COMPOSE) logs kafka-init-topics >&2; exit 1; fi; \
	ec=$$(docker inspect --format='{{.State.ExitCode}}' "$$cid" 2>/dev/null); \
	if [ "$$ec" != "0" ]; then echo "kafka-init-topics exited $$ec (seed data not created)" >&2; $(E2E_COMPOSE) logs kafka-init-topics >&2; exit 1; fi; \
	printf 'kafka:\n  clusters:\n    - name: local\n      bootstrapServers: localhost:9092\n' > /tmp/cy-kaf-e2e-p1c.yaml; \
	./dist/cy-kaf-client --no-browser --port 8080 --config /tmp/cy-kaf-e2e-p1c.yaml >/tmp/cy-kaf-e2e-p1c.log 2>&1 & \
	echo $$! > /tmp/cy-kaf-e2e-p1c.pid; \
	echo "waiting for cy-kaf-client health on :8080"; \
	ok=""; \
	for i in $$(seq 1 30); do \
	  if curl -sf http://127.0.0.1:8080/actuator/health >/dev/null 2>&1; then ok=1; break; fi; \
	  sleep 1; \
	done; \
	if [ -z "$$ok" ]; then echo "cy-kaf-client did not become healthy on :8080 after 30s" >&2; cat /tmp/cy-kaf-e2e-p1c.log >&2; exit 1; fi; \
	echo "waiting for cluster 'local' to report ONLINE"; \
	ok=""; \
	for i in $$(seq 1 60); do \
	  if curl -sf http://127.0.0.1:8080/api/clusters 2>/dev/null | grep -q '"status":"ONLINE"'; then ok=1; break; fi; \
	  sleep 1; \
	done; \
	if [ -z "$$ok" ]; then echo "cluster 'local' did not reach ONLINE after 60s" >&2; cat /tmp/cy-kaf-e2e-p1c.log >&2; exit 1; fi; \
	echo "running P1c TopicsMessages scenarios (produce/browse/serde/clear/smartfilter); --name anchored so the additive cucumber.js paths do not drag in other features"; \
	(cd e2e && ENV=prod FORCE_COLOR=0 npx cucumber-js --config config/cucumber.js --name "^(TopicName ui|Produce Message|Topic Message cleanup policy|Produce messages clear messages|Topic message filter)$$" src/features/TopicsMessages.feature)

verify: lint test contract-check build desktop-test
	@echo "VERIFY OK"

e2e-p2a: build-fe build
	$(E2E_COMPOSE) up -d kafka0 schemaregistry0; \
	trap '[ -f /tmp/cy-kaf-e2e-p2a.pid ] && kill "$$(cat /tmp/cy-kaf-e2e-p2a.pid)" 2>/dev/null; $(E2E_COMPOSE) down; rm -f /tmp/cy-kaf-e2e-p2a.pid /tmp/cy-kaf-e2e-p2a.yaml' EXIT; \
	echo "waiting for kafka0 to be healthy"; \
	ok=""; \
	for i in $$(seq 1 60); do \
	  h=$$(docker inspect --format='{{.State.Health.Status}}' kafka0 2>/dev/null); \
	  if [ "$$h" = "healthy" ]; then ok=1; break; fi; \
	  sleep 2; \
	done; \
	if [ -z "$$ok" ]; then echo "kafka0 not healthy after 120s" >&2; exit 1; fi; \
	echo "waiting for schema registry /subjects on :8085"; \
	ok=""; \
	for i in $$(seq 1 60); do \
	  if curl -sf http://127.0.0.1:8085/subjects >/dev/null 2>&1; then ok=1; break; fi; \
	  sleep 2; \
	done; \
	if [ -z "$$ok" ]; then echo "schema registry not reachable on :8085 after 120s" >&2; $(E2E_COMPOSE) logs schemaregistry0 >&2; exit 1; fi; \
	printf 'kafka:\n  clusters:\n    - name: local\n      bootstrapServers: localhost:9092\n      schemaRegistry: http://localhost:8085\n' > /tmp/cy-kaf-e2e-p2a.yaml; \
	./dist/cy-kaf-client --no-browser --port 8080 --config /tmp/cy-kaf-e2e-p2a.yaml >/tmp/cy-kaf-e2e-p2a.log 2>&1 & \
	echo $$! > /tmp/cy-kaf-e2e-p2a.pid; \
	echo "waiting for cy-kaf-client health on :8080"; \
	ok=""; \
	for i in $$(seq 1 30); do \
	  if curl -sf http://127.0.0.1:8080/actuator/health >/dev/null 2>&1; then ok=1; break; fi; \
	  sleep 1; \
	done; \
	if [ -z "$$ok" ]; then echo "cy-kaf-client did not become healthy on :8080 after 30s" >&2; cat /tmp/cy-kaf-e2e-p2a.log >&2; exit 1; fi; \
	echo "waiting for cluster 'local' to report ONLINE"; \
	ok=""; \
	for i in $$(seq 1 60); do \
	  if curl -sf http://127.0.0.1:8080/api/clusters 2>/dev/null | grep -q '"status":"ONLINE"'; then ok=1; break; fi; \
	  sleep 1; \
	done; \
	if [ -z "$$ok" ]; then echo "cluster 'local' did not reach ONLINE after 60s" >&2; cat /tmp/cy-kaf-e2e-p2a.log >&2; exit 1; fi; \
	echo "running P2a SchemaRegistry scenarios (create/view/delete avro+json+protobuf); --name anchored so additive cucumber.js paths don't drag in other features"; \
	(cd e2e && ENV=prod FORCE_COLOR=0 npx cucumber-js --config config/cucumber.js --name "^(SchemaRegistry and SchemaRegistryCreate visibility|SchemaRegistry Avro schema actions|SchemaRegistry Json schema actions)$$" src/features/SchemaRegistry.feature)

e2e-p2b: build-fe build
	$(E2E_COMPOSE) up -d kafka0 schemaregistry0 kafka-connect0 postgres-db create-connectors; \
	trap '[ -f /tmp/cy-kaf-e2e-p2b.pid ] && kill "$$(cat /tmp/cy-kaf-e2e-p2b.pid)" 2>/dev/null; $(E2E_COMPOSE) down; rm -f /tmp/cy-kaf-e2e-p2b.pid /tmp/cy-kaf-e2e-p2b.yaml' EXIT; \
	echo "waiting for kafka0 to be healthy"; \
	ok=""; \
	for i in $$(seq 1 60); do \
	  h=$$(docker inspect --format='{{.State.Health.Status}}' kafka0 2>/dev/null); \
	  if [ "$$h" = "healthy" ]; then ok=1; break; fi; \
	  sleep 2; \
	done; \
	if [ -z "$$ok" ]; then echo "kafka0 not healthy after 120s" >&2; exit 1; fi; \
	echo "waiting for kafka-connect0 /connectors on :8083"; \
	ok=""; \
	for i in $$(seq 1 60); do \
	  if curl -sf http://127.0.0.1:8083/connectors >/dev/null 2>&1; then ok=1; break; fi; \
	  sleep 2; \
	done; \
	if [ -z "$$ok" ]; then echo "kafka-connect0 not reachable on :8083 after 120s" >&2; $(E2E_COMPOSE) logs kafka-connect0 >&2; exit 1; fi; \
	echo "waiting for create-connectors seed job (sink_postgres_activities/s3-sink/... connectors) to finish"; \
	cid=$$($(E2E_COMPOSE) ps -a -q create-connectors 2>/dev/null); \
	ok=""; \
	for i in $$(seq 1 60); do \
	  s=$$(docker inspect --format='{{.State.Status}}' "$$cid" 2>/dev/null); \
	  if [ "$$s" = "exited" ]; then ok=1; break; fi; \
	  sleep 2; \
	done; \
	if [ -z "$$ok" ]; then echo "create-connectors did not finish after 120s" >&2; $(E2E_COMPOSE) logs create-connectors >&2; exit 1; fi; \
	ec=$$(docker inspect --format='{{.State.ExitCode}}' "$$cid" 2>/dev/null); \
	if [ "$$ec" != "0" ]; then echo "create-connectors exited $$ec (seed connectors not created)" >&2; $(E2E_COMPOSE) logs create-connectors >&2; exit 1; fi; \
	printf 'kafka:\n  clusters:\n    - name: local\n      bootstrapServers: localhost:9092\n      kafkaConnect:\n        - name: first\n          address: http://localhost:8083\n' > /tmp/cy-kaf-e2e-p2b.yaml; \
	./dist/cy-kaf-client --no-browser --port 8080 --config /tmp/cy-kaf-e2e-p2b.yaml >/tmp/cy-kaf-e2e-p2b.log 2>&1 & \
	echo $$! > /tmp/cy-kaf-e2e-p2b.pid; \
	echo "waiting for cy-kaf-client health on :8080"; \
	ok=""; \
	for i in $$(seq 1 30); do \
	  if curl -sf http://127.0.0.1:8080/actuator/health >/dev/null 2>&1; then ok=1; break; fi; \
	  sleep 1; \
	done; \
	if [ -z "$$ok" ]; then echo "cy-kaf-client did not become healthy on :8080 after 30s" >&2; cat /tmp/cy-kaf-e2e-p2b.log >&2; exit 1; fi; \
	echo "waiting for cluster 'local' to report ONLINE with KAFKA_CONNECT feature"; \
	ok=""; \
	for i in $$(seq 1 60); do \
	  resp=$$(curl -sf http://127.0.0.1:8080/api/clusters 2>/dev/null); \
	  if echo "$$resp" | grep -q '"status":"ONLINE"' && echo "$$resp" | grep -q 'KAFKA_CONNECT'; then ok=1; break; fi; \
	  sleep 1; \
	done; \
	if [ -z "$$ok" ]; then echo "cluster 'local' did not reach ONLINE w/ KAFKA_CONNECT after 60s" >&2; cat /tmp/cy-kaf-e2e-p2b.log >&2; exit 1; fi; \
	echo "running P2b KafkaConnect scenarios (search/main page/connector page/connector page functions); --name anchored so additive cucumber.js paths don't drag in other features"; \
	(cd e2e && ENV=prod FORCE_COLOR=0 npx cucumber-js --config config/cucumber.js --name "^(KafkaConnect search is working|KafkaConnect main page functions|KafkaConnect connector page|KafkaConnect connector page functions)$$" src/features/KafkaConnect.feature)

e2e-p2c: build-fe build
	$(E2E_COMPOSE_AUTHZ) up -d kafka0; \
	trap 'if [ -f /tmp/cy-kaf-e2e-p2c.pid ]; then pid=$$(cat /tmp/cy-kaf-e2e-p2c.pid); kill "$$pid" 2>/dev/null || true; wait "$$pid" 2>/dev/null || true; fi; $(E2E_COMPOSE_AUTHZ) down; rm -f /tmp/cy-kaf-e2e-p2c.pid /tmp/cy-kaf-e2e-p2c.yaml /tmp/cy-kaf-e2e-p2c.log' EXIT; \
	envs=$$(docker inspect --format='{{range .Config.Env}}{{println .}}{{end}}' kafka0 2>/dev/null); \
	echo "$$envs" | grep -Fxq 'KAFKA_AUTHORIZER_CLASS_NAME=org.apache.kafka.metadata.authorizer.StandardAuthorizer' || { echo "authorizer class missing from kafka0" >&2; exit 1; }; \
	echo "$$envs" | grep -Fxq 'KAFKA_SUPER_USERS=User:ANONYMOUS' || { echo "anonymous e2e super user missing from kafka0" >&2; exit 1; }; \
	echo "$$envs" | grep -Fxq 'KAFKA_ALLOW_EVERYONE_IF_NO_ACL_FOUND=true' || { echo "allow-if-no-acl e2e setting missing from kafka0" >&2; exit 1; }; \
	echo "authorizer overlay verified: StandardAuthorizer; User:ANONYMOUS super user; allow-if-no-acl=true"; \
	echo "waiting for kafka0 (authorizer-enabled) to be healthy"; \
	ok=""; \
	for i in $$(seq 1 60); do \
	  h=$$(docker inspect --format='{{.State.Health.Status}}' kafka0 2>/dev/null); \
	  if [ "$$h" = "healthy" ]; then ok=1; break; fi; \
	  sleep 2; \
	done; \
	if [ -z "$$ok" ]; then echo "kafka0 not healthy after 120s" >&2; $(E2E_COMPOSE_AUTHZ) logs kafka0 >&2; exit 1; fi; \
	printf 'kafka:\n  clusters:\n    - name: local\n      bootstrapServers: localhost:9092\n' > /tmp/cy-kaf-e2e-p2c.yaml; \
	./dist/cy-kaf-client --no-browser --port 8080 --config /tmp/cy-kaf-e2e-p2c.yaml >/tmp/cy-kaf-e2e-p2c.log 2>&1 & \
	echo $$! > /tmp/cy-kaf-e2e-p2c.pid; \
	echo "waiting for cy-kaf-client health on :8080"; \
	ok=""; \
	for i in $$(seq 1 30); do \
	  if curl -sf http://127.0.0.1:8080/actuator/health >/dev/null 2>&1; then ok=1; break; fi; \
	  sleep 1; \
	done; \
	if [ -z "$$ok" ]; then echo "cy-kaf-client not healthy on :8080 after 30s" >&2; cat /tmp/cy-kaf-e2e-p2c.log >&2; exit 1; fi; \
	echo "waiting for cluster 'local' ONLINE"; \
	ok=""; \
	for i in $$(seq 1 60); do \
	  resp=$$(curl -sf http://127.0.0.1:8080/api/clusters 2>/dev/null); \
	  if echo "$$resp" | grep -q '"status":"ONLINE"'; then ok=1; break; fi; \
	  sleep 1; \
	done; \
	if [ -z "$$ok" ]; then echo "cluster 'local' not ONLINE after 60s" >&2; cat /tmp/cy-kaf-e2e-p2c.log >&2; exit 1; fi; \
	echo "e2e-p2c smoke: acl create/list/delete + quota upsert/list via real binary + authorizer broker"; \
	status=$$(curl -sSf -X POST http://127.0.0.1:8080/api/clusters/local/acls -H 'Content-Type: application/json' \
	  -d '{"resourceType":"TOPIC","resourceName":"e2e-acl","namePatternType":"LITERAL","principal":"User:e2e","host":"*","operation":"READ","permission":"ALLOW"}' -o /dev/null -w '%{http_code}') || { echo "createAcl request failed" >&2; exit 1; }; \
	if [ "$$status" != "204" ]; then echo "createAcl != 204 (got $$status)" >&2; exit 1; fi; \
	ok=""; acls=""; \
	for i in $$(seq 1 30); do \
	  acls=$$(curl -sSf http://127.0.0.1:8080/api/clusters/local/acls) || { echo "listAcls after create failed" >&2; exit 1; }; \
	  printf '%s' "$$acls" | node scripts/check-p2c-acl.js present; \
	  rc=$$?; \
	  if [ "$$rc" = "0" ]; then ok=1; break; fi; \
	  if [ "$$rc" = "2" ]; then exit 1; fi; \
	  sleep 1; \
	done; \
	if [ -z "$$ok" ]; then echo "listAcls missing exact e2e ACL after create for 30s; body=$$acls" >&2; exit 1; fi; \
	status=$$(curl -sSf -X DELETE http://127.0.0.1:8080/api/clusters/local/acls -H 'Content-Type: application/json' \
	  -d '{"resourceType":"TOPIC","resourceName":"e2e-acl","namePatternType":"LITERAL","principal":"User:e2e","host":"*","operation":"READ","permission":"ALLOW"}' -o /dev/null -w '%{http_code}') || { echo "deleteAcl request failed" >&2; exit 1; }; \
	if [ "$$status" != "204" ]; then echo "deleteAcl != 204 (got $$status)" >&2; exit 1; fi; \
	ok=""; acls=""; \
	for i in $$(seq 1 30); do \
	  acls=$$(curl -sSf http://127.0.0.1:8080/api/clusters/local/acls) || { echo "listAcls after delete failed" >&2; exit 1; }; \
	  printf '%s' "$$acls" | node scripts/check-p2c-acl.js absent; \
	  rc=$$?; \
	  if [ "$$rc" = "0" ]; then ok=1; break; fi; \
	  if [ "$$rc" = "2" ]; then exit 1; fi; \
	  sleep 1; \
	done; \
	if [ -z "$$ok" ]; then echo "listAcls still contains exact e2e ACL for 30s after delete; body=$$acls" >&2; exit 1; fi; \
	status=$$(curl -sSf -X POST http://127.0.0.1:8080/api/clusters/local/clientquotas -H 'Content-Type: application/json' \
	  -d '{"user":"e2e","quotas":{"producer_byte_rate":1024}}' -o /dev/null -w '%{http_code}') || { echo "upsertClientQuotas request failed" >&2; exit 1; }; \
	if [ "$$status" != "204" ]; then echo "upsertClientQuotas != 204 (got $$status)" >&2; exit 1; fi; \
	ok=""; quotas=""; \
	for i in $$(seq 1 30); do \
	  quotas=$$(curl -sSf http://127.0.0.1:8080/api/clusters/local/clientquotas) || { echo "listQuotas after upsert failed" >&2; exit 1; }; \
	  printf '%s' "$$quotas" | node scripts/check-p2c-quota.js; \
	  rc=$$?; \
	  if [ "$$rc" = "0" ]; then ok=1; break; fi; \
	  if [ "$$rc" = "2" ]; then exit 1; fi; \
	  sleep 1; \
	done; \
	if [ -z "$$ok" ]; then echo "listQuotas missing exact e2e entity after 30s (user=e2e, no clientId/ip, quotas exactly {producer_byte_rate:1024 numeric}); body=$$quotas" >&2; exit 1; fi; \
	echo "e2e-p2c smoke OK"

# e2e-p3-ksql: focused upstream KSQL scenarios against the existing Compose
# stack.  Keep every temporary artifact target-specific so this target can be
# diagnosed independently from the earlier e2e targets.
e2e-p3-ksql: build-fe build
	$(E2E_COMPOSE) up -d kafka0 schemaregistry0 kafka-connect0 ksqldb; \
	trap 'if test ! -f /tmp/cy-kaf-e2e-p3-ksql.pid; then :; else pid=$$(cat /tmp/cy-kaf-e2e-p3-ksql.pid); kill "$$pid" 2>/dev/null || true; wait "$$pid" 2>/dev/null || true; fi; $(E2E_COMPOSE) down; rm -f /tmp/cy-kaf-e2e-p3-ksql.pid /tmp/cy-kaf-e2e-p3-ksql.yaml /tmp/cy-kaf-e2e-p3-ksql.log' EXIT; \
	ok=""; for i in $$(seq 1 60); do test "$$(docker inspect --format='{{.State.Health.Status}}' kafka0 2>/dev/null)" = healthy && ok=1 && break; sleep 2; done; \
	test -n "$$ok" || { $(E2E_COMPOSE) logs kafka0 >&2; exit 1; }; \
	ok=""; for i in $$(seq 1 60); do curl -sf http://127.0.0.1:8088/info >/dev/null 2>&1 && ok=1 && break; sleep 2; done; \
	test -n "$$ok" || { $(E2E_COMPOSE) logs ksqldb >&2; exit 1; }; \
	printf 'kafka:\n  clusters:\n    - name: local\n      bootstrapServers: localhost:9092\n      schemaRegistry: http://localhost:8085\n      kafkaConnect:\n        - name: first\n          address: http://localhost:8083\n      ksqldbServer: http://localhost:8088\n' > /tmp/cy-kaf-e2e-p3-ksql.yaml; \
	./dist/cy-kaf-client --no-browser --port 8080 --config /tmp/cy-kaf-e2e-p3-ksql.yaml >/tmp/cy-kaf-e2e-p3-ksql.log 2>&1 & echo $$! > /tmp/cy-kaf-e2e-p3-ksql.pid; \
	ok=""; for i in $$(seq 1 30); do curl -sf http://127.0.0.1:8080/actuator/health >/dev/null 2>&1 && ok=1 && break; sleep 1; done; \
	test -n "$$ok" || { cat /tmp/cy-kaf-e2e-p3-ksql.log >&2; exit 1; }; \
	ok=""; for i in $$(seq 1 60); do curl -sf http://127.0.0.1:8080/api/clusters 2>/dev/null | grep -q '"status":"ONLINE"' && ok=1 && break; sleep 1; done; \
	test -n "$$ok" || { cat /tmp/cy-kaf-e2e-p3-ksql.log >&2; exit 1; }; \
	(cd e2e && ENV=prod FORCE_COLOR=0 npx cucumber-js --config config/cucumber.js --name '^KSQL DB (elements visibility|queries clear result|queries|cancel queries)$$' src/features/KsqlDb.feature)
