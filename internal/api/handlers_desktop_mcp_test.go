package api

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/cy-kaf/cy-kaf-client/internal/app/mcpsettings"
)

func TestDesktopMCPRoutesEnforceModeSessionAndOrigin(t *testing.T) {
	routes := []struct {
		name       string
		method     string
		path       string
		body       string
		serviceHit func(*fakeDesktopMCPService) int
	}{
		{
			name: "get settings", method: http.MethodGet, path: "/__desktop/mcp-settings",
			serviceHit: func(fake *fakeDesktopMCPService) int { return fake.getCalls },
		},
		{
			name: "put settings", method: http.MethodPut, path: "/__desktop/mcp-settings",
			body:       `{"enabled":true,"allowWrites":true,"confirmWrites":true}`,
			serviceHit: func(fake *fakeDesktopMCPService) int { return fake.updateCalls },
		},
		{
			name: "get integrations", method: http.MethodGet, path: "/__desktop/mcp-integrations",
			serviceHit: func(fake *fakeDesktopMCPService) int { return fake.integrationsCalls },
		},
		{
			name: "configure codex", method: http.MethodPost,
			path: "/__desktop/mcp-integrations/codex/configure", body: `{"replace":false}`,
			serviceHit: func(fake *fakeDesktopMCPService) int { return fake.configureCalls },
		},
		{
			name: "configure claude code", method: http.MethodPost,
			path: "/__desktop/mcp-integrations/claude-code/configure", body: `{"replace":true}`,
			serviceHit: func(fake *fakeDesktopMCPService) int { return fake.configureCalls },
		},
	}

	for _, route := range routes {
		t.Run(route.name, func(t *testing.T) {
			t.Run("browser mode is JSON 404", func(t *testing.T) {
				fake := newFakeDesktopMCPService()
				deps := testDesktopDeps(nil)
				deps.DesktopMCP = fake
				rec := serveDesktopMCPRequest(t, NewServer(deps), route.method, route.path, route.body, "", "")

				require.Equal(t, http.StatusNotFound, rec.Code)
				require.Equal(t, "application/json", rec.Header().Get("Content-Type"))
				requireDesktopMCPError(t, rec.Body.String(), "desktop_only", "Desktop only.")
				require.NotContains(t, strings.ToLower(rec.Body.String()), "<html")
				require.Zero(t, route.serviceHit(fake))
			})

			t.Run("desktop mode requires session cookie", func(t *testing.T) {
				fake := newFakeDesktopMCPService()
				rec := serveDesktopMCPRequest(
					t, newDesktopMCPHandler(fake), route.method, route.path, route.body, "", desktopTestOrigin,
				)

				require.Equal(t, http.StatusUnauthorized, rec.Code)
				require.JSONEq(t, `{"message":"desktop session required"}`, rec.Body.String())
				require.NotContains(t, rec.Body.String(), "fake CLI output")
				require.Zero(t, route.serviceHit(fake))
			})

			if route.method == http.MethodGet {
				t.Run("desktop GET with valid session returns JSON", func(t *testing.T) {
					fake := newFakeDesktopMCPService()
					rec := serveDesktopMCPRequest(
						t, newDesktopMCPHandler(fake), route.method, route.path, route.body,
						desktopTestToken, "",
					)

					require.Equal(t, http.StatusOK, rec.Code)
					require.Equal(t, "application/json", rec.Header().Get("Content-Type"))
					require.Equal(t, 1, route.serviceHit(fake))
				})
				return
			}

			for _, origin := range []struct {
				name  string
				value string
			}{
				{name: "missing Origin"},
				{name: "foreign Origin", value: "http://127.0.0.1:9"},
			} {
				t.Run(origin.name, func(t *testing.T) {
					fake := newFakeDesktopMCPService()
					rec := serveDesktopMCPRequest(
						t, newDesktopMCPHandler(fake), route.method, route.path, route.body,
						desktopTestToken, origin.value,
					)

					require.Equal(t, http.StatusForbidden, rec.Code)
					require.JSONEq(t, `{"message":"desktop origin required"}`, rec.Body.String())
					require.NotContains(t, rec.Body.String(), "fake CLI output")
					require.Zero(t, route.serviceHit(fake))
				})
			}

			t.Run("exact Origin invokes service", func(t *testing.T) {
				fake := newFakeDesktopMCPService()
				rec := serveDesktopMCPRequest(
					t, newDesktopMCPHandler(fake), route.method, route.path, route.body,
					desktopTestToken, desktopTestOrigin,
				)

				require.Equal(t, http.StatusOK, rec.Code)
				require.Equal(t, 1, route.serviceHit(fake))
				if strings.Contains(route.path, "claude-code") {
					require.Equal(t, mcpsettings.ClientClaudeCode, fake.configureClient)
					require.True(t, fake.configureReplace)
				}
				if strings.Contains(route.path, "/codex/") {
					require.Equal(t, mcpsettings.ClientCodex, fake.configureClient)
					require.False(t, fake.configureReplace)
				}
				if route.method == http.MethodPut {
					require.Equal(t, mcpsettings.UpdateRequest{
						Enabled: true, AllowWrites: true, ConfirmWrites: true,
					}, fake.updateRequest)
				}
			})
		})
	}
}

func TestDesktopMCPSettingsAndIntegrationJSONContracts(t *testing.T) {
	fake := newFakeDesktopMCPService()
	handler := newDesktopMCPHandler(fake)

	settings := serveDesktopMCPRequest(
		t, handler, http.MethodGet, "/__desktop/mcp-settings", "", desktopTestToken, "",
	)
	require.Equal(t, http.StatusOK, settings.Code)
	require.JSONEq(t, `{"enabled":true,"allowWrites":false}`, settings.Body.String())
	require.NotContains(t, settings.Body.String(), "version")

	integrations := serveDesktopMCPRequest(
		t, handler, http.MethodGet, "/__desktop/mcp-integrations", "", desktopTestToken, "",
	)
	require.Equal(t, http.StatusOK, integrations.Code)
	require.JSONEq(t, `{
		"clients":[{
			"client":"codex",
			"status":"not_configured",
			"canConfigure":true,
			"manualCommand":"codex mcp add",
			"message":""
		},{
			"client":"claude-code",
			"status":"configured",
			"canConfigure":false,
			"manualCommand":"claude mcp add",
			"message":""
		}]
	}`, integrations.Body.String())
}

func TestDesktopMCPRejectsInvalidBodiesBeforeServiceInvocation(t *testing.T) {
	largeBody := `{"enabled":true,"allowWrites":false}` + strings.Repeat(" ", (16<<10)+1)
	cases := []struct {
		name   string
		method string
		path   string
		body   string
	}{
		{name: "settings unknown field", method: http.MethodPut, path: "/__desktop/mcp-settings",
			body: `{"enabled":true,"allowWrites":false,"unknown":true}`},
		{name: "settings trailing JSON", method: http.MethodPut, path: "/__desktop/mcp-settings",
			body: `{"enabled":true,"allowWrites":false} {}`},
		{name: "settings body over 16 KiB", method: http.MethodPut, path: "/__desktop/mcp-settings",
			body: largeBody},
		{name: "settings executable field", method: http.MethodPut, path: "/__desktop/mcp-settings",
			body: `{"enabled":true,"allowWrites":false,"executable":"/tmp/tool"}`},
		{name: "settings args field", method: http.MethodPut, path: "/__desktop/mcp-settings",
			body: `{"enabled":true,"allowWrites":false,"args":["mcp"]}`},
		{name: "settings path field", method: http.MethodPut, path: "/__desktop/mcp-settings",
			body: `{"enabled":true,"allowWrites":false,"path":"/tmp/config"}`},
		{name: "settings env field", method: http.MethodPut, path: "/__desktop/mcp-settings",
			body: `{"enabled":true,"allowWrites":false,"env":{"TOKEN":"fake-cli-output"}}`},
		{name: "configure unknown field", method: http.MethodPost,
			path: "/__desktop/mcp-integrations/codex/configure",
			body: `{"replace":false,"unknown":true}`},
		{name: "configure trailing JSON", method: http.MethodPost,
			path: "/__desktop/mcp-integrations/codex/configure",
			body: `{"replace":false} {}`},
		{name: "configure body over 16 KiB", method: http.MethodPost,
			path: "/__desktop/mcp-integrations/codex/configure",
			body: `{"replace":false}` + strings.Repeat(" ", (16<<10)+1)},
		{name: "configure executable field", method: http.MethodPost,
			path: "/__desktop/mcp-integrations/codex/configure",
			body: `{"replace":false,"executable":"/tmp/tool"}`},
		{name: "configure args field", method: http.MethodPost,
			path: "/__desktop/mcp-integrations/codex/configure",
			body: `{"replace":false,"args":["mcp"]}`},
		{name: "configure path field", method: http.MethodPost,
			path: "/__desktop/mcp-integrations/codex/configure",
			body: `{"replace":false,"path":"/tmp/config"}`},
		{name: "configure env field", method: http.MethodPost,
			path: "/__desktop/mcp-integrations/codex/configure",
			body: `{"replace":false,"env":{"TOKEN":"fake-cli-output"}}`},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fake := newFakeDesktopMCPService()
			rec := serveDesktopMCPRequest(
				t, newDesktopMCPHandler(fake), tc.method, tc.path, tc.body,
				desktopTestToken, desktopTestOrigin,
			)

			require.Equal(t, http.StatusBadRequest, rec.Code)
			requireDesktopMCPError(t, rec.Body.String(), "invalid_request", "Invalid request.")
			require.NotContains(t, rec.Body.String(), "fake-cli-output")
			require.Zero(t, fake.updateCalls)
			require.Zero(t, fake.configureCalls)
		})
	}
}

func TestDesktopMCPRejectsUnknownAndSuffixedPathsWithoutSPA(t *testing.T) {
	cases := []struct {
		name   string
		method string
		path   string
	}{
		{name: "unknown client", method: http.MethodPost,
			path: "/__desktop/mcp-integrations/other/configure"},
		{name: "settings suffix", method: http.MethodGet,
			path: "/__desktop/mcp-settings/extra"},
		{name: "integrations suffix", method: http.MethodGet,
			path: "/__desktop/mcp-integrations/extra"},
		{name: "configure suffix", method: http.MethodPost,
			path: "/__desktop/mcp-integrations/codex/configure/extra"},
		{name: "unknown desktop path", method: http.MethodGet,
			path: "/__desktop/unknown"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fake := newFakeDesktopMCPService()
			rec := serveDesktopMCPRequest(
				t, newDesktopMCPHandler(fake), tc.method, tc.path, `{"replace":false}`,
				desktopTestToken, desktopTestOrigin,
			)

			require.Equal(t, http.StatusNotFound, rec.Code)
			require.Equal(t, "application/json", rec.Header().Get("Content-Type"))
			requireDesktopMCPError(t, rec.Body.String(), "invalid_request", "Invalid request.")
			require.NotContains(t, strings.ToLower(rec.Body.String()), "<html")
			require.Zero(t, fake.getCalls+fake.updateCalls+fake.integrationsCalls+fake.configureCalls)
		})
	}

	browser := testDesktopDeps(nil)
	browserRec := serveDesktopMCPRequest(
		t, NewServer(browser), http.MethodGet, "/__desktop/unknown", "", "", "",
	)
	require.Equal(t, http.StatusNotFound, browserRec.Code)
	requireDesktopMCPError(t, browserRec.Body.String(), "desktop_only", "Desktop only.")
	require.NotContains(t, strings.ToLower(browserRec.Body.String()), "<html")
}

func TestDesktopMCPMissingServiceDependencyReturnsJSON404(t *testing.T) {
	deps := testDesktopDeps(&DesktopOptions{
		SessionToken: desktopTestToken,
		Origin:       desktopTestOrigin,
		Shutdown:     func() {},
	})
	handler := NewServer(deps)
	routes := []struct {
		method string
		path   string
		body   string
	}{
		{method: http.MethodGet, path: "/__desktop/mcp-settings"},
		{method: http.MethodPut, path: "/__desktop/mcp-settings",
			body: `{"enabled":true,"allowWrites":false}`},
		{method: http.MethodGet, path: "/__desktop/mcp-integrations"},
		{method: http.MethodPost, path: "/__desktop/mcp-integrations/codex/configure",
			body: `{"replace":false}`},
		{method: http.MethodPost, path: "/__desktop/mcp-integrations/claude-code/configure",
			body: `{"replace":false}`},
	}

	for _, route := range routes {
		rec := serveDesktopMCPRequest(
			t, handler, route.method, route.path, route.body,
			desktopTestToken, desktopTestOrigin,
		)
		require.Equal(t, http.StatusNotFound, rec.Code, route.path)
		requireDesktopMCPError(t, rec.Body.String(), "invalid_request", "Invalid request.")
		require.NotContains(t, strings.ToLower(rec.Body.String()), "<html")
	}
}

func TestDesktopMCPMapsServiceErrorsToFixedSafePayloads(t *testing.T) {
	cases := []struct {
		name       string
		path       string
		serviceErr error
		wantStatus int
		wantCode   string
		wantMsg    string
	}{
		{
			name: "write confirmation", path: "/__desktop/mcp-settings",
			serviceErr: mcpsettings.ErrWriteConfirmationRequired,
			wantStatus: http.StatusConflict, wantCode: "write_confirmation_required",
			wantMsg: "Confirm write access before enabling it.",
		},
		{
			name: "MCP disabled", path: "/__desktop/mcp-integrations/codex/configure",
			serviceErr: mcpsettings.ErrMCPDisabled,
			wantStatus: http.StatusConflict, wantCode: "mcp_disabled",
			wantMsg: "MCP is disabled.",
		},
		{
			name: "client command output hidden", path: "/__desktop/mcp-integrations/codex/configure",
			serviceErr: errors.Join(mcpsettings.ErrClientCommand, errors.New("fake CLI output")),
			wantStatus: http.StatusBadGateway, wantCode: "client_command_failed",
			wantMsg: "Client command failed.",
		},
		{
			name: "client not found", path: "/__desktop/mcp-integrations/codex/configure",
			serviceErr: mcpsettings.ErrClientNotFound,
			wantStatus: http.StatusNotFound, wantCode: "client_not_found",
			wantMsg: "Client not found.",
		},
		{
			name: "verification failed", path: "/__desktop/mcp-integrations/codex/configure",
			serviceErr: mcpsettings.ErrVerification,
			wantStatus: http.StatusBadGateway, wantCode: "verification_failed",
			wantMsg: "Configuration verification failed.",
		},
		{
			name: "rollback failed", path: "/__desktop/mcp-integrations/codex/configure",
			serviceErr: mcpsettings.ErrRollbackFailed,
			wantStatus: http.StatusInternalServerError, wantCode: "rollback_failed",
			wantMsg: "Rollback failed.",
		},
		{
			name: "concurrent modification", path: "/__desktop/mcp-integrations/codex/configure",
			serviceErr: mcpsettings.ErrConcurrentModification,
			wantStatus: http.StatusConflict, wantCode: "concurrent_modification",
			wantMsg: "Configuration changed externally.",
		},
		{
			name: "internal integration failure", path: "/__desktop/mcp-integrations/codex/configure",
			serviceErr: errors.Join(
				mcpsettings.ErrIntegration, errors.New("fake CLI output"),
			),
			wantStatus: http.StatusInternalServerError, wantCode: "client_command_failed",
			wantMsg: "Client command failed.",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fake := newFakeDesktopMCPService()
			if tc.path == "/__desktop/mcp-settings" {
				fake.updateErr = tc.serviceErr
			} else {
				fake.configureErr = tc.serviceErr
			}
			rec := serveDesktopMCPRequest(
				t, newDesktopMCPHandler(fake), requestMethodForPath(tc.path), tc.path,
				requestBodyForPath(tc.path), desktopTestToken, desktopTestOrigin,
			)

			require.Equal(t, tc.wantStatus, rec.Code)
			requireDesktopMCPError(t, rec.Body.String(), tc.wantCode, tc.wantMsg)
			require.NotContains(t, rec.Body.String(), "fake CLI output")
		})
	}
}

func TestDesktopMCPResponseStatusesAreLimitedToTheFixedEnum(t *testing.T) {
	statuses := []mcpsettings.Status{
		mcpsettings.StatusClientNotFound,
		mcpsettings.StatusNotConfigured,
		mcpsettings.StatusConfigured,
		mcpsettings.StatusConflict,
		mcpsettings.StatusRepairRequired,
		mcpsettings.StatusError,
	}
	fake := newFakeDesktopMCPService()
	fake.integrations = make([]mcpsettings.Integration, 0, len(statuses))
	for _, status := range statuses {
		fake.integrations = append(fake.integrations, mcpsettings.Integration{
			Client: mcpsettings.ClientCodex, Status: status, Message: "fake CLI output",
		})
	}

	rec := serveDesktopMCPRequest(
		t, newDesktopMCPHandler(fake), http.MethodGet, "/__desktop/mcp-integrations", "",
		desktopTestToken, "",
	)

	require.Equal(t, http.StatusOK, rec.Code)
	for _, status := range statuses {
		require.Contains(t, rec.Body.String(), `"status":"`+string(status)+`"`)
	}
	require.NotContains(t, rec.Body.String(), "fake CLI output")
}

func TestDesktopMCPRejectsUnknownResponseStatusWithoutLeakingMessage(t *testing.T) {
	fake := newFakeDesktopMCPService()
	fake.integrations = []mcpsettings.Integration{{
		Client: mcpsettings.ClientCodex, Status: mcpsettings.Status("future_status"),
		Message: "fake CLI output",
	}}

	rec := serveDesktopMCPRequest(
		t, newDesktopMCPHandler(fake), http.MethodGet, "/__desktop/mcp-integrations", "",
		desktopTestToken, "",
	)

	require.Equal(t, http.StatusBadGateway, rec.Code)
	requireDesktopMCPError(t, rec.Body.String(), "client_command_failed", "Client command failed.")
	require.NotContains(t, rec.Body.String(), "future_status")
	require.NotContains(t, rec.Body.String(), "fake CLI output")
}

type fakeDesktopMCPService struct {
	policy            mcpsettings.Policy
	integrations      []mcpsettings.Integration
	getErr            error
	updateErr         error
	integrationsErr   error
	configureErr      error
	getCalls          int
	updateCalls       int
	integrationsCalls int
	configureCalls    int
	configureClient   mcpsettings.Client
	configureReplace  bool
	updateRequest     mcpsettings.UpdateRequest
}

func newFakeDesktopMCPService() *fakeDesktopMCPService {
	return &fakeDesktopMCPService{
		policy: mcpsettings.Policy{Enabled: true, AllowWrites: false},
		integrations: []mcpsettings.Integration{
			{
				Client: mcpsettings.ClientCodex, Status: mcpsettings.StatusNotConfigured,
				CanConfigure: true, ManualCommand: "codex mcp add",
			},
			{
				Client: mcpsettings.ClientClaudeCode, Status: mcpsettings.StatusConfigured,
				ManualCommand: "claude mcp add",
			},
		},
	}
}

func (fake *fakeDesktopMCPService) Get(context.Context) (mcpsettings.Policy, error) {
	fake.getCalls++
	return fake.policy, fake.getErr
}

func (fake *fakeDesktopMCPService) Update(
	_ context.Context,
	request mcpsettings.UpdateRequest,
) (mcpsettings.Policy, error) {
	fake.updateCalls++
	fake.updateRequest = request
	return fake.policy, fake.updateErr
}

func (fake *fakeDesktopMCPService) Integrations(
	context.Context,
) ([]mcpsettings.Integration, error) {
	fake.integrationsCalls++
	return fake.integrations, fake.integrationsErr
}

func (fake *fakeDesktopMCPService) Configure(
	_ context.Context,
	client mcpsettings.Client,
	replace bool,
) (mcpsettings.Integration, error) {
	fake.configureCalls++
	fake.configureClient = client
	fake.configureReplace = replace
	if len(fake.integrations) == 0 {
		return mcpsettings.Integration{}, fake.configureErr
	}
	return fake.integrations[0], fake.configureErr
}

func newDesktopMCPHandler(service DesktopMCPServicer) http.Handler {
	deps := testDesktopDeps(&DesktopOptions{
		SessionToken: desktopTestToken,
		Origin:       desktopTestOrigin,
		Shutdown:     func() {},
	})
	deps.DesktopMCP = service
	return NewServer(deps)
}

func serveDesktopMCPRequest(
	t *testing.T,
	handler http.Handler,
	method string,
	path string,
	body string,
	token string,
	origin string,
) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(method, desktopTestOrigin+path, strings.NewReader(body))
	req.Host = "127.0.0.1:43127"
	if token != "" {
		req.AddCookie(&http.Cookie{Name: DesktopSessionCookieName, Value: token})
	}
	if origin != "" {
		req.Header.Set("Origin", origin)
	}
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	return rec
}

func requireDesktopMCPError(t *testing.T, body, code, message string) {
	t.Helper()
	require.JSONEq(t, `{"code":`+quoteJSON(code)+`,"message":`+quoteJSON(message)+`}`, body)
}

func quoteJSON(value string) string {
	return `"` + value + `"`
}

func requestMethodForPath(path string) string {
	if path == "/__desktop/mcp-settings" {
		return http.MethodPut
	}
	return http.MethodPost
}

func requestBodyForPath(path string) string {
	if path == "/__desktop/mcp-settings" {
		return `{"enabled":true,"allowWrites":true}`
	}
	return `{"replace":false}`
}
