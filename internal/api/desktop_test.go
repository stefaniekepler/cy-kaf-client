package api

import (
	"encoding/base64"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"testing/fstest"

	"github.com/stretchr/testify/require"

	"github.com/cy-kaf/cy-kaf-client/internal/version"
)

const desktopTestOrigin = "http://127.0.0.1:43127"

var desktopTestToken = base64.RawURLEncoding.EncodeToString(
	[]byte("0123456789abcdef0123456789abcdef"),
)

func TestDesktopSessionGuardEnforcesCookieAndWriteOrigin(t *testing.T) {
	opts := &DesktopOptions{
		SessionToken: desktopTestToken,
		Origin:       desktopTestOrigin,
	}
	inner := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	handler := desktopSessionGuard(opts, inner)

	cases := []struct {
		name   string
		method string
		path   string
		cookie string
		origin string
		want   int
	}{
		{name: "health anonymous", method: http.MethodGet, path: "/actuator/health", want: http.StatusOK},
		{name: "business missing cookie", method: http.MethodGet, path: "/api/info", want: http.StatusUnauthorized},
		{name: "business wrong cookie", method: http.MethodGet, path: "/api/info", cookie: "wrong", want: http.StatusUnauthorized},
		{name: "business valid cookie", method: http.MethodGet, path: "/api/info", cookie: desktopTestToken, want: http.StatusOK},
		{name: "write missing origin", method: http.MethodPost, path: "/write", cookie: desktopTestToken, want: http.StatusForbidden},
		{name: "write foreign origin", method: http.MethodPost, path: "/write", cookie: desktopTestToken, origin: "http://127.0.0.1:9", want: http.StatusForbidden},
		{name: "write exact origin", method: http.MethodPost, path: "/write", cookie: desktopTestToken, origin: desktopTestOrigin, want: http.StatusOK},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(tc.method, desktopTestOrigin+tc.path, nil)
			if tc.cookie != "" {
				req.AddCookie(&http.Cookie{Name: DesktopSessionCookieName, Value: tc.cookie})
			}
			if tc.origin != "" {
				req.Header.Set("Origin", tc.origin)
			}
			rec := httptest.NewRecorder()

			handler.ServeHTTP(rec, req)

			require.Equal(t, tc.want, rec.Code)
			require.Contains(t, rec.Header().Get("Content-Security-Policy"), "frame-ancestors 'none'")
			require.Equal(t, "no-referrer", rec.Header().Get("Referrer-Policy"))
			require.Equal(t, "nosniff", rec.Header().Get("X-Content-Type-Options"))
			require.Empty(t, rec.Header().Get("Access-Control-Allow-Origin"))
			require.NotContains(t, rec.Body.String(), desktopTestToken)
		})
	}
}

func TestDesktopSessionGuardRejectsWriteWhenConfiguredOriginIsEmpty(t *testing.T) {
	opts := &DesktopOptions{SessionToken: desktopTestToken}
	handler := desktopSessionGuard(opts, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	req := httptest.NewRequest(http.MethodPost, desktopTestOrigin+"/write", nil)
	req.AddCookie(&http.Cookie{Name: DesktopSessionCookieName, Value: desktopTestToken})
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	require.Equal(t, http.StatusForbidden, rec.Code)
	require.NotContains(t, rec.Body.String(), desktopTestToken)
}

func TestDesktopShutdownIsRegisteredOnlyInDesktopMode(t *testing.T) {
	shutdownCalls := 0
	desktopHandler := NewServer(testDesktopDeps(&DesktopOptions{
		SessionToken: desktopTestToken,
		Origin:       desktopTestOrigin,
		Shutdown:     func() { shutdownCalls++ },
	}))

	missingCookie := desktopRequest(http.MethodPost, "/__desktop/shutdown", "", desktopTestOrigin)
	missingCookieRec := httptest.NewRecorder()
	desktopHandler.ServeHTTP(missingCookieRec, missingCookie)
	require.Equal(t, http.StatusUnauthorized, missingCookieRec.Code)
	require.Zero(t, shutdownCalls)

	wrongOrigin := desktopRequest(http.MethodPost, "/__desktop/shutdown", desktopTestToken, "http://127.0.0.1:9")
	wrongOriginRec := httptest.NewRecorder()
	desktopHandler.ServeHTTP(wrongOriginRec, wrongOrigin)
	require.Equal(t, http.StatusForbidden, wrongOriginRec.Code)
	require.Zero(t, shutdownCalls)

	valid := desktopRequest(http.MethodPost, "/__desktop/shutdown", desktopTestToken, desktopTestOrigin)
	validRec := httptest.NewRecorder()
	desktopHandler.ServeHTTP(validRec, valid)
	require.Equal(t, http.StatusNoContent, validRec.Code)
	require.Equal(t, 1, shutdownCalls)

	cliHandler := NewServer(testDesktopDeps(nil))
	cliReq := desktopRequest(http.MethodPost, "/__desktop/shutdown", desktopTestToken, desktopTestOrigin)
	cliRec := httptest.NewRecorder()
	cliHandler.ServeHTTP(cliRec, cliReq)
	require.Equal(t, http.StatusNotFound, cliRec.Code)
	require.Equal(t, 1, shutdownCalls)
}

func TestDesktopServerProtectsStaticAndAPIWhileHealthStaysMinimal(t *testing.T) {
	handler := NewServer(testDesktopDeps(&DesktopOptions{
		SessionToken: desktopTestToken,
		Origin:       desktopTestOrigin,
		Shutdown:     func() {},
	}))

	for _, path := range []string{"/", "/api/info"} {
		withoutSession := desktopRequest(http.MethodGet, path, "", "")
		withoutSessionRec := httptest.NewRecorder()
		handler.ServeHTTP(withoutSessionRec, withoutSession)
		require.Equal(t, http.StatusUnauthorized, withoutSessionRec.Code, path)

		withSession := desktopRequest(http.MethodGet, path, desktopTestToken, "")
		withSessionRec := httptest.NewRecorder()
		handler.ServeHTTP(withSessionRec, withSession)
		require.Equal(t, http.StatusOK, withSessionRec.Code, path)
	}

	health := desktopRequest(http.MethodGet, "/actuator/health", "", "")
	healthRec := httptest.NewRecorder()
	handler.ServeHTTP(healthRec, health)
	require.Equal(t, http.StatusOK, healthRec.Code)
	require.JSONEq(t, `{"status":"UP"}`, healthRec.Body.String())
}

func TestCLIModeRemainsCookieFree(t *testing.T) {
	handler := NewServer(testDesktopDeps(nil))

	for _, path := range []string{"/", "/api/info"} {
		req := desktopRequest(http.MethodGet, path, "", "")
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		require.Equal(t, http.StatusOK, rec.Code, path)
		require.Empty(t, rec.Header().Get("Content-Security-Policy"))
	}
}

func testDesktopDeps(desktop *DesktopOptions) Deps {
	return Deps{
		IsReadOnly: func(string) bool { return false },
		Build:      version.BuildInfo{Version: "test"},
		Static: fstest.MapFS{
			"index.html": {Data: []byte("<html>PUBLIC-PATH-VARIABLE</html>")},
		},
		Desktop: desktop,
	}
}

func desktopRequest(method, path, token, origin string) *http.Request {
	req := httptest.NewRequest(method, desktopTestOrigin+path, strings.NewReader(""))
	req.Host = "127.0.0.1:43127"
	if token != "" {
		req.AddCookie(&http.Cookie{Name: DesktopSessionCookieName, Value: token})
	}
	if origin != "" {
		req.Header.Set("Origin", origin)
	}
	return req
}
