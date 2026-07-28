package api

import (
	"crypto/subtle"
	"net/http"
)

const (
	DesktopSessionCookieName     = "cy_kaf_desktop_session"
	desktopContentSecurityPolicy = "default-src 'self'; base-uri 'self'; object-src 'none'; " +
		"frame-ancestors 'none'; form-action 'self'; script-src 'self' 'unsafe-inline'; " +
		"style-src 'self' 'unsafe-inline'; img-src 'self' data: blob:; font-src 'self' data:; " +
		"connect-src 'self'; worker-src 'self' blob:"
)

type DesktopOptions struct {
	SessionToken string
	Origin       string
	Shutdown     func()
}

func desktopSessionGuard(opts *DesktopOptions, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		setDesktopSecurityHeaders(w.Header())
		if r.URL.Path == "/actuator/health" {
			next.ServeHTTP(w, r)
			return
		}

		cookie, err := r.Cookie(DesktopSessionCookieName)
		if err != nil || opts.SessionToken == "" ||
			subtle.ConstantTimeCompare([]byte(cookie.Value), []byte(opts.SessionToken)) != 1 {
			writeJSON(w, http.StatusUnauthorized, map[string]string{
				"message": "desktop session required",
			})
			return
		}

		if isUnsafeMethod(r.Method) &&
			(opts.Origin == "" || r.Header.Get("Origin") != opts.Origin) {
			writeJSON(w, http.StatusForbidden, map[string]string{
				"message": "desktop origin required",
			})
			return
		}

		next.ServeHTTP(w, r)
	})
}

func setDesktopSecurityHeaders(header http.Header) {
	header.Set("Content-Security-Policy", desktopContentSecurityPolicy)
	header.Set("Referrer-Policy", "no-referrer")
	header.Set("X-Content-Type-Options", "nosniff")
}

func isUnsafeMethod(method string) bool {
	switch method {
	case http.MethodPost, http.MethodPut, http.MethodPatch, http.MethodDelete:
		return true
	default:
		return false
	}
}
