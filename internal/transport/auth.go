package transport

import (
	"crypto/subtle"
	"net/http"
	"strings"
)

// staticBearerAuth wraps next with HTTP Bearer-token authentication: a request
// is served only when it carries an `Authorization: Bearer <token>` header whose
// token matches want. Any other request is rejected with 401 and a
// WWW-Authenticate challenge — the token gates *reach*, not privilege (spec Q8),
// so this is the single edge that stands between the network and the guarded
// tool surface. The comparison is constant-time to avoid leaking the token by
// timing.
func staticBearerAuth(want string, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		const scheme = "Bearer "
		h := r.Header.Get("Authorization")
		if !strings.HasPrefix(h, scheme) {
			unauthorized(w)
			return
		}
		got := strings.TrimPrefix(h, scheme)
		if subtle.ConstantTimeCompare([]byte(got), []byte(want)) != 1 {
			unauthorized(w)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// unauthorized writes a 401 with the Bearer challenge. The body is deliberately
// terse: it must not echo anything about the expected token.
func unauthorized(w http.ResponseWriter) {
	w.Header().Set("WWW-Authenticate", `Bearer`)
	http.Error(w, "unauthorized", http.StatusUnauthorized)
}
