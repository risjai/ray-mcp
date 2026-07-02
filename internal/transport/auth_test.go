package transport

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

// okHandler is the protected handler: it records that it was reached and writes 200.
func okHandler(reached *bool) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		*reached = true
		w.WriteHeader(http.StatusOK)
	})
}

func TestStaticBearerAuthAcceptsCorrectToken(t *testing.T) {
	var reached bool
	h := staticBearerAuth("s3cret", okHandler(&reached))

	req := httptest.NewRequest(http.MethodPost, "/", nil)
	req.Header.Set("Authorization", "Bearer s3cret")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if !reached {
		t.Fatal("protected handler was not reached with a valid token")
	}
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
}

func TestStaticBearerAuthRejectsMissingHeader(t *testing.T) {
	var reached bool
	h := staticBearerAuth("s3cret", okHandler(&reached))

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/", nil))

	if reached {
		t.Fatal("protected handler was reached without an Authorization header")
	}
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401", rec.Code)
	}
	if got := rec.Header().Get("WWW-Authenticate"); got == "" {
		t.Fatal("401 response is missing the WWW-Authenticate header")
	}
}

func TestStaticBearerAuthRejectsWrongToken(t *testing.T) {
	var reached bool
	h := staticBearerAuth("s3cret", okHandler(&reached))

	req := httptest.NewRequest(http.MethodPost, "/", nil)
	req.Header.Set("Authorization", "Bearer wrong")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if reached {
		t.Fatal("protected handler was reached with a wrong token")
	}
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401", rec.Code)
	}
}

func TestStaticBearerAuthRejectsMalformedHeader(t *testing.T) {
	var reached bool
	h := staticBearerAuth("s3cret", okHandler(&reached))

	// No "Bearer " scheme prefix — the raw token in the header must not pass.
	req := httptest.NewRequest(http.MethodPost, "/", nil)
	req.Header.Set("Authorization", "s3cret")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if reached {
		t.Fatal("protected handler was reached with a malformed (schemeless) header")
	}
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401", rec.Code)
	}
}
