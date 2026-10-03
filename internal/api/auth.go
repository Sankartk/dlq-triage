package api

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/json"
	"net/http"
	"strings"

	"github.com/Sankartk/dlq-triage/internal/config"
)

// Principal is the authenticated caller.
type Principal struct {
	Name string
	Role string
}

func (p Principal) CanOperate() bool { return p.Role == config.RoleOperator }

type principalKey struct{}

// PrincipalFrom returns the caller attached by the auth middleware.
func PrincipalFrom(ctx context.Context) (Principal, bool) {
	p, ok := ctx.Value(principalKey{}).(Principal)
	return p, ok
}

// Authenticator checks bearer tokens in constant time.
type Authenticator struct {
	tokens      []tokenEntry
	allowNoAuth bool
	// OnFailure is called for every rejected request.
	OnFailure func()
}

type tokenEntry struct {
	hash [32]byte
	p    Principal
}

// NewAuthenticator builds an authenticator from configured tokens. Tokens are
// held only as SHA-256 digests, so comparison time does not depend on length.
func NewAuthenticator(tokens []config.Token, allowNoAuth bool) *Authenticator {
	a := &Authenticator{allowNoAuth: allowNoAuth && len(tokens) == 0}
	for _, t := range tokens {
		a.tokens = append(a.tokens, tokenEntry{hash: sha256.Sum256([]byte(t.Token)), p: Principal{Name: t.Name, Role: t.Role}})
	}
	return a
}

// Open reports whether the service runs without authentication.
func (a *Authenticator) Open() bool { return a.allowNoAuth }

// Authenticate resolves a request to a principal.
func (a *Authenticator) Authenticate(r *http.Request) (Principal, bool) {
	if a.allowNoAuth {
		return Principal{Name: "local", Role: config.RoleOperator}, true
	}
	h := r.Header.Get("Authorization")
	token, ok := strings.CutPrefix(h, "Bearer ")
	if !ok || token == "" {
		return Principal{}, false
	}
	sum := sha256.Sum256([]byte(token))
	var found Principal
	matched := 0
	for _, t := range a.tokens {
		// Compare against every token so timing does not reveal which matched.
		m := subtle.ConstantTimeCompare(sum[:], t.hash[:])
		if m == 1 {
			found = t.p
		}
		matched |= m
	}
	return found, matched == 1
}

// Middleware rejects unauthenticated requests and attaches the principal.
func (a *Authenticator) Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		p, ok := a.Authenticate(r)
		if !ok {
			if a.OnFailure != nil {
				a.OnFailure()
			}
			w.Header().Set("WWW-Authenticate", `Bearer realm="dlq-triage"`)
			writeJSON(w, http.StatusUnauthorized, map[string]any{"errors": []map[string]string{{"message": "authentication required"}}})
			return
		}
		next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), principalKey{}, p)))
	})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
