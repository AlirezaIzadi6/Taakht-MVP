// Command devtoken prints a dev JWT (HS256) accepted by the local Envoy gateway.
//
//	go run ./tools/devtoken user-1            (from the repo root, via go.work)
//	TOKEN=$(cd tools/devtoken && go run . user-1)
//
// The signing key comes from JWT_SIGNING_KEY (default: the public dev key, the same default as
// scripts/gen-envoy-config.sh, which publishes it to Envoy as deploy/envoy/jwks.json). JWT_KEY_ID names the
// key in the token header (default dev-1) so Envoy can pick it from a multi-key set during rotation.
// The issuer and audience match gateway/envoy.yaml. Dev only.
package main

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"
)

const (
	// defaultSecret is the DEV ONLY key, public in the repo. Override with JWT_SIGNING_KEY.
	defaultSecret = "taakht-dev-secret-0123456789abcdef"
	defaultKeyID  = "dev-1"
	issuer        = "taakht-dev"
	// audience must match the jwt_authn provider's audiences in gateway/envoy.yaml.
	audience = "taakht-api"
)

func main() {
	if len(os.Args) != 2 {
		fmt.Fprintln(os.Stderr, "usage: devtoken <user-id>   (seed users: user-1 .. user-4)")
		os.Exit(2)
	}
	if strings.HasPrefix(os.Args[1], "system:") {
		fmt.Fprintln(os.Stderr, "devtoken: the system: prefix is reserved for services and is never minted")
		os.Exit(2)
	}
	secret := envOr("JWT_SIGNING_KEY", defaultSecret)
	enc := base64.RawURLEncoding
	header, _ := json.Marshal(map[string]string{"alg": "HS256", "typ": "JWT", "kid": envOr("JWT_KEY_ID", defaultKeyID)})
	claims, _ := json.Marshal(map[string]any{
		"iss": issuer,
		"aud": audience,
		"sub": os.Args[1],
		"iat": time.Now().Unix(),
		"exp": time.Now().Add(24 * time.Hour).Unix(),
	})
	signingInput := enc.EncodeToString(header) + "." + enc.EncodeToString(claims)
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(signingInput))
	fmt.Println(signingInput + "." + enc.EncodeToString(mac.Sum(nil)))
}

func envOr(name, def string) string {
	if v := os.Getenv(name); v != "" {
		return v
	}
	return def
}
