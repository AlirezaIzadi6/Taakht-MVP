// Command devtoken prints a dev JWT (HS256) accepted by the local Envoy gateway.
//
//	go run ./tools/devtoken user-1            (from the repo root, via go.work)
//	TOKEN=$(cd tools/devtoken && go run . user-1)
//
// The secret and issuer match gateway/envoy.yaml. Dev only.
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
	secret = "taakht-dev-secret-0123456789abcdef"
	issuer = "taakht-dev"
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
	enc := base64.RawURLEncoding
	header, _ := json.Marshal(map[string]string{"alg": "HS256", "typ": "JWT"})
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
