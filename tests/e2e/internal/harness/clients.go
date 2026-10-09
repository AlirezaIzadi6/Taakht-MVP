// Package harness drives the whole Taakht system over gRPC for end-to-end tests and the demo scenario.
package harness

import (
	"context"
	"fmt"
	"net"
	"os"
	"time"

	adv1 "github.com/taakht/taakht/gen/taakht/ad/v1"
	matchingv1 "github.com/taakht/taakht/gen/taakht/matching/v1"
	negotiationv1 "github.com/taakht/taakht/gen/taakht/negotiation/v1"
	swapv1 "github.com/taakht/taakht/gen/taakht/swap/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
)

// Addrs holds the gRPC address of every service.
type Addrs struct {
	Ad, Matching, Negotiation, Swap string
}

// AddrsFromEnv reads AD_ADDR, MATCHING_ADDR, NEGOTIATION_ADDR and SWAP_ADDR with the local defaults.
func AddrsFromEnv() Addrs {
	return Addrs{
		Ad:          envOr("AD_ADDR", "127.0.0.1:9001"),
		Matching:    envOr("MATCHING_ADDR", "127.0.0.1:9002"),
		Negotiation: envOr("NEGOTIATION_ADDR", "127.0.0.1:9003"),
		Swap:        envOr("SWAP_ADDR", "127.0.0.1:9004"),
	}
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// Clients bundles the typed clients of all four services.
type Clients struct {
	Addrs       Addrs
	Ad          adv1.AdServiceClient
	Matching    matchingv1.MatchingServiceClient
	Negotiation negotiationv1.NegotiationServiceClient
	Swap        swapv1.SwapServiceClient

	conns []*grpc.ClientConn
}

// Dial creates the clients. Connections are lazy, so this succeeds even when a service is down.
func Dial(a Addrs) (*Clients, error) {
	c := &Clients{Addrs: a}
	dial := func(addr string) (*grpc.ClientConn, error) {
		conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return nil, fmt.Errorf("dial %s: %w", addr, err)
		}
		c.conns = append(c.conns, conn)
		return conn, nil
	}
	conn, err := dial(a.Ad)
	if err != nil {
		return nil, err
	}
	c.Ad = adv1.NewAdServiceClient(conn)
	if conn, err = dial(a.Matching); err != nil {
		return nil, err
	}
	c.Matching = matchingv1.NewMatchingServiceClient(conn)
	if conn, err = dial(a.Negotiation); err != nil {
		return nil, err
	}
	c.Negotiation = negotiationv1.NewNegotiationServiceClient(conn)
	if conn, err = dial(a.Swap); err != nil {
		return nil, err
	}
	c.Swap = swapv1.NewSwapServiceClient(conn)
	return c, nil
}

// Close releases all connections.
func (c *Clients) Close() {
	for _, conn := range c.conns {
		_ = conn.Close()
	}
}

// Unreachable lists the named services ("ad", "matching", "negotiation", "swap") that do not accept TCP connections.
func (c *Clients) Unreachable(services ...string) []string {
	addrs := map[string]string{
		"ad": c.Addrs.Ad, "matching": c.Addrs.Matching,
		"negotiation": c.Addrs.Negotiation, "swap": c.Addrs.Swap,
	}
	var down []string
	for _, s := range services {
		dialCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		conn, err := (&net.Dialer{}).DialContext(dialCtx, "tcp", addrs[s])
		cancel()
		if err != nil {
			down = append(down, fmt.Sprintf("%s (%s)", s, addrs[s]))
			continue
		}
		_ = conn.Close()
	}
	return down
}

// AsUser returns a context carrying the x-user-id metadata the services use for identity.
func AsUser(ctx context.Context, userID string) context.Context {
	return metadata.AppendToOutgoingContext(ctx, "x-user-id", userID)
}
