// Package main is the client harness binary used by the Antithesis test harness.
//
// SETUP-stage scope: connect to each raft-node, wait until the cluster has
// converged to a healthy steady state (full membership, one agreed leader),
// emit setup_complete via the Antithesis lifecycle SDK, then sit idle.
//
// The workload skill will extend this with the actual workload (Apply traffic,
// membership changes, snapshot pressure, Always-class property checks).
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/antithesishq/antithesis-sdk-go/lifecycle"
	"raft-client/test-utils"
)

func main() {
	var (
		nodes      = flag.String("nodes", "", "Comma-separated node client endpoints (host:port)")
		waitFor    = flag.Duration("wait-for-leader", 60*time.Second, "How long to wait for a leader before giving up")
	)
	flag.Parse()

	endpoints := testutils.SplitCSV(*nodes)
	if len(endpoints) == 0 {
		fmt.Fprintln(os.Stderr, "client: -nodes is required")
		os.Exit(2)
	}

	fmt.Printf("client: waiting up to %s for a healthy cluster across %v\n", *waitFor, endpoints)

	// Gate setup_complete on a fully-formed, healthy cluster rather than on
	// "some node reports Leader". The raft-node bootstrap path forms the
	// cluster by electing node-0 as a single-node leader and then adding the
	// other peers as voters; node-0 therefore reports Leader before the peers
	// have joined. Waiting for the healthy predicate (every node reachable,
	// one agreed leader, agreed term) ensures the full 3-voter membership has
	// converged to steady state before any test command runs.
	const interval = 500 * time.Millisecond
	maxAttempts := int(*waitFor / interval)
	if maxAttempts < 1 {
		maxAttempts = 1
	}
	logger := testutils.NewLogger("client-setup", "")
	res := testutils.WaitForHealthyCluster(endpoints, 1*time.Second, interval, maxAttempts, logger)
	if !res.Healthy {
		fmt.Fprintln(os.Stderr, "client: cluster did not reach a healthy steady state before timeout — aborting")
		os.Exit(1)
	}
	fmt.Printf("client: leader=%s term=%d, emitting setup_complete\n", res.LeaderEndpoint, res.Term)

	lifecycle.SetupComplete(map[string]any{
		"leader_endpoint": res.LeaderEndpoint,
		"leader_term":     res.Term,
		"endpoints":       endpoints,
	})

	// Stay alive so Antithesis can dispatch test commands.
	sigctx, sigcancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer sigcancel()
	<-sigctx.Done()
}
