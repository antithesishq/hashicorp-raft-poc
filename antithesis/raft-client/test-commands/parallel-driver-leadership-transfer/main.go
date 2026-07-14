// Package main is the parallel_driver_leadership_transfer test command.
// Each invocation picks a random non-leader target, sends a
// transfer_leadership op to the current leader (blocking until the transfer
// completes or aborts), and emits `transfer-start` / `transfer-end` hclog
// records that bracket the transfer window. Leadership transfers exercise
// election churn as a workload driver; the bracket markers remain available
// for triage.
//
// Antithesis re-dispatches parallel_driver_* commands repeatedly throughout
// a timeline; we don't run our own loop.
//
// Compiles to /usr/local/bin/parallel-driver-leadership-transfer, installed
// at /opt/antithesis/test/v1/raft/parallel_driver_leadership_transfer in the
// raft-client container.
//
// Logging: every dispatch emits a `transfer-start` record at the moment the
// transfer request is sent to the leader, and a `transfer-end` record once
// the leader's future returns (success or any error). Both records carry
// {source_id, target_id, attempt_id}; `transfer-end` adds {ok, err}. All
// records are emitted at DEBUG through an hclog.InterceptLogger. Stderr
// sees INFO+ only and the raft-monitor sidecar receives every record as
// JSON via a DEBUG sink — the same fan-out pattern raft-node and
// parallel-driver-apply use. Set -monitor-addr to override the default
// `raft-monitor:9000` or pass an empty value to skip the sink. A failed
// dial silently drops the records; the consumer falls back to the cluster's
// own state-transition stream and will only miss the per-attempt window
// brackets for that one dispatch.
package main

import (
	"bufio"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"net"
	"time"

	sdkrand "github.com/antithesishq/antithesis-sdk-go/random"
	"github.com/hashicorp/go-hclog"
	"raft-client/test-utils"
)

// peer is a Raft cluster member as the client sees it.
type peer struct {
	ID         string
	ClientAddr string // host:port for the JSON-over-TCP client API
	RaftAddr   string // host:port for Raft RPCs (used as target address)
}

// allPeers matches docker-compose.yaml. Hard-coded for the harness; if the
// topology grows beyond 3 nodes, plumb this through flags.
var allPeers = []peer{
	{ID: "node-0", ClientAddr: "raft-node-0:8400", RaftAddr: "raft-node-0:8300"},
	{ID: "node-1", ClientAddr: "raft-node-1:8400", RaftAddr: "raft-node-1:8300"},
	{ID: "node-2", ClientAddr: "raft-node-2:8400", RaftAddr: "raft-node-2:8300"},
}

func main() {
	var (
		transferTimeout = flag.Duration("transfer-timeout", 30*time.Second,
			"Hard cap on a single transfer attempt (covers cases where the future never returns)")
		monitorAddr = flag.String("monitor-addr", "raft-monitor:9000",
			"host:port of the raft-monitor sidecar. Pass empty to disable the monitor sink.")
	)
	flag.Parse()

	logger := testutils.NewLogger("parallel-driver-leadership-transfer", *monitorAddr)

	// Outer bound: transferTimeout for the request + small buffer for teardown.
	ctx, cancel := context.WithTimeout(context.Background(), *transferTimeout+5*time.Second)
	defer cancel()

	triggerOne(ctx, *transferTimeout, logger)
}

// triggerOne runs a single transfer attempt.
func triggerOne(ctx context.Context, timeout time.Duration, logger hclog.Logger) {
	// Find the current leader by querying everyone. Skip the transfer if no
	// leader is reachable.
	currentLeader := findLeader(ctx)
	if currentLeader == nil {
		fmt.Println("driver-leadership-transfer: no leader observed; skipping cycle")
		return
	}

	// Pick a random non-leader target using Antithesis's deterministic RNG.
	var candidates []peer
	for _, p := range allPeers {
		if p.ID != currentLeader.ID {
			candidates = append(candidates, p)
		}
	}
	if len(candidates) == 0 {
		return
	}
	target := candidates[int(sdkrand.GetRandom()%uint64(len(candidates)))]

	// attempt_id ties the start/end pair together in the monitor stream even
	// if multiple dispatches land in close succession.
	attemptID := sdkrand.GetRandom()

	fmt.Printf("driver-leadership-transfer: attempting transfer leader=%s -> target=%s\n",
		currentLeader.ID, target.ID)

	logger.Debug("transfer-start",
		"source_id", currentLeader.ID,
		"target_id", target.ID,
		"attempt_id", attemptID,
	)

	txCtx, txCancel := context.WithTimeout(ctx, timeout)
	defer txCancel()
	err := sendTransfer(txCtx, currentLeader.ClientAddr, target.ID, target.RaftAddr)

	endFields := []any{
		"source_id", currentLeader.ID,
		"target_id", target.ID,
		"attempt_id", attemptID,
		"ok", err == nil,
	}
	if err != nil {
		endFields = append(endFields, "err", err.Error())
	}
	logger.Debug("transfer-end", endFields...)

	if err != nil {
		fmt.Printf("driver-leadership-transfer: transfer failed (acceptable): %v\n", err)
		return
	}
	fmt.Printf("driver-leadership-transfer: transfer succeeded leader=%s -> target=%s\n",
		currentLeader.ID, target.ID)
}

// findLeader queries all peers concurrently and returns the one currently
// reporting Leader, or nil if none. Picks the first observed; in the rare
// case of two reporting Leader we still pick one and proceed — raft-monitor
// observes the cluster's own state-transition stream and will surface any
// safety violation independently.
func findLeader(ctx context.Context) *peer {
	type result struct {
		idx       int
		state     string
		reachable bool
	}
	ch := make(chan result, len(allPeers))
	for i, p := range allPeers {
		go func(i int, p peer) {
			s, ok := queryState(ctx, p.ClientAddr)
			ch <- result{idx: i, state: s, reachable: ok}
		}(i, p)
	}
	for range allPeers {
		r := <-ch
		if r.reachable && r.state == "Leader" {
			peerCopy := allPeers[r.idx]
			return &peerCopy
		}
	}
	return nil
}

// sendTransfer opens a TCP connection to the leader and sends a
// transfer_leadership op. Blocks until the response or context cancellation.
func sendTransfer(ctx context.Context, leaderClientAddr, targetID, targetAddr string) error {
	dialer := net.Dialer{Timeout: 2 * time.Second}
	conn, err := dialer.DialContext(ctx, "tcp", leaderClientAddr)
	if err != nil {
		return err
	}
	defer conn.Close()

	// Wire the context-cancel to close the connection so the read unblocks.
	go func() {
		<-ctx.Done()
		_ = conn.Close()
	}()

	req := map[string]any{
		"op":   "transfer_leadership",
		"id":   targetID,
		"addr": targetAddr,
	}
	if err := json.NewEncoder(conn).Encode(req); err != nil {
		return err
	}

	scan := bufio.NewScanner(conn)
	scan.Buffer(make([]byte, 4096), 1<<20)
	if !scan.Scan() {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return fmt.Errorf("no response from leader")
	}
	var resp map[string]any
	if err := json.Unmarshal(scan.Bytes(), &resp); err != nil {
		return err
	}
	if ok, _ := resp["ok"].(bool); !ok {
		errMsg, _ := resp["err"].(string)
		return fmt.Errorf("transfer rejected: %s", errMsg)
	}
	return nil
}

// queryState dials a raft-node client endpoint and asks for its state.
// Used only to pick a transfer source; the consumer reads role transitions
// directly from the hclog stream.
func queryState(ctx context.Context, endpoint string) (string, bool) {
	dialer := net.Dialer{Timeout: 500 * time.Millisecond}
	conn, err := dialer.DialContext(ctx, "tcp", endpoint)
	if err != nil {
		return "", false
	}
	defer conn.Close()

	if _, err := fmt.Fprintln(conn, `{"op":"state"}`); err != nil {
		return "", false
	}

	_ = conn.SetReadDeadline(time.Now().Add(1 * time.Second))
	scan := bufio.NewScanner(conn)
	scan.Buffer(make([]byte, 4096), 1<<20)
	if !scan.Scan() {
		return "", false
	}
	var resp map[string]any
	if err := json.Unmarshal(scan.Bytes(), &resp); err != nil {
		return "", false
	}
	s, _ := resp["state"].(string)
	return s, true
}
