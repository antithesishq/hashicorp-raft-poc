// Package main is the eventually-fsm-convergence test command. It polls every
// raft-node's client endpoint and checks that every reachable node has
// converged on the same applied_index and FSM state (XOR-hash and block
// count). Antithesis
// dispatches eventually_* commands during the end-of-timeline quiescence window
// after a fault-quiescent settle window, and may run them repeatedly as the
// system settles, treating the result as a liveness property that must
// eventually hold. This command contains its own retry loop (default 24 attempts
// at 5s intervals, so the give-up-and-fire-Unreachable path spans 120s) for the
// cluster to settle after injected faults heal.
//
// On convergence the command fires
//
//	assert.Reachable("[fsm-convergence] all live nodes converged ...", details)
//
// and exits. On retry budget exhaustion it fires
//
//	assert.Unreachable("[fsm-convergence] cluster failed to converge ...", details)
//
// and exits. Same slug on both paths so triage pairs the two
// dispatch outcomes as one property.
//
// Before checking convergence the command gates on cluster health
// (testutils.WaitForHealthyCluster): every node reachable, all agreeing on the
// same term, and a single agreed leader. If that gate fails — the cluster is
// mid-election or partitioned — a non-convergence result is inconclusive rather
// than a violation, so the command logs a WARN and exits without firing the
// property. The Unreachable verdict therefore fires only when the cluster was
// healthy yet still failed to converge.
//
// The convergence predicate requires a strict majority of endpoints reachable,
// every reachable node reporting the same applied_index AND the same FSM state
// (XOR-hash and block count), and applied_index > 0 (the last clause excludes
// the trivial pre-workload baseline where every node is "converged" at index 0).
//
// Compiles to /usr/local/bin/eventually-fsm-convergence, installed at
// /opt/antithesis/test/v1/raft/eventually_fsm_convergence in the raft-client
// container so Antithesis discovers it as an eventually_* test command.
//
// Logging mirrors parallel-driver-apply: stderr sees INFO+; the raft-monitor
// sidecar receives every record (DEBUG+) as JSON via an hclog InterceptLogger
// sink. The monitor has no consumer for this property (the check is workload-
// side), but the records are useful for triage.
package main

import (
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/antithesishq/antithesis-sdk-go/assert"
	"raft-client/test-utils"
)

// nodeObservation is one poll result for a single endpoint. Reachable is the
// boolean predicate the convergence check uses; the remaining fields are
// populated only when Reachable is true. Err captures the failure mode for
// unreachable endpoints so triage can distinguish dial-refused (node dead)
// from dial-timeout (partition) from decode-failed (protocol drift).
type nodeObservation struct {
	Reachable    bool
	State        string
	Term         uint64
	AppliedIndex uint64
	LastIndex    uint64
	Hash         string // FSM XOR-hash as a hex string
	Count        uint64 // number of blocks applied to the FSM
	Err          string
}

func main() {
	nodes := flag.String("nodes", "raft-node-0:8400,raft-node-1:8400,raft-node-2:8400",
		"Comma-separated client endpoints (host:port)")
	monitorAddr := flag.String("monitor-addr", "raft-monitor:9000",
		"host:port of the raft-monitor sidecar. Pass empty to disable the monitor sink.")
	// max-attempts governs BOTH the health gate and the convergence loop. At the
	// 5s interval, 24 attempts makes the convergence loop that ends in
	// Unreachable span 120s, giving network partitions / node kills injected
	// earlier in the timeline time to heal before we call the cluster
	// non-convergent; the health gate gets the same budget so a still-settling
	// cluster can become healthy and let the convergence loop run at all.
	maxAttempts := flag.Int("max-attempts", 24,
		"Maximum polling attempts (health gate + convergence loop) before giving "+
			"up and firing Unreachable. At the 5s interval this spans 120s.")
	attemptInterval := flag.Duration("attempt-interval", 5*time.Second,
		"Sleep between polling attempts.")
	dialTimeout := flag.Duration("dial-timeout", 1*time.Second,
		"Per-endpoint TCP dial+read timeout.")
	flag.Parse()

	endpoints := testutils.SplitCSV(*nodes)
	if len(endpoints) == 0 {
		fmt.Fprintln(os.Stderr, "eventually-fsm-convergence: at least one endpoint required")
		os.Exit(2)
	}
	if *maxAttempts < 1 {
		fmt.Fprintln(os.Stderr, "eventually-fsm-convergence: -max-attempts must be >= 1")
		os.Exit(2)
	}

	logger := testutils.NewLogger("eventually-fsm-convergence", *monitorAddr)
	logger.Info("eventually-fsm-convergence start",
		"endpoints", endpoints,
		"max_attempts", *maxAttempts,
		"attempt_interval", attemptInterval.String(),
		"dial_timeout", dialTimeout.String())

	// Health gate. A cluster that is mid-election or partitioned makes a
	// non-convergence result inconclusive, not a violation: warn and exit
	// without firing the property.
	health := testutils.WaitForHealthyCluster(endpoints, *dialTimeout, *attemptInterval, *maxAttempts, logger)
	if !health.Healthy {
		logger.Warn("cluster-health gate failed; skipping the convergence check (inconclusive)",
			"attempts_taken", health.AttemptsTaken,
			"per_node", testutils.PerNodeStateMap(health.PerNode))
		return
	}
	logger.Info("cluster healthy",
		"leader", health.LeaderEndpoint,
		"term", health.Term,
		"health_attempts_taken", health.AttemptsTaken)

	var obs map[string]*nodeObservation
	for attempt := 1; attempt <= *maxAttempts; attempt++ {
		obs = pollAll(endpoints, *dialTimeout)
		logger.Debug("convergence-check attempt",
			"attempt", attempt,
			"per_node", perNodeMap(obs))

		if converged(obs, len(endpoints)) {
			any := firstLive(obs)
			logger.Info("convergence-check converged",
				"attempts_taken", attempt,
				"applied_index", any.AppliedIndex,
				"hash", any.Hash,
				"count", any.Count)
			assert.Reachable(
				"[fsm-convergence] all live nodes converged on the same applied_index and FSM state",
				detailsMap(attempt, *maxAttempts, *attemptInterval, obs),
			)
			return
		}
		if attempt < *maxAttempts {
			time.Sleep(*attemptInterval)
		}
	}

	logger.Error("convergence-check give-up; cluster healthy but did not converge",
		"attempts_taken", *maxAttempts,
		"per_node", perNodeMap(obs))

	assert.Unreachable(
		"[fsm-convergence] cluster failed to converge within retry budget",
		detailsMap(*maxAttempts, *maxAttempts, *attemptInterval, obs),
	)
}

// pollAll queries every endpoint once and returns one observation per
// endpoint. Endpoints that fail to dial or respond appear with Reachable=false
// and a populated Err field. Sequential rather than parallel — the per-attempt
// dial timeout is small and 3-node fan-out is cheap; parallel polling would
// just add goroutine bookkeeping for no real wall-clock saving.
func pollAll(endpoints []string, dialTimeout time.Duration) map[string]*nodeObservation {
	out := make(map[string]*nodeObservation, len(endpoints))
	for _, ep := range endpoints {
		out[ep] = queryNode(ep, dialTimeout)
	}
	return out
}

// queryNode issues `{"op":"state"}` and `{"op":"read"}` against a single
// endpoint and merges the responses into one observation. Either request
// failing marks the observation unreachable — a partial reply (state ok but
// read failed, say) is not enough for a convergence claim against this node.
func queryNode(endpoint string, dialTimeout time.Duration) *nodeObservation {
	o := &nodeObservation{}

	ns := testutils.QueryState(endpoint, dialTimeout)
	if !ns.Reachable {
		o.Err = "state: " + ns.Err
		return o
	}
	readResp, errMsg := testutils.SendLine(endpoint, dialTimeout, `{"op":"read"}`)
	if errMsg != "" {
		o.Err = "read: " + errMsg
		return o
	}

	o.State = ns.State
	o.Term = ns.Term
	o.AppliedIndex = ns.AppliedIndex
	o.LastIndex = ns.LastIndex
	if h, ok := readResp["hash"].(string); ok {
		o.Hash = h
	}
	if c, ok := readResp["count"].(float64); ok {
		o.Count = uint64(c)
	}
	o.Reachable = true
	return o
}

// converged returns true iff a strict majority of endpoints is reachable,
// every reachable endpoint reports the same (applied_index, hash, count), and
// the common applied_index is greater than zero. The applied_index>0 clause
// keeps the property honest before any workload has touched the cluster.
func converged(obs map[string]*nodeObservation, total int) bool {
	var first *nodeObservation
	live := 0
	for _, o := range obs {
		if !o.Reachable {
			continue
		}
		live++
		if first == nil {
			first = o
			continue
		}
		if o.AppliedIndex != first.AppliedIndex || o.Hash != first.Hash || o.Count != first.Count {
			return false
		}
	}
	if live*2 <= total {
		return false
	}
	if first == nil || first.AppliedIndex == 0 {
		return false
	}
	return true
}

// firstLive returns any one reachable observation, used for logging the
// converged-on values once we know they all agree. Callers must ensure at
// least one observation is reachable.
func firstLive(obs map[string]*nodeObservation) *nodeObservation {
	for _, o := range obs {
		if o.Reachable {
			return o
		}
	}
	return nil
}

// perNodeMap returns a serializable representation of every observation,
// suitable for inclusion in a logger field or assertion details map.
// Unreachable nodes carry only `reachable:false` and the error reason; live
// nodes carry the full state/value tuple.
func perNodeMap(obs map[string]*nodeObservation) map[string]any {
	out := make(map[string]any, len(obs))
	for ep, o := range obs {
		if !o.Reachable {
			out[ep] = map[string]any{
				"reachable": false,
				"err":       o.Err,
			}
			continue
		}
		out[ep] = map[string]any{
			"reachable":     true,
			"state":         o.State,
			"term":          o.Term,
			"applied_index": o.AppliedIndex,
			"last_index":    o.LastIndex,
			"hash":          o.Hash,
			"count":         o.Count,
		}
	}
	return out
}

// detailsMap assembles the map passed to assert.Reachable / assert.Unreachable.
// The shape is identical on both paths so triage can join the success and
// failure dispatches under the same slug and read the per_node
// observations from the same field name.
func detailsMap(attempts, maxAttempts int, interval time.Duration, obs map[string]*nodeObservation) map[string]any {
	return map[string]any{
		"slug":                "fsm-convergence",
		"attempts_taken":      attempts,
		"max_attempts":        maxAttempts,
		"attempt_interval_ms": interval.Milliseconds(),
		"per_node":            perNodeMap(obs),
	}
}
