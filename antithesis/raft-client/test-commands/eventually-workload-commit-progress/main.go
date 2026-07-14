// Package main is the eventually-workload-commit-progress test command.
// It is the end-of-timeline liveness check that a cluster which has settled
// after fault injection can still commit a fresh client write — i.e., commit
// progress resumes once a quorum is reachable, which is Raft's actual liveness
// guarantee.
//
// Antithesis dispatches eventually_* commands once near the end of each timeline
// after a fault-quiescent settle window, so by the time this runs the faults
// (kills, partitions, link degradation) are gone. That is what makes a
// no-progress result meaningful: during active faults a stall is expected and
// uninteresting, but a cluster that cannot commit after faults heal is wedged.
//
// Flow:
//
//  1. Gate on cluster health (testutils.WaitForHealthyCluster): every node
//     reachable, all agreeing on the same term, and a single agreed leader.
//     This separates "the cluster is mid-election / partitioned" (inconclusive,
//     not a bug) from "the cluster is healthy but not committing" (a bug). If
//     the gate fails we log a WARN and exit WITHOUT firing any assertion.
//  2. Record the leader's applied_index as a baseline, submit one {"op":"apply"}
//     to the leader, and confirm a fresh entry committed past the baseline,
//     retrying within a budget. A successful apply only returns after the entry
//     committed and applied on the leader, so the ack's `index` is the proof of
//     progress — no follow-up read is required for the verdict.
//
// On observed progress the command fires
//
//	assert.Reachable("[workload-commit-progress] ... advanced applied_index", details)
//
// On retry-budget exhaustion (health passed but no fresh entry committed) it fires
//
//	assert.Unreachable("[workload-commit-progress] ... failed to commit ...", details)
//
// Same slug on both paths so triage pairs the two outcomes as
// one property, mirroring the convergence check. `unreachable` passes if it is never hit, so a
// timeline that never reaches a healthy-but-stalled state does not fail.
//
// Compiles to /usr/local/bin/eventually-workload-commit-progress, installed at
// /opt/antithesis/test/v1/raft/eventually_workload_commit_progress in the
// raft-client container so Antithesis discovers it as an eventually_* command.
package main

import (
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/antithesishq/antithesis-sdk-go/assert"
	"raft-client/test-utils"
)

func main() {
	nodes := flag.String("nodes", "raft-node-0:8400,raft-node-1:8400,raft-node-2:8400",
		"Comma-separated client endpoints (host:port)")
	monitorAddr := flag.String("monitor-addr", "raft-monitor:9000",
		"host:port of the raft-monitor sidecar. Pass empty to disable the monitor sink.")
	// Retry budgets are sized so that, at the 5s attempt interval, the loop that
	// ends in Unreachable spans 120s — long enough for network partitions / node
	// kills injected earlier in the timeline to fully heal before we declare a
	// healthy cluster wedged. progress-attempts is the load-bearing one for that
	// guarantee: 24 attempts at 5s. The health gate gets the same budget so a
	// cluster still re-electing after a fault has time to look healthy and let
	// the progress loop run at all (a health-gate timeout only yields an
	// inconclusive skip, never a false Unreachable).
	healthAttempts := flag.Int("health-attempts", 24,
		"Maximum cluster-health probe attempts before giving up (inconclusive). "+
			"At the 5s interval this lets a post-fault cluster take 120s to settle.")
	progressAttempts := flag.Int("progress-attempts", 24,
		"Maximum apply/commit-progress attempts before firing Unreachable. At the "+
			"5s interval this spans 120s so faults have time to heal first.")
	attemptInterval := flag.Duration("attempt-interval", 5*time.Second,
		"Sleep between attempts, for both the health and progress loops.")
	dialTimeout := flag.Duration("dial-timeout", 1*time.Second,
		"Per-endpoint TCP dial+read timeout for state/find-leader queries.")
	applyTimeout := flag.Duration("apply-timeout", 6*time.Second,
		"Dial+read timeout for the apply call. Must exceed the node's own apply "+
			"timeout (5s) so a slow-but-committing apply is not cut off client-side.")
	flag.Parse()

	endpoints := testutils.SplitCSV(*nodes)
	if len(endpoints) == 0 {
		fmt.Fprintln(os.Stderr, "eventually-workload-commit-progress: at least one endpoint required")
		os.Exit(2)
	}
	if *healthAttempts < 1 || *progressAttempts < 1 {
		fmt.Fprintln(os.Stderr, "eventually-workload-commit-progress: -health-attempts and -progress-attempts must be >= 1")
		os.Exit(2)
	}

	logger := testutils.NewLogger("eventually-workload-commit-progress", *monitorAddr)
	logger.Info("eventually-workload-commit-progress start",
		"endpoints", endpoints,
		"health_attempts", *healthAttempts,
		"progress_attempts", *progressAttempts,
		"attempt_interval", attemptInterval.String(),
		"dial_timeout", dialTimeout.String())

	// 1. Health gate. An unhealthy cluster (mid-election or partitioned) makes
	// a no-progress result inconclusive, not a violation: warn and exit.
	health := testutils.WaitForHealthyCluster(endpoints, *dialTimeout, *attemptInterval, *healthAttempts, logger)
	if !health.Healthy {
		logger.Warn("cluster-health gate failed; skipping the commit-progress check (inconclusive)",
			"attempts_taken", health.AttemptsTaken,
			"per_node", testutils.PerNodeStateMap(health.PerNode))
		return
	}
	logger.Info("cluster healthy",
		"leader", health.LeaderEndpoint,
		"term", health.Term,
		"health_attempts_taken", health.AttemptsTaken)

	// Baseline the leader's applied_index. If the leader vanished between the
	// health gate and now, that is itself inconclusive — bail with a warning.
	baseObs := testutils.QueryState(health.LeaderEndpoint, *dialTimeout)
	if !baseObs.Reachable {
		logger.Warn("leader unreachable immediately after health gate; skipping the commit-progress check (inconclusive)",
			"leader", health.LeaderEndpoint, "err", baseObs.Err)
		return
	}
	baseline := baseObs.AppliedIndex

	// 2. Drive a fresh apply and confirm a fresh entry commits. The apply ack
	// itself is the proof of progress, so a transient failure of any follow-up
	// read can't discard a real success.
	lastLeader := ""
	lastErr := "no attempt reached the apply step"
	for attempt := 1; attempt <= *progressAttempts; attempt++ {
		leaderEP, ok := testutils.FindLeader(endpoints, *dialTimeout)
		if !ok {
			lastErr = "no leader this round"
			logger.Debug("commit-progress attempt: no leader this round", "attempt", attempt)
			if attempt < *progressAttempts {
				time.Sleep(*attemptInterval)
			}
			continue
		}
		lastLeader = leaderEP

		resp, errMsg := testutils.SendLine(leaderEP, *applyTimeout, `{"op":"apply","data":"00000001"}`)
		if errMsg != "" {
			lastErr = "apply: " + errMsg
			logger.Debug("commit-progress attempt: apply send failed",
				"attempt", attempt, "leader", leaderEP, "err", errMsg)
			if attempt < *progressAttempts {
				time.Sleep(*attemptInterval)
			}
			continue
		}
		if ok, _ := resp["ok"].(bool); !ok {
			lastErr = fmt.Sprintf("apply rejected: %v", resp["err"])
			logger.Debug("commit-progress attempt: apply rejected",
				"attempt", attempt, "leader", leaderEP, "err", resp["err"])
			if attempt < *progressAttempts {
				time.Sleep(*attemptInterval)
			}
			continue
		}

		// Apply returned ok, so the entry committed AND applied on the leader's
		// FSM before the future resolved. The ack's `index` is that entry's log
		// index; since the leader applied it, applied_index is now >= index >
		// baseline. Credit progress from the ack directly rather than from a
		// second round-trip that could spuriously fail.
		idxF, hasIdx := resp["index"].(float64)
		if !hasIdx {
			lastErr = "apply ok but response carried no index"
			logger.Debug("commit-progress attempt: apply ok but no index field",
				"attempt", attempt, "leader", leaderEP, "resp", resp)
			if attempt < *progressAttempts {
				time.Sleep(*attemptInterval)
			}
			continue
		}
		committed := uint64(idxF)

		// Best-effort observability read; not required for the verdict.
		finalApplied := committed
		if obs := testutils.QueryState(leaderEP, *dialTimeout); obs.Reachable && obs.AppliedIndex > finalApplied {
			finalApplied = obs.AppliedIndex
		}
		logger.Info("commit-progress confirmed",
			"attempt", attempt,
			"leader", leaderEP,
			"baseline_applied_index", baseline,
			"committed_index", committed,
			"applied_index", finalApplied)
		assert.Reachable(
			"[workload-commit-progress] cluster committed a fresh entry and advanced applied_index",
			detailsMap(attempt, *progressAttempts, baseline, committed, finalApplied, leaderEP, "", health),
		)
		return
	}

	logger.Error("commit-progress give-up; cluster healthy but no fresh entry committed",
		"attempts_taken", *progressAttempts,
		"baseline_applied_index", baseline,
		"last_err", lastErr,
		"per_node", testutils.PerNodeStateMap(health.PerNode))
	assert.Unreachable(
		"[workload-commit-progress] cluster failed to commit a fresh entry within retry budget after health gate passed",
		detailsMap(*progressAttempts, *progressAttempts, baseline, 0, 0, lastLeader, lastErr, health),
	)
}

// detailsMap assembles the map passed to assert.Reachable / assert.Unreachable.
// The shape is identical on both paths so triage joins the success and failure
// dispatches under the same slug. On the success path lastErr is
// empty and committed/finalApplied are set; on give-up they are zero and
// lastErr records why the last attempt failed.
func detailsMap(attempts, maxAttempts int, baseline, committed, finalApplied uint64, leader, lastErr string, health testutils.HealthResult) map[string]any {
	return map[string]any{
		"slug":                   "workload-commit-progress",
		"attempts_taken":         attempts,
		"max_attempts":           maxAttempts,
		"health_attempts_taken":  health.AttemptsTaken,
		"healthy_term":           health.Term,
		"leader":                 leader,
		"baseline_applied_index": baseline,
		"committed_index":        committed,
		"final_applied_index":    finalApplied,
		"last_err":               lastErr,
	}
}
