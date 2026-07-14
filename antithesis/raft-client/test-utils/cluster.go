package testutils

import (
	"bufio"
	"encoding/json"
	"fmt"
	"net"
	"time"

	"github.com/hashicorp/go-hclog"
)

// SendLine opens a short-lived TCP connection to a raft-node client endpoint,
// writes one line of JSON, and reads one line of JSON back. It returns the
// decoded response on success, or a structured error string (via
// ClassifyDialErr / ClassifyReadErr) classifying the network or protocol
// failure mode. Shared by every harness binary that speaks the client API.
func SendLine(endpoint string, timeout time.Duration, req string) (map[string]any, string) {
	conn, err := net.DialTimeout("tcp", endpoint, timeout)
	if err != nil {
		return nil, ClassifyDialErr(err)
	}
	defer conn.Close()

	if _, err := fmt.Fprintln(conn, req); err != nil {
		return nil, "write-failed: " + err.Error()
	}

	_ = conn.SetReadDeadline(time.Now().Add(timeout + 1*time.Second))
	scan := bufio.NewScanner(conn)
	scan.Buffer(make([]byte, 4096), 1<<20)
	if !scan.Scan() {
		if err := scan.Err(); err != nil {
			return nil, ClassifyReadErr(err)
		}
		return nil, "read-eof: connection closed before response"
	}

	var resp map[string]any
	if err := json.Unmarshal(scan.Bytes(), &resp); err != nil {
		return nil, "decode-failed: " + err.Error()
	}
	return resp, ""
}

// NodeState is one node's reply to {"op":"state"}. Reachable is false when the
// query failed, in which case only Err is populated; the remaining fields are
// meaningful only when Reachable is true.
type NodeState struct {
	Endpoint     string
	Reachable    bool
	State        string // "Leader" | "Follower" | "Candidate"
	Term         uint64
	LeaderID     string
	LeaderAddr   string
	LastIndex    uint64
	AppliedIndex uint64
	Err          string
}

// QueryState issues {"op":"state"} against one endpoint and decodes the reply
// into a NodeState.
func QueryState(endpoint string, timeout time.Duration) NodeState {
	ns := NodeState{Endpoint: endpoint}
	resp, errMsg := SendLine(endpoint, timeout, `{"op":"state"}`)
	if errMsg != "" {
		ns.Err = errMsg
		return ns
	}
	ns.State, _ = resp["state"].(string)
	ns.LeaderID, _ = resp["leader_id"].(string)
	ns.LeaderAddr, _ = resp["leader_addr"].(string)
	if t, ok := resp["term"].(float64); ok {
		ns.Term = uint64(t)
	}
	if l, ok := resp["last_index"].(float64); ok {
		ns.LastIndex = uint64(l)
	}
	if a, ok := resp["applied_index"].(float64); ok {
		ns.AppliedIndex = uint64(a)
	}
	ns.Reachable = true
	return ns
}

// FindLeader polls every endpoint once and returns the endpoint of the node
// currently reporting state=="Leader". The second return is false if no
// reachable node claims leadership this round.
func FindLeader(endpoints []string, timeout time.Duration) (string, bool) {
	for _, ep := range endpoints {
		ns := QueryState(ep, timeout)
		if ns.Reachable && ns.State == "Leader" {
			return ep, true
		}
	}
	return "", false
}

// PerNodeStateMap renders a set of observations for a logger field or an
// assertion details map. Unreachable nodes carry only reachable:false and the
// error reason; reachable nodes carry the full state tuple.
func PerNodeStateMap(perNode map[string]NodeState) map[string]any {
	out := make(map[string]any, len(perNode))
	for ep, ns := range perNode {
		if !ns.Reachable {
			out[ep] = map[string]any{"reachable": false, "err": ns.Err}
			continue
		}
		out[ep] = map[string]any{
			"reachable":     true,
			"state":         ns.State,
			"term":          ns.Term,
			"leader_id":     ns.LeaderID,
			"last_index":    ns.LastIndex,
			"applied_index": ns.AppliedIndex,
		}
	}
	return out
}

// HealthResult is the outcome of WaitForHealthyCluster.
type HealthResult struct {
	Healthy        bool
	AttemptsTaken  int
	Term           uint64               // the term all nodes agreed on (when Healthy)
	LeaderEndpoint string               // endpoint of the sole node reporting Leader (when Healthy)
	PerNode        map[string]NodeState // observations from the final attempt
}

// WaitForHealthyCluster polls every endpoint with {"op":"state"} until the
// cluster looks healthy and well-connected, or the retry budget is exhausted.
//
// Healthy, in a single attempt, means: every endpoint is reachable, every node
// reports the same term, exactly one node reports state=="Leader", and every
// node agrees on that leader's id. This is the precondition for the
// end-of-timeline liveness commands (commit-progress, convergence): it keeps
// them from mistaking an in-progress election or a partitioned cluster for a
// liveness violation. Reachability alone is insufficient — a cluster mid-
// election has every node reachable yet no stable leader and disagreeing terms,
// and a stall there is expected rather than a bug.
//
// On success the caller proceeds with its property check; on failure the caller
// should log a warning and exit without firing any assertion (an inconclusive
// timeline, not a violation).
func WaitForHealthyCluster(
	endpoints []string,
	dialTimeout, interval time.Duration,
	maxAttempts int,
	logger hclog.Logger,
) HealthResult {
	var res HealthResult
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		perNode := make(map[string]NodeState, len(endpoints))
		for _, ep := range endpoints {
			perNode[ep] = QueryState(ep, dialTimeout)
		}
		res.PerNode = perNode
		res.AttemptsTaken = attempt

		term, leaderEP, ok := evalHealth(perNode, len(endpoints))
		logger.Debug("cluster-health attempt",
			"attempt", attempt,
			"healthy", ok,
			"term", term,
			"leader", leaderEP,
			"per_node", PerNodeStateMap(perNode))
		if ok {
			res.Healthy = true
			res.Term = term
			res.LeaderEndpoint = leaderEP
			return res
		}
		if attempt < maxAttempts {
			time.Sleep(interval)
		}
	}
	return res
}

// evalHealth evaluates the health predicate for one attempt, returning the
// agreed term and the leader endpoint when healthy.
func evalHealth(perNode map[string]NodeState, total int) (uint64, string, bool) {
	live := 0
	var term uint64
	termSet := false
	leaderEP := ""
	leaders := 0
	leaderID := ""
	for _, ns := range perNode {
		if !ns.Reachable {
			return 0, "", false // every node must be reachable
		}
		live++
		if !termSet {
			term, termSet = ns.Term, true
		} else if ns.Term != term {
			return 0, "", false // term disagreement => mid-election / unstable
		}
		if ns.State == "Leader" {
			leaders++
			leaderEP = ns.Endpoint
		}
	}
	if live != total || leaders != 1 {
		return 0, "", false // need every node reachable and exactly one leader
	}
	// Every node must agree on who the leader is.
	for _, ns := range perNode {
		if ns.LeaderID == "" {
			return 0, "", false
		}
		if leaderID == "" {
			leaderID = ns.LeaderID
		} else if ns.LeaderID != leaderID {
			return 0, "", false
		}
	}
	return term, leaderEP, true
}
