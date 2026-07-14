// Package main is the parallel-driver-apply test command. Each invocation
// submits N Apply operations (N chosen via the SDK RNG between 1 and 10) to
// raft-node endpoints. The first endpoint is picked via the SDK RNG; on
// success the next iteration reuses the same endpoint, on failure the next
// iteration rotates to the next endpoint in round-robin order. Antithesis
// re-dispatches parallel_driver_* commands repeatedly throughout a timeline
// — we don't run our own outer loop.
//
// Compiles to /usr/local/bin/parallel-driver-apply, installed at
// /opt/antithesis/test/v1/raft/parallel_driver_apply in the raft-client
// container so Antithesis discovers it as a parallel_driver_ test command.
//
// Logging: every iteration emits an `apply-request sent` record, then either
// `apply-request success` (the leader's Apply future returned with no error
// — the entry was committed to a majority and applied to the leader's FSM;
// followers apply asynchronously) or `apply-request error` with a structured
// `reason` field (dial-timeout, dial-refused, write-failed, read-timeout,
// decode-failed, the server's err string verbatim, ...). The reason
// distinguishes network-layer failures (partition / dead node) from
// server-reported errors. Note: a `success` guarantees commit; an `error`
// does NOT guarantee the entry was not applied (e.g.
// `leadership lost while committing log` can occur after the entry already
// replicated to a majority — see hashicorp/raft commit semantics).
//
// All sent/success/error records are emitted at DEBUG level through an
// hclog.InterceptLogger. Stderr sees INFO+ only (so the routine stream stays
// quiet) and the raft-monitor sidecar receives every record as JSON via a
// DEBUG sink, in the same way raft-node does. Set -monitor-addr to override
// the default `raft-monitor:9000` or pass an empty value to skip the sink.
// A failed dial silently drops the records — these are short-lived dispatches
// and the next one will re-establish the connection.
package main

import (
	"bufio"
	"context"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"net"
	"os"
	"time"

	sdkrand "github.com/antithesishq/antithesis-sdk-go/random"
	"raft-client/test-utils"
)

// blockSize is the fixed size, in bytes, of every "Chain of Blocks" block —
// must match the raft-node FSM's blockSize.
const blockSize = 4

func main() {
	nodes := flag.String("nodes", "raft-node-0:8400,raft-node-1:8400,raft-node-2:8400",
		"Comma-separated client endpoints (host:port)")
	monitorAddr := flag.String("monitor-addr", "raft-monitor:9000",
		"host:port of the raft-monitor sidecar. Pass empty to disable the monitor sink.")
	flag.Parse()

	endpoints := testutils.SplitCSV(*nodes)
	if len(endpoints) == 0 {
		fmt.Fprintln(os.Stderr, "parallel-driver-apply: at least one endpoint required")
		os.Exit(2)
	}

	logger := testutils.NewLogger("parallel-driver-apply", *monitorAddr)

	// Randomize the dispatch size and starting endpoint via the SDK RNG so
	// the choices participate in schedule exploration.
	n := int(sdkrand.GetRandom()%10) + 1
	endpointIdx := int(sdkrand.GetRandom() % uint64(len(endpoints)))

	for i := 0; i < n; i++ {
		endpoint := endpoints[endpointIdx]
		// A "Chain of Blocks" block is blockSize random bytes.
		var blk [blockSize]byte
		binary.BigEndian.PutUint32(blk[:], uint32(sdkrand.GetRandom()))
		blockHex := "0x" + hex.EncodeToString(blk[:])

		logger.Debug("apply-request sent", "endpoint", endpoint, "block", blockHex)

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		ok, reason := submitApply(ctx, endpoint, blk[:])
		cancel()

		if ok {
			logger.Debug("apply-request success", "endpoint", endpoint, "block", blockHex)
			// Stick with the same endpoint on success.
		} else {
			logger.Debug("apply-request error", "endpoint", endpoint, "reason", reason)
			// Rotate to the next endpoint on failure.
			endpointIdx = (endpointIdx + 1) % len(endpoints)
		}
	}
}

// submitApply opens a short-lived TCP connection to a raft-node client endpoint,
// submits one apply with the given data payload, and reports whether it was
// accepted along with a structured reason on failure. Fault injection produces
// these failures constantly under normal operation; they are NOT driver errors
// but the reason string distinguishes network-layer failures (partition / dead
// node) from rejection by the leader (which surfaces state-divergence symptoms).
func submitApply(ctx context.Context, endpoint string, block []byte) (bool, string) {
	dialer := net.Dialer{Timeout: 1 * time.Second}
	conn, err := dialer.DialContext(ctx, "tcp", endpoint)
	if err != nil {
		return false, testutils.ClassifyDialErr(err)
	}
	defer conn.Close()

	if _, err := fmt.Fprintf(conn, `{"op":"apply","data":%q}`+"\n", hex.EncodeToString(block)); err != nil {
		return false, "write-failed: " + err.Error()
	}

	_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	scan := bufio.NewScanner(conn)
	scan.Buffer(make([]byte, 4096), 1<<20)
	if !scan.Scan() {
		if err := scan.Err(); err != nil {
			return false, testutils.ClassifyReadErr(err)
		}
		return false, "read-eof: connection closed before response"
	}

	var resp map[string]any
	if err := json.Unmarshal(scan.Bytes(), &resp); err != nil {
		return false, "decode-failed: " + err.Error()
	}
	if ok, _ := resp["ok"].(bool); ok {
		return true, ""
	}
	errStr, _ := resp["err"].(string)
	if errStr == "" {
		errStr = "no detail"
	}
	return false, errStr
}
