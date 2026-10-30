// Package main is the raft-node harness binary used by the Antithesis test harness.
//
// Each container runs one instance. It embeds the (patched) hashicorp/raft library,
// stands up a small Raft cluster over TCP, exposes a minimal client API for the
// client to drive, and otherwise stays idle.
//
// This is the SETUP-stage driver — minimal viable. The workload skill will extend
// this with fault-injecting LogStore/StableStore wrappers, leadership-transfer
// trigger paths, query endpoints for cluster-wide property checks, etc.
package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/raft"
	boltdb "github.com/hashicorp/raft-boltdb/v2"
)

// blockSize is the fixed size, in bytes, of every block in the "Chain of
// Blocks" workload. The client sends blocks of exactly this many random bytes.
const blockSize = 4

// blockFSM implements the "Chain of Blocks" workload FSM. Each Apply consumes a
// fixed-size blockSize-byte block from the log entry's data, folds it into a
// running XOR-hash of every block applied, and increments the applied-block
// count. Its entire state is (hash, count). Folding is pure and deterministic:
// every replica applies the same log in the same order, so the hash agrees.
//
// Apply, Snapshot, and Restore are all invoked serially from the library's
// single runFSM goroutine, so they never race one another. The mutex exists
// only because the client `read` op loads the fields concurrently from a
// per-connection goroutine; holding it there returns a consistent joint
// snapshot of (hash, count) rather than a torn pair.
type blockFSM struct {
	logger hclog.Logger
	mu     sync.Mutex
	hash   uint32 // XOR of every applied block, interpreted big-endian
	count  uint32 // number of blocks applied
}

func (f *blockFSM) Apply(l *raft.Log) any {
	if len(l.Data) != blockSize {
		return fmt.Errorf("invalid apply data: want %d-byte block, got %d bytes", blockSize, len(l.Data))
	}
	block := binary.BigEndian.Uint32(l.Data)
	f.mu.Lock()
	defer f.mu.Unlock()
	old := f.hash
	f.hash ^= block
	f.count++
	f.logger.Info(fmt.Sprintf("Applied block %d (0x%08x) state: 0x%08x => 0x%08x", f.count, block, old, f.hash))
	return f.hash
}

func (f *blockFSM) Snapshot() (raft.FSMSnapshot, error) {
	f.mu.Lock()
	snap := &blockSnap{state: blockSnapState{Hash: f.hash, Count: f.count}}
	f.mu.Unlock()
	return snap, nil
}

func (f *blockFSM) Restore(rc io.ReadCloser) error {
	defer rc.Close()
	dec := json.NewDecoder(rc)
	var s blockSnapState
	if err := dec.Decode(&s); err != nil {
		return err
	}
	f.mu.Lock()
	f.hash = s.Hash
	f.count = s.Count
	f.mu.Unlock()
	return nil
}

// blockSnapState is the on-disk snapshot payload: the folded FSM state and
// nothing else. Serialized as JSON.
type blockSnapState struct {
	Hash  uint32 `json:"hash"`
	Count uint32 `json:"count"`
}

type blockSnap struct{ state blockSnapState }

func (s *blockSnap) Persist(sink raft.SnapshotSink) error {
	if err := json.NewEncoder(sink).Encode(s.state); err != nil {
		sink.Cancel()
		return err
	}
	return sink.Close()
}

func (s *blockSnap) Release() {}

func main() {
	var (
		nodeID   = flag.String("id", "", "Node ID (e.g. node-0)")
		raftAddr = flag.String("raft-addr", "0.0.0.0:8300", "Raft RPC bind address")
		clientAddr = flag.String("client-addr", "0.0.0.0:8400", "Client RPC bind address")
		peers    = flag.String("peers", "", "Comma-separated list of peer addresses, format id=host:raftport")
		bootstrap = flag.Bool("bootstrap", false, "Bootstrap the cluster with the configured peer set")
		monitorAddr = flag.String("monitor-addr", "", "host:port of the raft-monitor sidecar. If empty, no monitor sink is registered.")
		dataDir = flag.String("data-dir", "/var/lib/raft-node", "Persistent state directory for raft log, stable store, and snapshots. Survives container kill+restart via the writable layer.")
	)
	flag.Parse()

	if *nodeID == "" {
		fmt.Fprintln(os.Stderr, "raft-node: -id is required")
		os.Exit(2)
	}

	// Primary logger writes human-readable text at DEBUG to stderr. The
	// InterceptLogger lets us attach extra sinks at runtime — see below for
	// the JSON-over-TCP sink that ships every record (down to DEBUG) to the
	// raft-monitor sidecar for global-property checking.
	ilogger := hclog.NewInterceptLogger(&hclog.LoggerOptions{
		Name:   *nodeID,
		Level:  hclog.Debug,
		Output: os.Stderr,
	})
	var logger hclog.Logger = ilogger

	// If a monitor sidecar address was provided, connect and register a JSON
	// sink that ships every record (down to DEBUG) for global-property
	// checking. Dial retries for up to 30s so docker-compose startup order
	// doesn't matter. If the monitor flag is set but the monitor never comes
	// up we exit — silently dropping global property events would be worse
	// than a startup failure.
	if *monitorAddr != "" {
		monitorConn, err := dialMonitor(*monitorAddr, 30*time.Second)
		if err != nil {
			logger.Error("connect to raft-monitor", "addr", *monitorAddr, "err", err)
			os.Exit(1)
		}
		ilogger.RegisterSink(hclog.NewSinkAdapter(&hclog.LoggerOptions{
			Name:       *nodeID,
			Level:      hclog.Debug,
			Output:     monitorConn,
			JSONFormat: true,
		}))
		logger.Info("registered raft-monitor sink", "addr", *monitorAddr)
	}

	cfg := raft.DefaultConfig()
	cfg.LocalID = raft.ServerID(*nodeID)
	cfg.Logger = logger
	// Lower timeouts so leadership turns over quickly in test runs.
	cfg.HeartbeatTimeout = 200 * time.Millisecond
	cfg.ElectionTimeout = 200 * time.Millisecond
	cfg.LeaderLeaseTimeout = 100 * time.Millisecond
	cfg.CommitTimeout = 50 * time.Millisecond
	// Aggressive snapshot settings so the snapshot path is exercised heavily
	// during typical 15-minute timelines. Defaults (SnapshotInterval=120s,
	// SnapshotThreshold=8192) mean snapshots barely happen with the current
	// driver-apply rate. With these settings, snapshots fire every few
	// seconds whenever there are new applies — exercising the snapshot path
	// heavily.
	cfg.SnapshotInterval = 5 * time.Second
	cfg.SnapshotThreshold = 128
	// Keep only a small trailing-log tail after compaction (default is 10240).
	// This makes log compaction actually trim the live region close to the
	// snapshot index on every snapshot — exercising the compaction/DeleteRange
	// path heavily (more follower InstallSnapshot catch-ups). Followers that
	// fall behind catch up via snapshot install rather than log replay.
	cfg.TrailingLogs = 16

	// Persistent stores so log + votedFor + currentTerm survive container
	// kill+restart. raft-boltdb's BoltStore satisfies both LogStore and
	// StableStore on a single bbolt file; snapshots live as files under the
	// same data dir via raft.NewFileSnapshotStore.
	if err := os.MkdirAll(*dataDir, 0o755); err != nil {
		logger.Error("create data dir", "path", *dataDir, "err", err)
		os.Exit(1)
	}
	boltDB, err := boltdb.NewBoltStore(filepath.Join(*dataDir, "raft.db"))
	if err != nil {
		logger.Error("new bolt store", "path", *dataDir, "err", err)
		os.Exit(1)
	}
	logStore := boltDB
	stableStore := boltDB
	snapStore, err := raft.NewFileSnapshotStore(*dataDir, 3, os.Stderr)
	if err != nil {
		logger.Error("new file snapshot store", "path", *dataDir, "err", err)
		os.Exit(1)
	}

	addr, err := net.ResolveTCPAddr("tcp", *raftAddr)
	if err != nil {
		logger.Error("resolve raft addr", "err", err)
		os.Exit(1)
	}
	transport, err := raft.NewTCPTransport(*raftAddr, addr, 3, 10*time.Second, os.Stderr)
	if err != nil {
		logger.Error("new tcp transport", "err", err)
		os.Exit(1)
	}

	fsm := &blockFSM{logger: logger}
	r, err := raft.NewRaft(cfg, fsm, logStore, stableStore, snapStore, transport)
	if err != nil {
		logger.Error("new raft", "err", err)
		os.Exit(1)
	}

	if *bootstrap {
		// Every initial voter bootstraps with the IDENTICAL full configuration.
		// BootstrapCluster writes the configuration entry straight to the local
		// log store (index 1, term 1) with no replication, so all three nodes
		// independently seed the committed 3-voter membership. The cluster then
		// only has to hold a single election to choose a leader — no membership
		// changes, no follower catch-up, no config replication. This is the
		// documented hashicorp/raft bootstrap procedure (call on every initial
		// voter with the same configuration). Whichever node already has state
		// (e.g. on a container restart) returns ErrCantBootstrap, which is
		// expected and ignored.
		servers := parsePeers(*peers)
		future := r.BootstrapCluster(raft.Configuration{Servers: servers})
		if err := future.Error(); err != nil && err != raft.ErrCantBootstrap {
			logger.Error("bootstrap", "err", err)
			os.Exit(1)
		}
	}

	// Start the client API server.
	go serveClient(*clientAddr, r, fsm, logger)

	logger.Info("raft-node ready", "raft_addr", *raftAddr, "client_addr", *clientAddr)

	// Wait for shutdown.
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()
	<-ctx.Done()
	logger.Info("shutting down")

	if err := r.Shutdown().Error(); err != nil {
		logger.Error("shutdown", "err", err)
	}
}

// dialMonitor retries net.Dial until it succeeds or the deadline expires.
// The retry exists so docker-compose service startup order doesn't matter:
// raft-node-0 may try to log before raft-monitor's listener is bound.
func dialMonitor(addr string, deadline time.Duration) (net.Conn, error) {
	end := time.Now().Add(deadline)
	var lastErr error
	for time.Now().Before(end) {
		conn, err := net.DialTimeout("tcp", addr, 1*time.Second)
		if err == nil {
			return conn, nil
		}
		lastErr = err
		time.Sleep(500 * time.Millisecond)
	}
	return nil, lastErr
}

func parsePeers(s string) []raft.Server {
	if s == "" {
		return nil
	}
	var out []raft.Server
	for _, part := range strings.Split(s, ",") {
		eq := strings.IndexByte(part, '=')
		if eq < 0 {
			continue
		}
		id := strings.TrimSpace(part[:eq])
		addr := strings.TrimSpace(part[eq+1:])
		out = append(out, raft.Server{
			ID:       raft.ServerID(id),
			Address:  raft.ServerAddress(addr),
			Suffrage: raft.Voter,
		})
	}
	return out
}

// Client API: line-delimited JSON over a single TCP connection.
//
// Commands:
//
//	{"op":"apply","data":"<hex block>"}            → {"ok":true,"index":N,"value":"<hex hash>"}
//	{"op":"state"}                                 → {"state":"Leader|Follower|Candidate","term":T,"leader_addr":"...","leader_id":"...","last_index":L,"applied_index":A}
//	{"op":"read"}                                  → {"hash":"<hex hash>","count":C}
//	{"op":"add_voter","id":"x","addr":"y"}         → leader-only
//	{"op":"remove_server","id":"x"}                → leader-only
//	{"op":"transfer_leadership","id":"x","addr":"y"} → leader-only; blocks until transfer completes/aborts
//
// Every request received and every response sent is logged at debug level
// as "received request from client: <json>" / "sent response to client: <json>".
func serveClient(addr string, r *raft.Raft, fsm *blockFSM, logger hclog.Logger) {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		logger.Error("client listen", "err", err)
		os.Exit(1)
	}
	for {
		conn, err := ln.Accept()
		if err != nil {
			logger.Error("client accept", "err", err)
			continue
		}
		go handleClient(conn, r, fsm, logger)
	}
}

func handleClient(conn net.Conn, r *raft.Raft, fsm *blockFSM, logger hclog.Logger) {
	defer conn.Close()
	scan := bufio.NewScanner(conn)
	scan.Buffer(make([]byte, 4096), 1<<20)
	sendResp := func(v map[string]any) {
		b, err := json.Marshal(v)
		if err != nil {
			logger.Error("encode response", "err", err)
			return
		}
		logger.Debug(fmt.Sprintf("sent response to client: %s", b))
		_, _ = fmt.Fprintf(conn, "%s\n", b)
	}
	for scan.Scan() {
		logger.Debug(fmt.Sprintf("received request from client: %s", scan.Bytes()))
		dec := json.NewDecoder(bytes.NewReader(scan.Bytes()))
		dec.UseNumber()
		var req map[string]any
		if err := dec.Decode(&req); err != nil {
			sendResp(map[string]any{"ok": false, "err": err.Error()})
			continue
		}
		op, _ := req["op"].(string)
		switch op {
		case "apply":
			dataStr, ok := req["data"].(string)
			if !ok {
				sendResp(map[string]any{"ok": false, "err": "apply: data must be a hex-encoded block string"})
				continue
			}
			block, err := hex.DecodeString(dataStr)
			if err != nil {
				sendResp(map[string]any{"ok": false, "err": "apply: data must be valid hex: " + err.Error()})
				continue
			}
			if len(block) != blockSize {
				sendResp(map[string]any{"ok": false, "err": fmt.Sprintf("apply: data must decode to a %d-byte block, got %d bytes", blockSize, len(block))})
				continue
			}
			fut := r.Apply(block, 5*time.Second)
			if err := fut.Error(); err != nil {
				sendResp(map[string]any{"ok": false, "err": err.Error()})
				continue
			}
			if applyErr, isErr := fut.Response().(error); isErr {
				sendResp(map[string]any{"ok": false, "err": applyErr.Error()})
				continue
			}
			hash, _ := fut.Response().(uint32)
			sendResp(map[string]any{"ok": true, "index": fut.Index(), "value": fmt.Sprintf("%08x", hash)})
		case "state":
			// r.State(), r.CurrentTerm(), r.LeaderWithID(), r.AppliedIndex(),
			// and r.LastIndex() are independent synchronized loads; a concurrent
			// step-down or commit landing between them can yield a torn,
			// mutually-inconsistent snapshot (e.g. an impossible (Leader,
			// new-term) pair, or applied_index > last_index). Term grows
			// monotonically, so we read it before and after the whole group and
			// retry on mismatch, yielding a snapshot consistent with a single
			// term. Within the group, AppliedIndex is read before LastIndex:
			// both only grow, so sampling applied first and last second keeps
			// applied <= last even if an entry commits concurrently (last, read
			// later, is >= last-at-applied-time >= applied). Term changes
			// infrequently relative to this read, so the loop almost always
			// exits on the first iteration.
			var (
				state        raft.RaftState
				term         uint64
				leader       raft.ServerAddress
				leaderID     raft.ServerID
				appliedIndex uint64
				lastIndex    uint64
			)
			for {
				before := r.CurrentTerm()
				state = r.State()
				leader, leaderID = r.LeaderWithID()
				appliedIndex = r.AppliedIndex()
				lastIndex = r.LastIndex()
				term = r.CurrentTerm()
				if term == before {
					break
				}
			}
			sendResp(map[string]any{
				"state":         state.String(),
				"term":          term,
				"leader_addr":   string(leader),
				"leader_id":     string(leaderID),
				"last_index":    lastIndex,
				"applied_index": appliedIndex,
			})
		case "read":
			fsm.mu.Lock()
			h, c := fsm.hash, fsm.count
			fsm.mu.Unlock()
			sendResp(map[string]any{"hash": fmt.Sprintf("%08x", h), "count": c})
		case "add_voter":
			id, _ := req["id"].(string)
			peerAddr, _ := req["addr"].(string)
			fut := r.AddVoter(raft.ServerID(id), raft.ServerAddress(peerAddr), 0, 5*time.Second)
			if err := fut.Error(); err != nil {
				sendResp(map[string]any{"ok": false, "err": err.Error()})
				continue
			}
			sendResp(map[string]any{"ok": true})
		case "remove_server":
			id, _ := req["id"].(string)
			fut := r.RemoveServer(raft.ServerID(id), 0, 5*time.Second)
			if err := fut.Error(); err != nil {
				sendResp(map[string]any{"ok": false, "err": err.Error()})
				continue
			}
			sendResp(map[string]any{"ok": true})
		case "transfer_leadership":
			id, _ := req["id"].(string)
			peerAddr, _ := req["addr"].(string)
			fut := r.LeadershipTransferToServer(raft.ServerID(id), raft.ServerAddress(peerAddr))
			if err := fut.Error(); err != nil {
				sendResp(map[string]any{"ok": false, "err": err.Error()})
				continue
			}
			sendResp(map[string]any{"ok": true})
		default:
			sendResp(map[string]any{"ok": false, "err": "unknown op"})
		}
	}
}
