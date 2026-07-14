// Package testutils holds helpers shared by the Antithesis raft harness
// binaries (setup + test-commands). These are not used by the SUT itself
// and are excluded from the raft-node image.
package testutils

import (
	"errors"
	"net"
	"os"
	"strings"
	"time"

	"github.com/hashicorp/go-hclog"
)

// SplitCSV parses a comma-separated endpoint list. Whitespace is trimmed and
// empty entries are dropped.
func SplitCSV(s string) []string {
	if s == "" {
		return nil
	}
	var out []string
	for _, p := range strings.Split(s, ",") {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}

// ClassifyDialErr maps a net.Dial error into a stable structured reason
// (dial-timeout / dial-refused / dial-reset / dial-unreachable /
// dial-failed:<msg>). Used for the `reason` field in error records so triage
// can group by failure class.
func ClassifyDialErr(err error) string {
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return "dial-timeout"
	}
	msg := err.Error()
	switch {
	case strings.Contains(msg, "connection refused"):
		return "dial-refused"
	case strings.Contains(msg, "connection reset"):
		return "dial-reset"
	case strings.Contains(msg, "no route to host"):
		return "dial-unreachable"
	}
	return "dial-failed: " + msg
}

// ClassifyReadErr maps a TCP read error into a stable structured reason
// (read-timeout / read-reset / read-failed:<msg>).
func ClassifyReadErr(err error) string {
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return "read-timeout"
	}
	msg := err.Error()
	if strings.Contains(msg, "connection reset") {
		return "read-reset"
	}
	return "read-failed: " + msg
}

// NewLogger returns an hclog.InterceptLogger that writes INFO+ to stderr and,
// when monitorAddr is non-empty and reachable, ships every record (DEBUG+) as
// JSON to the raft-monitor sidecar — the same fan-out pattern raft-node uses.
// A failed monitor dial silently degrades to stderr-only: these are short-lived
// dispatches and the next one will re-establish the connection.
func NewLogger(name, monitorAddr string) hclog.Logger {
	ilogger := hclog.NewInterceptLogger(&hclog.LoggerOptions{
		Name:   name,
		Level:  hclog.Info,
		Output: os.Stderr,
	})
	if monitorAddr == "" {
		return ilogger
	}
	conn, err := net.DialTimeout("tcp", monitorAddr, 3*time.Second)
	if err != nil {
		return ilogger
	}
	ilogger.RegisterSink(hclog.NewSinkAdapter(&hclog.LoggerOptions{
		Name:       name,
		Level:      hclog.Debug,
		Output:     conn,
		JSONFormat: true,
	}))
	return ilogger
}
