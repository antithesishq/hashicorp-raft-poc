# Hashicorp Raft — Antithesis benchmark

This directory is the Antithesis test harness for the `hashicorp/raft` library, which
is a slightly modified version of the upstream library with extra debug logging for
diagnosing found bugs.

If you just want to run a test, jump to [Typical workflow](#typical-workflow).
If you want to know what's in here, read on.

## What's here

```
antithesis/
├── README.md                  ← this file
├── architecture.svg           ← rendered deployment/topology diagram
├── upstream.sha               ← full SHA of the upstream commit this library was imported from
├── build.sh                   ← build the four images locally (no push)
├── build-and-push.sh          ← build (via build.sh) + push the four images to ant-presearch
├── run.sh                     ← launch one baseline run
├── raft-node/                 ← SUT binary (TCP transport + Chain-of-Blocks FSM). Built into the hashicorp-raft-node image.
├── raft-monitor/              ← Python TCP server that receives JSON log records
│                                from each raft-node's hclog InterceptLogger sink and
│                                dispatches them to consumers that check global
│                                (cross-node) properties. Built into the hashicorp-raft-monitor image.
├── raft-client/               ← Self-contained Go module for the client container.
│   │                            Built into the hashicorp-raft-client image; shares no Go code
│   │                            with the SUT.
│   ├── go.mod
│   ├── setup/                 ← client container ENTRYPOINT: wait-for-leader → emit SetupComplete → idle
│   ├── test-commands/         ← every dir builds a test command Antithesis dispatches into the client container.
│   │   ├── parallel-driver-apply/               ← sustained Apply traffic
│   │   ├── parallel-driver-leadership-transfer/ ← drives leadership transfers
│   │   ├── eventually-workload-commit-progress/ ← end-of-timeline commit-progress check
│   │   └── eventually-fsm-convergence/          ← end-of-timeline FSM convergence check
│   └── test-utils/            ← shared helpers for setup + test-commands (logger, dial-err classifier, CSV split, cluster health gate + client-API query helpers)
└── config/
    ├── Dockerfile.raft-node
    ├── Dockerfile.raft-client
    ├── Dockerfile.raft-monitor
    ├── Dockerfile.raft-config
    └── docker-compose.yaml    ← 3 raft nodes + client + monitor
```

## Typical workflow

### 1. Make a change

Edit `raft.go` / `fsm.go` / `snapshot.go` for SUT-side assertions, a
`raft-client/test-commands/<antithesis-prefix>-<name>/` binary for workload-side
ones (e.g., `raft-client/test-commands/anytime-new-property/`), or a consumer
in `raft-monitor/` for properties that need cross-node visibility (consumers
receive every hclog record from every raft-node as a Python dict).

### 2. Build (and push) images

```bash
# from the repository root (the directory containing antithesis/)
./antithesis/build.sh                    # build locally only — compile-check without touching the registry
./antithesis/build-and-push.sh           # build (via build.sh) and push tagged :latest
```

`build.sh` builds the four `linux/amd64` images; `build-and-push.sh` runs
`build.sh` and then pushes them tagged `:latest` to the registry configured via
the `ANTITHESIS_REGISTRY` environment variable.

- `hashicorp-raft-node` — the SUT harness
- `hashicorp-raft-client` — client + all test-command binaries
- `hashicorp-raft-monitor` — Python sidecar that consumes JSON log records from each
  raft-node's hclog InterceptLogger sink and checks global properties
- `hashicorp-raft-config` — tiny image carrying just `docker-compose.yaml`

Both scripts tag everything `:latest` by default; pass a tag as the first
argument to override (e.g. `./antithesis/build-and-push.sh verbose` tags and
pushes `:verbose`).

The notebook references these by name and pulls `:latest`. Override with
`benchmark.tag=…` if you need a non-latest experiment tag.

### 3. Launch a run

The `run.sh` wrapper launches with `snouty launch --webhook benchmark`, passing
hashicorp-raft's specifics (`benchmark.name`, `benchmark.image`,
`benchmark.exclude_faults`) as params.

For a one-off launch:

```bash
snouty launch \
    --webhook benchmark \
    --param benchmark.name="Hashicorp Raft" \
    --param benchmark.image="hashicorp-raft-config" \
    --param benchmark.exclude_faults="raft-client,raft-monitor" \
    --param benchmark.tag=<tag?> \
    --ephemeral \
    --duration 15 \
    --description "<the change being tested>" \
    --recipients <email>
```

`--duration` is in minutes (the notebook clamps to `[15, 2400]`). The trailing
`benchmark.*` param is optional:

- `benchmark.tag` — image tag to run (defaults to `latest`).

### 4. Triage

Open the resulting triage report and check that the properties you expected
to fire actually fired. Use the `antithesis-triage` skill in an agent session
if you want help walking through it.

## Skills

- `antithesis-workload` — implement / refine assertions and test commands
- `antithesis-launch` — for snouty launch
- `antithesis-triage` — examine results of a test run + query logs
