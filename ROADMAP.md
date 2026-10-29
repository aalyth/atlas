# Atlas Roadmap

Atlas is being built toward a distributed key-value store with Paxos-based consensus and gossip-based peer discovery. Milestones are ordered so the lower layer is stable and tested before the next one lands on top.

## Milestone 1 — Stable single-node engine (done)

- Single `internal/data` package owns the on-disk entry format with explicit timestamp and tombstone flag.
- Validation: empty keys, oversized keys/values, newlines rejected at the data boundary.
- Logging: `go.uber.org/zap` injected through `AtlasServer → Atlas → Wal`; structured fields, no package globals.
- HTTP API: chi router under `/v1/atlas/{key}`, request-id + recoverer + structured request log, `GET /healthz`.
- Storage: `createNewLsm` creates level dirs under the LSM root; `SSTable.Entries()` returns the full set; WAL replays existing `.wal` files into the cache on boot.
- Tests: Given-When-Then table tests for `data`, `storage`, and `engine`. `make check` runs `go vet` + `go test ./...`.
- Runtime root resolved from `ATLAS_ROOT`, falling back to `~/atlas`.

## Milestone 2 — Finish the LSM

- Implement `Lsm.MergeWal` end-to-end: drain WAL, dedupe by latest timestamp, persist a new level-0 SSTable, truncate the WAL.
- Cascading merges: when a level's total size exceeds its budget, k-way-merge overlapping SSTables into the next level using the existing `SSTableIterator`.
- Cleanup: delete input SSTables only after the new one is fsync'd.
- Background compaction goroutine on `Atlas` with stop channel, triggered by WAL size + idle ticks.
- Tombstone GC at the deepest level only.

## Milestone 3 — Paxos with static config

- New `internal/consensus/paxos` package: proposer/acceptor/learner with an internal `Transport` interface (first impl: HTTP).
- Extend `AtlasServerConfig` with `Peers []PeerConfig` — static membership, no discovery yet.
- Write path: handlers submit proposals, wait for chosen value, then apply locally.
- Tests: 3-node in-process cluster with an in-memory transport plus chaos cases (dropped messages, leader kill).

## Milestone 4 — Gossip discovery

- New `internal/discovery/gossip` package — start with simple anti-entropy push-pull, evolve toward SWIM if needed.
- Replace static `Peers` with a seed list; gossip maintains live membership consumed by Paxos.
- Reconfiguration protocol for membership changes (joint consensus or stop-the-world — decide at the milestone).
