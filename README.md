# nanrouter-runner-5

24/7 **NaNVerse runner** for slot `5` and zero point `HW10:0`.

Koyeb remains the NANNET portal (pairing, issuer, auth, training merge). This
repository is a **non-Koyeb** runner: it monitors, translates and routes NaN
traffic for the runner ring. It never holds portal secrets and never mints an
issuer.

## Role

`role` = `agent`

- `hub` (normally runner-0): runs the central zero-point fabric, monitors every
  registered point, persists pending frames and exposes the bounded
  `/v1/fabric/*` API.
- `agent` (runners 1-5): owns `HW10:0`, registers/heartbeats with the hub,
  drains NaN16DE frames into a durable local journal and routes them by an
  explicit kind-prefix table.

The agent never evaluates NaN words and never invents a route from a runner
slot or byte offset. It preserves the exact NaN16DE word bytes.

## Run

```sh
export NANROUTER_FABRIC_TOKEN='<32+ random characters>'
export NANROUTER_URL='https://<hub-host>'
export NANROUTER_RUNNER_STATE_DIR=/data/runner-state
./start.sh
```

Health:

```text
GET /healthz   liveness + counters
GET /readyz    200 only after a recent successful poll
GET /metrics   same bounded JSON counters
```

## Hub mode

```sh
export NANROUTER_FABRIC_TOKEN='<32+ random characters>'
export FABRIC_DB=/data/point-fabric.sqlite
export HUB_ACTIVE_POINTS=0,2
./start.sh
```

`HW0:0` is the Bootstrap network port. `HW2:0` is the HERMES endpoint. General
runners own `HW5:0`..`HW255:0`; the portal remains the allocation authority.

## Route table

`NANROUTER_RUNNER_ROUTES` is a JSON object of kind prefixes to destination zero points:

```json
{"hermes.": 2, "wordnet.": 4, "vector": 0}
```

A route to the agent's own point is ignored to prevent a loop. Without an
explicit route, drained frames are only journaled, not re-emitted.

## Boundaries

- No secret is stored in Git.
- No NaN arithmetic is performed on the host.
- The existing NANNET portal and HERMES lease authority remain authoritative.
- Durable pending frames are SQLite WAL in hub mode and JSONL in agent mode.
- This repository is deployable on any 24/7 non-Koyeb host (Termux/Android,
  VPS, home server, Codespace) with Python 3.10+ or Docker.

## Tests

```sh
python -m unittest discover -s tests -p 'test_*.py' -v
```
