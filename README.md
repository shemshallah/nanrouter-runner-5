# nanrouter-runner-5

NaNVerse six-slot **runner-ring node** `5`.

All six `nanrouter-runner-0..5` repositories run the same code. Only the `slot`
file differs. They are the internal **BSR0..BSR5** members of `MC0:0`; they do
**not** receive individual HW addresses. The ring as a whole is **`HW5:0`** for
NaN routing.

Koyeb remains the NANNET portal (pairing, issuer, auth, training merge) and is
also the atomic runner-ring authority: nanclock tick, single-active-slot lease,
epoch, and bounded handoff checkpoint. The runners themselves are non-Koyeb
24/7 processes.

## Temporal handoff

```text
runner-0 ──┐
runner-1 ──┤   Koyeb atomic lease + nanclock + epoch
runner-2 ──┼─> exactly ONE active slot runs the HW5:0 fabric
runner-3 ──┤   the other five are hot standbys
runner-4 ──┤
runner-5 ──┘
```

- The portal allocates a strictly monotone `nanclock_tick` under a SQLite
  `BEGIN IMMEDIATE` transaction.
- One slot holds the lease. A different slot is denied while the lease is live.
- On expiry, release or explicit handoff, the next slot acquires with
  `epoch + 1`, restores the bounded checkpoint and continues.
- The checkpoint carries the exact `HW5:0` fabric inbox, including NaN16DE
  words, so accepted frames are not silently lost during handoff.
- If the active process dies, its lease expires and a standby takes over.

The 7 GiB budget is reported and soft-enforced per runner process. It is not a
claim that the host physically reserves 7 GiB.

## Run

```sh
export NANROUTER_RING_URL='https://nannet.apps.mistral.run'
export NANROUTER_RING_TOKEN='<32+ random characters matching the portal>'
export NANROUTER_RUNNER_URL='https://<this-runner-public-url>'
export NANROUTER_RUNNER_STATE_DIR=/data/runner-state
./start.sh
```

The portal must have `RUNNER_RING_TOKEN` set to the same secret. Do not put the
token in Git.

Health:

```text
GET  /healthz            liveness + phase + memory + fabric summary
GET  /readyz             200 after a successful nanclock tick
GET  /metrics            same bounded JSON counters
GET  /v1/ring/status     authenticated full node/fabric status
GET  /v1/ring/points     authenticated central zero-point fabric monitor
GET  /v1/ring/clock      authenticated portal nanclock tick
POST /v1/ring/register   authenticated register another HW zero point
POST /v1/ring/submit     authenticated NaN16DE frame -> active HW5:0 fabric
POST /v1/ring/drain      authenticated drain from HW5/HW0/HW2 or a registered point
POST /v1/ring/checkpoint authenticated checkpoint publication
POST /v1/ring/handoff    authenticated explicit step-down
```

## 24/7 deployment

The repository is intentionally non-Koyeb. Any always-on host works:

```sh
docker compose up -d --build
```

or a systemd unit:

```sh
sudo install -d /opt/nanrouter-runner /var/lib/nanrouter-runner
sudo cp -a . /opt/nanrouter-runner
sudo cp deploy/nanrouter-runner.service /etc/systemd/system/
sudo cp deploy/nanrouter-runner.env.example /etc/nanrouter-runner.env
# edit the token and public URL, then:
sudo systemctl enable --now nanrouter-runner
```

Run the six repositories on six independent hosts (or six supervised
processes). Each has the same code and a different `slot` file. The Koyeb
authority guarantees one active `HW5:0` owner and an epoch-bound handoff.

## Ring addresses

```text
HW0:0   Bootstrap network port
HW2:0   HERMES endpoint
HW5:0   the runner ring (owned by the one active slot)
```

Frames are routed as exact NaN16DE words. The active central fabric can register
additional zero points (`HW5:0`..`HW255:0`) on demand and route NaN to them. The
runner does not invent an HW address for its own process; it owns the ring
coordinate `HW5:0` only while the lease is active.

## Tests

```sh
python -m unittest discover -s tests -p 'test_*.py' -v
```

Coverage includes slot-only identity (no HW field), 7 GiB budget, atomic lease
activation, standby denial, checkpoint publication, handoff with inbox
preservation, authenticated submit/drain, and no-token rejection.
