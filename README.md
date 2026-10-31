# Retail event-stream processing

A bounded local portfolio demonstration of keyed Kafka events, an inventory projection,
idempotent PostgreSQL effects, actual consumer crashes and a fresh-state rebuild. All
schemas, records and business rules are independently written and synthetic.

Java 25 / Spring Boot 4.1.2 / Spring Kafka 4.1.2, Kafka 4.3.2 and PostgreSQL 17.11.
Exact dependencies, image digests and Maven wrapper checksum are in
[toolchain-lock.json](toolchain-lock.json). One broker, three partitions, one consumer
thread and one database suffice. There is no web UI or cloud service.

## Run locally

Use Linux or WSL, Bash, Python 3 and a working Docker Engine with Compose. Docker must
be accessible to your user. The Java/Maven build runs in a pinned container; host Java
is unused. Reserve approximately 4 GiB free engine memory for serial verification and
runtime containers. Other active workloads can affect the timings.

Commit your source first, then run from the repository root:

```bash
RUN="r$(python3 -c 'import uuid; print(uuid.uuid4().hex[:12])')"
./scripts/run_checks.sh --run-id "$RUN"
```

This performs format/clean unit/integration checks, Python harness checks, records the
effective POM/dependency tree, builds one source-labelled image, validates the packaged
fixture and runs A1-A7. An unsuccessful command or skipped check fails the run. Compilation
and image preparation are timed separately; runtime startup through cleanup is limited
to 600 seconds. Barriers and catch-up are bounded. A failed run stays visible; use a new
ID after a repair.

Results are private, ignored files under `artifacts/<RUN>-verification` and
`artifacts/<RUN>-study`. `acceptance.json` and `run.json` are authoritative; raw SQL,
offsets, process logs, hashes and sanitized cleanup inventory support them. The private
`environment.env` contains a generated local database password. Never publish it.
Cleanup stops/removes only labelled owned containers and their empty network, preserving
Kafka/PostgreSQL named volumes and evidence. It never prunes Docker globally.

## Individual CLI commands

The acceptance runner sets up the stack automatically. For a manual walkthrough, reuse
the image from a successful verification but choose a fresh manual namespace. Replace
`<verified-run-id>` below with its actual ID. Run these commands in the same Bash shell:

```bash
export P22_IMAGE="$(docker image inspect --format '{{.Id}}' p22-retail-stream:<verified-run-id>)"
export RUN="r$(python3 -c 'import uuid; print(uuid.uuid4().hex[:12])')"
export DIR="$PWD/artifacts/$RUN-manual"
python3 - <<'PY'
import base64,os,pathlib,uuid
p=pathlib.Path(os.environ['DIR']);p.mkdir(parents=True,exist_ok=False)
values={'P22_RUN_ID':os.environ['RUN'],'P22_CLUSTER_ID':base64.urlsafe_b64encode(uuid.uuid4().bytes).decode().rstrip('='),'P22_DB_PASSWORD':uuid.uuid4().hex,'P22_EVIDENCE_DIR':str(p),'P22_APP_IMAGE':os.environ['P22_IMAGE'],'P22_UID':str(os.getuid()),'P22_GID':str(os.getgid())}
f=p/'environment.env';f.write_text(''.join(k+'='+v+'\n' for k,v in values.items()));f.chmod(0o600)
PY
C=(docker compose -f compose.yaml --env-file "$DIR/environment.env" -p "p22_$RUN")
"${C[@]}" up -d --wait --wait-timeout 30 kafka postgres
"${C[@]}" run --rm --no-deps app validate --fixture fixtures
ARGS=(--run-id "$RUN" --scenario normal --manifest /evidence/normal-published.json)
"${C[@]}" run --rm --no-deps app publish "${ARGS[@]}"
"${C[@]}" run --rm --no-deps app init "${ARGS[@]}"
"${C[@]}" run --rm --no-deps app consume "${ARGS[@]}"
"${C[@]}" run --rm --no-deps app snapshot "${ARGS[@]}"
"${C[@]}" run --rm --no-deps app offsets "${ARGS[@]}"
"${C[@]}" run --rm --no-deps app rebuild "${ARGS[@]}"
python3 scripts/cleanup.py --run-id "$RUN" > "$DIR/manual-cleanup.json"
```

Run cleanup even after an unsuccessful command. Existing manifests/topics/schemas are
refused for new publication/initialization/rebuild; snapshot and consume support the
matching retained projection. Exit codes: 0 success, 2 invalid arguments/configuration,
1 runtime failure. Fault switches are explicit test controls on `consume`; the automatic
harness checks exact image/run/record/container identity before issuing SIGKILL.

## What this demonstrates

The frozen workload contains 22 physical records. The expected oracle is 12 applied,
3 byte-exact duplicates and 7 durable rejections, with final A/B/C balances 3/4/2.
Negative inventory is allowed and flagged; event-time lateness is flagged without
reordering. Read [the measured report](docs/acceptance-report.md) for observed results,
[the contract](docs/event-contract.md) and [the flow](docs/architecture.md).

Delivery is at least once. A database transaction and durable receipts make effects
idempotent within one projection. This is not distributed exactly-once delivery.
Single-broker retained storage does not establish HA, replication or failover. No
throughput, production scale, customer adoption or commercial Kafka tenure is claimed.
