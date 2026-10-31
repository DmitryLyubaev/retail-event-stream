# Local acceptance report, 31 October 2026

**A1-A7 passed**, one bounded clean-source sample, run `rca880a91e724`.
One fresh read-only whole-branch review and its single regression-backed correction pass
are complete. The owner's integration choice remains pending.
All schemas, rules and records are independently written and synthetic.

| Identity | Observed value |
|---|---|
| Tested Git source | `1e8ec079154dcd30961d13bc9afa16d54ad45a33`, clean |
| Tracked source manifest SHA256 | `7a754c72368f4c569770b368d160f7c5cfd05d536fc5065b7d03a408a4d9bd61` |
| Runtime input SHA256 | `b65474072a222b338e4d3da5f1084826d0a6704a4721978413410260bd9687fb` |
| Fixture SHA256 | `5c23d47fbf2b0d4bd2e00fe19f5218763753abdd81b1f1d18cec1c17af5e44fd` |
| Packaged JAR SHA256 | `4d41567df43675a33c36b8617611e536065c773856b46b2290842794cfda43f2` |
| Actual image ID | `sha256:8bd58e8924c6f9f022b1aeb8edc9232ee3544d8efc7aabaf560e098b94d10dcc` |

`./scripts/run_checks.sh --run-id rca880a91e724` exited 0. Verification began at
03:31:43 UTC (13:31:43 Australia/Brisbane). Build/metadata/image preparation and packaged
validation took 143.044 s; startup through owned cleanup took 280.764 s, below the 600 s
limit. Runtime startup was 7.662 s. These are one local sample's wall times, not a
throughput benchmark or performance improvement.

Environment: x86_64 WSL2 Linux 6.18.54.1, Docker Engine 29.7.2, Compose 5.4.0, Python
3.14.4, eight configured CPUs and 8,298,102,784 bytes engine RAM (about 7.73 GiB).
No other containers were running at preflight. Containers have explicit memory/CPU limits;
shared-host timings are not assumed comparable. Java 25, Maven 3.10.0, Boot 4.1.2,
Spring Kafka 4.1.2, Kafka client 4.2.2, broker 4.3.2, PostgreSQL 17.11, Flyway 12.4.0,
JDBC driver 42.7.14, Testcontainers 2.0.5 and JUnit 6.0.3 are captured in the effective
POM, dependency tree and pin lock.

| Case | Actual observation |
|---|---|
| A1 | Format/clean build passed: 54 Java unit, 28 actual Kafka/PostgreSQL integration and 23 Python checks; zero failures/errors/skips. Packaged validator matched all 22 records and the independently computed fixture hash. |
| A2 | 22 published coordinates; 12 APPLIED / 3 DUPLICATE / 7 REJECTED, 12 receipts, A/B/C 3/4/2, four applied ordinals each and one late event. Every intermediate disposition/balance/ordinal/raw identity matched the static oracle. Independent SQL matched packaged CLI readback. |
| A3 | Before commit, PostgreSQL showed an open transaction whose last query inserted processing evidence. External SQL saw A=2, ordinal 0, no target receipt/processing row. SIGKILL gave exit137/OOMfalse. The same committed state remained afterward, the transaction disappeared, and target partition 0 offset 0 remained uncommitted. Restart applied it once and reached the full oracle. |
| A4 | After commit/pre-ack, external SQL saw A=-1, ordinal 1, one target receipt/processing row and no open target transaction. Offset 0 remained uncommitted. SIGKILL gave exit137/OOMfalse, preserving those observations. Restart logged target redelivery=true, with one effect and full oracle equality. |
| A5 | Fresh schema/group rebuilt 22 retained A4 records. Original before/after and rebuilt canonical snapshot SHA256 all equalled `39d892db7f489e2efeaf2338b637c4e74fc69f53cb00c8c0fc69fdafd250eee2`. Original group offsets stayed unchanged. SQL and CLI agreed. |
| A6 | Separate C+4 samples: two injected transient failures succeeded on attempt three. Three injected failures stopped with exit1, C=0, no receipt/processing row and unadvanced offset; one explicit fault-free restart reached C=4 once. Attempt-start gaps were 0.292/0.525 s and 0.324/0.561 s, including SQL/rollback overhead around 100/500 ms backoffs. |
| A7 | Commands/logs, SQL, offsets, exact barriers/process IDs and source/JAR/image identities were retained with SHA256 indexes. Ownership-checked cleanup removed this run's containers and empty network. Zero owned running containers remained; two data volumes remained. |

Assignments were partitions [0,1,2]. The full logs began at [0,0,0] and ended at next
positions [9,7,6]. Normal/recovered groups reached [9,7,6], with final backlog zero.
A4's original group kept those positions through rebuild. A's intermediate balances
were [-1,0,5,3], with negative stock allowed/flagged. B's late timestamp was flagged
without reordering. Crash SQL snapshots each used one statement for consistent readback.

Sample accounting: 66 records across three independent 22-record runs plus two one-record
retry samples equals **68 newly published physical records**. Rebuild read 22 existing
records and published none. Selected consumer logs recorded 93 delivery callbacks
including rebuild/redeliveries, 97 database attempts including in-process retries, and
90 acknowledged completions. These are separate counts, not 93 independent inputs.
Each full projection had 12 accepted effects.

Private ignored evidence: `artifacts/rca880a91e724-verification` contains aggregate
`acceptance.json`, copied JUnit reports, effective POM/dependency tree, image inspect,
versions and source hashes. `artifacts/rca880a91e724-study` contains runtime `run.json`,
acknowledged manifests, SQL/offset snapshots, barrier/kill/restart/retry logs and cleanup
inventory. Both have SHA256 indexes. Credentials remain in the private ignored environment
file. Later measured-prose edits do not change tested runtime inputs.

Retained canonical volumes: `p22_rca880a91e724_kafka_data` and
`p22_rca880a91e724_postgres_data`. Earlier smokes `r069a4941ee2f` and `rb048d787f443`
retain two volumes each and honest dirty-source identities; the latter exercised one
actual postcommit kill in 90.137 s. They are separate from canonical evidence. No canonical
case failed or was skipped. Development RED assertions, corrected JUnit parameter-source
setup errors and initial dependency-resolution warnings remain in private evidence.
Expected injected-failure ERROR logs are not hidden or counted as JUnit failures.

Unrun: broker restart, storage loss, replication/failover, sustained load, multiple
producers/consumers, real network acknowledgement outage, cloud and hosted CI. An ack
callback failure was injected in integration tests, not a real network fault. A late
source-boundary change aborts rebuild and retains its failed target with original unchanged.
Retained local volumes are not a backup/HA guarantee.

The demonstrated guarantee is at-least-once delivery with idempotent database effects
within one product-scoped projection. It is not distributed exactly-once, production
scale, adoption or commercial streaming experience. A CV-safe description: built and
tested a local Java/Spring Kafka inventory projection with durable duplicate handling,
actual pre/post-commit consumer-crash recovery and isolated replay into PostgreSQL.

The fresh reviewer examined `16d503b..b363393` and requested connection-start retry and
executable packaging fixes. Snapshot incoherence was regraded Important because normal
concurrent readback could describe a state that never existed. All three were corrected
in one pass with witnessed RED->GREEN regressions: classified SQLSTATE retry/recovery/stop
through real transaction/Kafka paths, a writer commit between snapshot reads, and Git
launcher modes. A Linux-filesystem Git archive also executed the documented launcher
directly. The full canonical run above verifies the corrected source; no second reviewer
was used. Deferred minors: none. The earlier canonical `r90cac148f56b` at `3595595` passed
its functional study on DrvFS in 285.219 s; it is retained unchanged, including its two
volumes, and is superseded for Linux packaging by this corrected sample. Alongside both
earlier smokes, eight Project22 named volumes are retained in total. No application
remote, public push, hosted CI, paid/provider call or cloud resource was created.
