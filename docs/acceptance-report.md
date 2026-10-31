# Local acceptance report, 31 October 2026

**A1-A7 passed**, one bounded clean-source sample, run `r90cac148f56b`.
Fresh whole-branch review and the owner's integration choice follow this measurement.
All schemas, rules and records are independently written and synthetic.

| Identity | Observed value |
|---|---|
| Tested Git source | `3595595af7533a832b50cb4c2f08ea7f4410bc4a`, clean |
| Tracked source manifest SHA256 | `90771d7631b333f6a22bcbef3a28bad3ffadb41d52d2448dadf9ca7056334edc` |
| Runtime input SHA256 | `f7d3e189e3170f637c543ab1841784d6c786825cb8c8d8a6b41b88627395c296` |
| Fixture SHA256 | `5c23d47fbf2b0d4bd2e00fe19f5218763753abdd81b1f1d18cec1c17af5e44fd` |
| Packaged JAR SHA256 | `88b5c0476b371b66d930798d899ac2d6d571fb8cbb2ca92c192cd38b88e3d13b` |
| Actual image ID | `sha256:eaa9ebbca4690b0bd6e9552d2ea12ba0397592cf5b5aa38d153df1b392aeea51` |

`./scripts/run_checks.sh --run-id r90cac148f56b` exited 0. Verification began at
03:06:51 UTC (13:06:51 Australia/Brisbane). Build/metadata/image preparation and packaged
validation took 152.540 s; startup through owned cleanup took 285.219 s, below the 600 s
limit. Runtime startup was 7.718 s. These are one local sample's wall times, not a
throughput benchmark or performance improvement.

Environment: x86_64 WSL2 Linux 6.18.54.1, Docker Engine 29.7.2, Compose 5.4.0, Python
3.14.4, eight configured CPUs and 8,298,098,688 bytes engine RAM (about 7.73 GiB).
No other containers were running at preflight. Containers have explicit memory/CPU limits;
shared-host timings are not assumed comparable. Java 25, Maven 3.10.0, Boot 4.1.2,
Spring Kafka 4.1.2, Kafka client 4.2.2, broker 4.3.2, PostgreSQL 17.11, Flyway 12.4.0,
JDBC driver 42.7.14, Testcontainers 2.0.5 and JUnit 6.0.3 are captured in the effective
POM, dependency tree and pin lock.

| Case | Actual observation |
|---|---|
| A1 | Format/clean build passed: 47 Java unit, 25 actual Kafka/PostgreSQL integration and 22 Python checks; zero failures/errors/skips. Packaged validator matched all 22 records and the independently computed fixture hash. |
| A2 | 22 published coordinates; 12 APPLIED / 3 DUPLICATE / 7 REJECTED, 12 receipts, A/B/C 3/4/2, four applied ordinals each and one late event. Every intermediate disposition/balance/ordinal/raw identity matched the static oracle. Independent SQL matched packaged CLI readback. |
| A3 | Before commit, PostgreSQL showed an open transaction whose last query inserted processing evidence. External SQL saw A=2, ordinal 0, no target receipt/processing row. SIGKILL gave exit137/OOMfalse. The same committed state remained afterward, the transaction disappeared, and target partition 0 offset 0 remained uncommitted. Restart applied it once and reached the full oracle. |
| A4 | After commit/pre-ack, external SQL saw A=-1, ordinal 1, one target receipt/processing row and no open target transaction. Offset 0 remained uncommitted. SIGKILL gave exit137/OOMfalse, preserving those observations. Restart logged target redelivery=true, with one effect and full oracle equality. |
| A5 | Fresh schema/group rebuilt 22 retained A4 records. Original before/after and rebuilt canonical snapshot SHA256 all equalled `6b7eedd8b641d301e6d83fa724c2107a54d89c686e0174750c1361456417adf1`. Original group offsets stayed unchanged. SQL and CLI agreed. |
| A6 | Separate C+4 samples: two injected transient failures succeeded on attempt three. Three injected failures stopped with exit1, C=0, no receipt/processing row and unadvanced offset; one explicit fault-free restart reached C=4 once. Attempt-start gaps were 0.365/0.526 s and 0.291/0.596 s, including SQL/rollback overhead around 100/500 ms backoffs. |
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

Private ignored evidence: `artifacts/r90cac148f56b-verification` contains aggregate
`acceptance.json`, copied JUnit reports, effective POM/dependency tree, image inspect,
versions and source hashes. `artifacts/r90cac148f56b-study` contains runtime `run.json`,
acknowledged manifests, SQL/offset snapshots, barrier/kill/restart/retry logs and cleanup
inventory. Both have SHA256 indexes. Credentials remain in the private ignored environment
file. Later measured-prose edits do not change tested runtime inputs.

Retained canonical volumes: `p22_r90cac148f56b_kafka_data` and
`p22_r90cac148f56b_postgres_data`. Earlier smokes `r069a4941ee2f` and `rb048d787f443`
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
