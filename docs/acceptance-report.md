# Local acceptance report, 31 October 2026

Final clean-source A1-A7 measurement is pending. This file will be replaced with the
observed results after the canonical run, retaining failed runs rather than overwriting
their raw evidence.

Interim evidence: Task 5 source `41cbd1a` passed 39 Java unit, 25 actual Kafka/PostgreSQL
integration and 19 Python checks, zero failures/errors/skips. Disposable postcommit
smoke `rb048d787f443` passed in 90.137 seconds including owned cleanup: one C+4 effect,
one receipt/processing row, unadvanced Kafka offset, intentional SIGKILL exit137 with
OOM false, and fault-free restart redelivery without another effect. Its actual dirty
source fingerprint, JAR and image are preserved in ignored artifacts. This is not the
final A1-A7 result. Owned containers stopped and two smoke volumes were retained.

The bounded scope excludes broker restart/failover, log loss, sustained load, multi-node
operation, multiple consumers/producers and real network acknowledgement outages. No
production scale, HA or distributed exactly-once delivery is claimed.
