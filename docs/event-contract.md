# Synthetic event contract, version 1

Kafka keys are strict UTF-8 product IDs: `P22_A_5`, `P22_B_0`, `P22_C_2`, denoted A/B/C.
With this client's keyed partitioning and three partitions they map to 0/1/2. A valid
value is a UTF-8 JSON object of at most 65,536 bytes with exactly these six fields:

```json
{"eventId":"00000000-0000-0000-0000-000000000001","version":1,"productId":"P22_A_5","type":"SALE","quantity":3,"occurredAt":"2026-10-01T00:00:00Z"}
```

| Field | Rule |
|---|---|
| eventId | Canonical lowercase UUID string. |
| version | JSON integer 1. Unknown versions are rejected. |
| productId | Known product, exactly equal to the Kafka key. |
| type | Uppercase SALE, RETURN or ADJUSTMENT. |
| quantity | JSON integer. SALE/RETURN: 1..1000; ADJUSTMENT: -1000..1000 excluding zero. |
| occurredAt | Valid UTC timestamp ending in Z; fractional seconds allowed. |

Missing, extra or repeated fields, malformed JSON/UTF-8, decimal or string quantities,
unknown products/types, null keys and tombstones are durable rejections. SALE subtracts,
RETURN adds and ADJUSTMENT adds its signed quantity. Balances use PostgreSQL bigint;
an overflow is rejected atomically. Starting seed version 1 is A=2, B=10, C=0.
Negative stock remains a valid result and is flagged in readback.

Business identity is `(productId,eventId)` within a projection. Identical identity and
exact key/value bytes are a duplicate with no second effect. Changed bytes on that
identity conflict, even if their JSON meaning is equivalent. The same UUID on another
product is a different identity. Hashes use signed 32-bit big-endian lengths, then raw
bytes, with -1 for null and 0 for empty; raw bytes are retained as the identity authority.
Caller mutations cannot change retained byte arrays.

Each unique topic/partition/offset has a durable processing disposition. Redelivery of
that coordinate verifies the original raw bytes and does not append another disposition.
Receipts, effects and processing commit together. Duplicate/rejected rows have no new
business ordinal or post-balance. Rejections retain a reason and acknowledge only after
the processing row commits; there is no quarantine topic.

Processing follows each product's partition log order. Accepted events get a per-product
ordinal. An event earlier than the previously accepted maximum occurredAt is marked late;
it is still applied. This is observation, not a business sequence repair or global order.

The frozen files `seed.json`, `events.json` and independently hand-checked `expected.json`
include A's intermediate balances [-1,0,5,3], B's one late event and seven invalid/conflict
records. Fixture identity is SHA256 of each file's signed 64-bit big-endian byte length
and bytes in seed/events/expected order. The static oracle calls no application logic.
