package dev.dmitry.retailstream.projection;

import static dev.dmitry.retailstream.projection.ProcessingResult.Disposition.*;
import static org.junit.jupiter.api.Assertions.*;

import dev.dmitry.retailstream.contract.RawRecord;
import dev.dmitry.retailstream.fixture.FrozenFixture;
import dev.dmitry.retailstream.support.TestDatabase;
import java.nio.file.Path;
import java.util.Base64;
import java.util.UUID;
import org.junit.jupiter.api.*;
import org.springframework.dao.TransientDataAccessResourceException;
import tools.jackson.databind.json.JsonMapper;

class ProjectionStoreIT {
  static Object call(java.lang.reflect.Method method, Object target, Object[] args)
      throws Throwable {
    try {
      return method.invoke(target, args);
    } catch (java.lang.reflect.InvocationTargetException error) {
      throw error.getCause();
    }
  }

  @Test
  void snapshot_is_coherent_when_a_writer_commits_between_reads() throws Exception {
    var afterFirstResult = new java.util.concurrent.atomic.AtomicReference<Runnable>();
    var interleaving =
        new org.springframework.jdbc.datasource.DelegatingDataSource(db.dataSource) {
          @Override
          public java.sql.Connection getConnection() throws java.sql.SQLException {
            var real = super.getConnection();
            return (java.sql.Connection)
                java.lang.reflect.Proxy.newProxyInstance(
                    getClass().getClassLoader(),
                    new Class<?>[] {java.sql.Connection.class},
                    (connection, method, args) -> {
                      var result = call(method, real, args);
                      if (method.getName().equals("createStatement")) {
                        var statement = (java.sql.Statement) result;
                        return java.lang.reflect.Proxy.newProxyInstance(
                            getClass().getClassLoader(),
                            new Class<?>[] {java.sql.Statement.class},
                            (ignored, operation, parameters) -> {
                              var value = call(operation, statement, parameters);
                              if (value instanceof java.sql.ResultSet rows)
                                return java.lang.reflect.Proxy.newProxyInstance(
                                    getClass().getClassLoader(),
                                    new Class<?>[] {java.sql.ResultSet.class},
                                    (ignoredRows, read, arguments) -> {
                                      var answer = call(read, rows, arguments);
                                      if (read.getName().equals("close")) {
                                        var action = afterFirstResult.getAndSet(null);
                                        if (action != null) action.run();
                                      }
                                      return answer;
                                    });
                              return value;
                            });
                      }
                      return result;
                    });
          }
        };
    var reader =
        new ProjectionStore(interleaving, config, fixture.seed().keySet(), ProcessingHooks.NONE);
    var committed = new java.util.concurrent.atomic.AtomicBoolean();
    afterFirstResult.set(
        () -> {
          store.process(raw(0, 0));
          committed.set(true);
        });
    var observed = new JsonMapper().readTree(reader.snapshot().canonicalJson());
    assertTrue(committed.get(), "Writer really committed between snapshot query results");
    assertEquals(2, observed.get("inventory").get(0).get("balance").longValue());
    assertEquals(
        0,
        observed.get("receipts").size(),
        "Readback must not combine old balance and new receipt");
    assertEquals(0, observed.get("processing").size());
    assertEquals(
        -1,
        new JsonMapper()
            .readTree(store.snapshot().canonicalJson())
            .get("inventory")
            .get(0)
            .get("balance")
            .longValue());
  }

  static TestDatabase db;
  final FrozenFixture fixture = FrozenFixture.load(Path.of("fixtures"));
  ProjectionConfig config;
  ProjectionStore store;

  @BeforeAll
  static void start() {
    db = new TestDatabase();
  }

  @AfterAll
  static void stop() {
    if (db != null) db.close();
  }

  @BeforeEach
  void fresh() {
    String id = UUID.randomUUID().toString().replace("-", "").substring(0, 12);
    config =
        new ProjectionConfig(
            "p22_t" + id, "p22.test." + id, "p22_g" + id, fixture.sha256(), fixture.seedSha256());
    new SchemaManager(db.dataSource).createFresh(config, fixture.seed());
    store =
        new ProjectionStore(db.dataSource, config, fixture.seed().keySet(), ProcessingHooks.NONE);
  }

  RawRecord raw(int index, long offset) {
    var f = fixture.records().get(index);
    return new RawRecord(
        config.topic(),
        0,
        offset,
        Base64.getDecoder().decode(f.keyBase64()),
        f.valueBase64() == null ? null : Base64.getDecoder().decode(f.valueBase64()));
  }

  @Test
  void sale_below_zero_and_return_clear_flag() {
    assertEquals(-1L, store.process(raw(0, 0)).postBalance());
    var first = new JsonMapper().readTree(store.snapshot().canonicalJson());
    assertTrue(first.get("inventory").get(0).get("negativeStock").booleanValue());
    assertEquals(0L, store.process(raw(3, 1)).postBalance());
    assertFalse(
        new JsonMapper()
            .readTree(store.snapshot().canonicalJson())
            .get("inventory")
            .get(0)
            .get("negativeStock")
            .booleanValue());
  }

  @Test
  void rollback_is_atomic() {
    var before = store.snapshot();
    var failing =
        new ProjectionStore(
            db.dataSource,
            config,
            fixture.seed().keySet(),
            new ProcessingHooks() {
              @Override
              public void beforeCommit(RawRecord record) {
                throw new TransientDataAccessResourceException("injected");
              }
            });
    assertThrows(TransientDataAccessResourceException.class, () -> failing.process(raw(0, 0)));
    assertEquals(before, store.snapshot());
  }

  @Test
  void duplicates_conflicts_and_redelivery() {
    assertEquals(APPLIED, store.process(raw(0, 0)).disposition());
    assertEquals(DUPLICATE, store.process(raw(4, 1)).disposition());
    assertEquals("CONFLICTING_ID", store.process(raw(15, 2)).reason());
    var before = store.snapshot();
    assertTrue(store.process(raw(0, 0)).redelivery());
    assertEquals(before, store.snapshot());
    assertThrows(IllegalStateException.class, () -> store.process(raw(15, 0)));
  }

  @Test
  void same_uuid_on_different_products_is_separate() {
    store.process(raw(0, 0));
    var a = raw(0, 0);
    var b =
        new RawRecord(
            config.topic(),
            1,
            0,
            "P22_B_0".getBytes(java.nio.charset.StandardCharsets.UTF_8),
            new String(a.value(), java.nio.charset.StandardCharsets.UTF_8)
                .replace("P22_A_5", "P22_B_0")
                .getBytes(java.nio.charset.StandardCharsets.UTF_8));
    assertEquals(APPLIED, store.process(b).disposition());
    var json = new JsonMapper().readTree(store.snapshot().canonicalJson());
    assertEquals(2, json.get("receipts").size());
    assertEquals(7, json.get("inventory").get(1).get("balance").longValue());
  }

  @Test
  void equivalent_json_with_changed_bytes_conflicts() {
    var original = raw(0, 0);
    store.process(original);
    var changed =
        new RawRecord(
            config.topic(),
            0,
            1,
            original.key(),
            (" " + new String(original.value(), java.nio.charset.StandardCharsets.UTF_8) + " ")
                .getBytes(java.nio.charset.StandardCharsets.UTF_8));
    assertEquals("CONFLICTING_ID", store.process(changed).reason());
    assertEquals(
        1, new JsonMapper().readTree(store.snapshot().canonicalJson()).get("receipts").size());
  }

  @Test
  void bigint_overflow_rejects_without_effect() {
    new org.springframework.jdbc.core.JdbcTemplate(db.dataSource)
        .update(
            "update \"" + config.schema() + "\".inventory set balance=? where product_id='P22_A_5'",
            Long.MAX_VALUE);
    assertEquals("OVERFLOW", store.process(raw(3, 0)).reason());
    var json = new JsonMapper().readTree(store.snapshot().canonicalJson());
    assertEquals(Long.MAX_VALUE, json.get("inventory").get(0).get("balance").longValue());
    assertEquals(0, json.get("receipts").size());
  }

  @Test
  void null_and_empty_raw_values_remain_distinct() {
    byte[] key = raw(0, 0).key();
    store.process(new RawRecord(config.topic(), 0, 0, key, null));
    store.process(new RawRecord(config.topic(), 0, 1, key, new byte[0]));
    var rows = new JsonMapper().readTree(store.snapshot().canonicalJson()).get("processing");
    assertTrue(rows.get(0).get("rawValue").isNull());
    assertEquals("", rows.get(1).get("rawValue").asString());
    assertNotEquals(rows.get(0).get("fingerprint"), rows.get(1).get("fingerprint"));
  }

  @Test
  void invalid_record_is_durable() {
    assertEquals(REJECTED, store.process(raw(17, 0)).disposition());
    var row = new JsonMapper().readTree(store.snapshot().canonicalJson()).get("processing").get(0);
    assertEquals(0, row.get("offset").longValue());
    assertTrue(row.get("eventId").isNull());
    assertFalse(row.get("reason").asString().isBlank());
    assertEquals("ew==", row.get("rawValue").asString());
  }

  @Test
  void nanosecond_timestamp_high_water_is_preserved() {
    var first = raw(0, 0);
    var second = raw(3, 1);
    byte[] v1 =
        new String(first.value(), java.nio.charset.StandardCharsets.UTF_8)
            .replace("00:00:00Z", "00:00:00.000000002Z")
            .getBytes(java.nio.charset.StandardCharsets.UTF_8);
    byte[] v2 =
        new String(second.value(), java.nio.charset.StandardCharsets.UTF_8)
            .replace("00:10:00Z", "00:00:00.000000001Z")
            .getBytes(java.nio.charset.StandardCharsets.UTF_8);
    assertFalse(store.process(new RawRecord(config.topic(), 0, 0, first.key(), v1)).lateEvent());
    assertTrue(store.process(new RawRecord(config.topic(), 0, 1, second.key(), v2)).lateEvent());
    assertEquals(
        "2026-10-01T00:00:00.000000002Z",
        new JsonMapper()
            .readTree(store.snapshot().canonicalJson())
            .get("inventory")
            .get(0)
            .get("maxOccurredAt")
            .asString());
  }
}
