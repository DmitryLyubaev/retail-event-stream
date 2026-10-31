package dev.dmitry.retailstream.kafka;

import static org.junit.jupiter.api.Assertions.*;

import dev.dmitry.retailstream.contract.RawRecord;
import dev.dmitry.retailstream.fixture.FrozenFixture;
import dev.dmitry.retailstream.projection.*;
import dev.dmitry.retailstream.support.TestBroker;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.*;
import org.springframework.dao.TransientDataAccessResourceException;
import tools.jackson.databind.json.JsonMapper;

class KafkaPipelineIT {
  @org.junit.jupiter.params.ParameterizedTest
  @org.junit.jupiter.params.provider.ValueSource(ints = {2, 3})
  void connection_opening_failures_use_budget_and_preserve_offsets(int failures) {
    var cfg = config();
    new SchemaManager(broker.database.dataSource).createFresh(cfg, fixture.seed());
    class ConnectionGate extends org.springframework.jdbc.datasource.DelegatingDataSource {
      int remaining;
      int attempts;
      boolean armed;

      ConnectionGate() {
        super(broker.database.dataSource);
      }

      @Override
      public java.sql.Connection getConnection() throws java.sql.SQLException {
        if (armed) {
          attempts++;
          if (remaining-- > 0)
            throw new java.sql.SQLException("injected connection opening refusal", "08001");
        }
        return super.getConnection();
      }
    }
    var source = new ConnectionGate();
    var guarded = new ProjectionStore(source, cfg, fixture.seed().keySet(), ProcessingHooks.NONE);
    var healthy = store(cfg, ProcessingHooks.NONE);
    try (var runtime =
        new KafkaRuntime(
            broker.container.getBootstrapServers(),
            fixture.sha256(),
            guarded,
            ProcessingHooks.NONE)) {
      runtime.createTopic(cfg.topic());
      var manifest =
          new FixtureProducer(runtime)
              .publishRecords(List.of(fixture.records().get(2)), fixture.sha256(), cfg.topic());
      source.remaining = failures;
      source.armed = true;
      if (failures == 2) assertDoesNotThrow(() -> runtime.consumeTo(cfg, manifest.boundary()));
      else assertThrows(RuntimeException.class, () -> runtime.consumeTo(cfg, manifest.boundary()));
      assertEquals(3, source.attempts);
      source.armed = false;
      var snapshot = new JsonMapper().readTree(healthy.snapshot().canonicalJson());
      assertEquals(failures == 2 ? 1 : 0, snapshot.get("receipts").size());
      assertEquals(failures == 2 ? 1 : 0, snapshot.get("processing").size());
      assertEquals(
          failures == 2 ? 4 : 0, snapshot.get("inventory").get(2).get("balance").longValue());
      Long position = runtime.committed(cfg.groupId(), cfg.topic()).get(2);
      if (failures == 2) assertEquals(1L, position);
      else {
        assertTrue(position == null || position == 0);
        try (var recovery =
            new KafkaRuntime(
                broker.container.getBootstrapServers(),
                fixture.sha256(),
                healthy,
                ProcessingHooks.NONE)) {
          recovery.consumeTo(cfg, manifest.boundary());
          assertEquals(1L, recovery.committed(cfg.groupId(), cfg.topic()).get(2));
          assertEquals(
              1,
              new JsonMapper().readTree(healthy.snapshot().canonicalJson()).get("receipts").size());
        }
      }
    }
  }

  @Test
  void rejection_write_failure_does_not_ack() {
    var cfg = config();
    new SchemaManager(broker.database.dataSource).createFresh(cfg, fixture.seed());
    var attempts = new AtomicInteger();
    ProcessingHooks hooks =
        new ProcessingHooks() {
          @Override
          public void beforeCommit(RawRecord r) {
            attempts.incrementAndGet();
            throw new TransientDataAccessResourceException("injected rejection write failure");
          }
        };
    var failing = store(cfg, hooks);
    SourceBoundary target;
    try (var runtime =
        new KafkaRuntime(
            broker.container.getBootstrapServers(), fixture.sha256(), failing, hooks)) {
      runtime.createTopic(cfg.topic());
      var manifest =
          new FixtureProducer(runtime)
              .publishRecords(List.of(fixture.records().get(17)), fixture.sha256(), cfg.topic());
      target = manifest.boundary();
      assertThrows(RuntimeException.class, () -> runtime.consumeTo(cfg, target));
      assertEquals(3, attempts.get());
      assertEquals(
          0,
          new JsonMapper().readTree(failing.snapshot().canonicalJson()).get("processing").size());
      Long committed = runtime.committed(cfg.groupId(), cfg.topic()).get(0);
      assertTrue(committed == null || committed == 0);
    }
    var healthy = store(cfg, ProcessingHooks.NONE);
    try (var runtime =
        new KafkaRuntime(
            broker.container.getBootstrapServers(),
            fixture.sha256(),
            healthy,
            ProcessingHooks.NONE)) {
      runtime.consumeTo(cfg, target);
      assertEquals(
          "REJECTED",
          new JsonMapper()
              .readTree(healthy.snapshot().canonicalJson())
              .get("processing")
              .get(0)
              .get("disposition")
              .asString());
    }
  }

  @Test
  void ack_failure_after_database_commit_is_recoverable() {
    var cfg = config();
    new SchemaManager(broker.database.dataSource).createFresh(cfg, fixture.seed());
    var healthy = store(cfg, ProcessingHooks.NONE);
    try (var runtime =
        new KafkaRuntime(
            broker.container.getBootstrapServers(),
            fixture.sha256(),
            healthy,
            ProcessingHooks.NONE)) {
      runtime.createTopic(cfg.topic());
      var manifest =
          new FixtureProducer(runtime)
              .publishRecords(List.of(fixture.records().get(0)), fixture.sha256(), cfg.topic());
      try (var consumer =
          new org.apache.kafka.clients.consumer.KafkaConsumer<byte[], byte[]>(
              runtime.consumerProperties(cfg.groupId()))) {
        consumer.subscribe(List.of(cfg.topic()));
        org.apache.kafka.clients.consumer.ConsumerRecord<byte[], byte[]> record = null;
        long deadline = System.nanoTime() + java.time.Duration.ofSeconds(30).toNanos();
        while (record == null && System.nanoTime() < deadline)
          for (var next : consumer.poll(java.time.Duration.ofMillis(100))) record = next;
        assertNotNull(record);
        var delivered = record;
        var listener =
            new ProjectionListener(
                healthy,
                ProcessingHooks.NONE,
                new java.util.concurrent.atomic.AtomicReference<RuntimeException>());
        assertThrows(
            org.apache.kafka.common.KafkaException.class,
            () ->
                listener.onMessage(
                    delivered,
                    () -> {
                      throw new org.apache.kafka.common.KafkaException(
                          "injected acknowledgement callback failure");
                    }));
        assertEquals(
            1,
            new JsonMapper().readTree(healthy.snapshot().canonicalJson()).get("receipts").size());
        Long committed = runtime.committed(cfg.groupId(), cfg.topic()).get(0);
        assertTrue(committed == null || committed == 0);
      }
      var before = healthy.snapshot();
      runtime.consumeTo(cfg, manifest.boundary());
      assertEquals(before, healthy.snapshot());
      assertEquals(1L, runtime.committed(cfg.groupId(), cfg.topic()).get(0));
    }
  }

  static TestBroker broker;
  final FrozenFixture fixture = FrozenFixture.load(Path.of("fixtures"));

  @BeforeAll
  static void start() {
    broker = new TestBroker();
  }

  @AfterAll
  static void stop() {
    if (broker != null) broker.close();
  }

  ProjectionConfig config() {
    String id = UUID.randomUUID().toString().replace("-", "").substring(0, 12);
    return new ProjectionConfig(
        "p22_k" + id, "p22.kafka." + id, "p22_g" + id, fixture.sha256(), fixture.seedSha256());
  }

  ProjectionStore store(ProjectionConfig cfg, ProcessingHooks hooks) {
    return new ProjectionStore(broker.database.dataSource, cfg, fixture.seed().keySet(), hooks);
  }

  @Test
  void normal_fixture_matches_oracle() {
    var cfg = config();
    new SchemaManager(broker.database.dataSource).createFresh(cfg, fixture.seed());
    var store = store(cfg, ProcessingHooks.NONE);
    try (var runtime =
        new KafkaRuntime(
            broker.container.getBootstrapServers(),
            fixture.sha256(),
            store,
            ProcessingHooks.NONE)) {
      runtime.createTopic(cfg.topic());
      var manifest = new FixtureProducer(runtime).publish(fixture, cfg.topic());
      assertEquals(22, manifest.records().size());
      runtime.consumeTo(cfg, manifest.boundary());
      var json = new JsonMapper().readTree(store.snapshot().canonicalJson());
      assertEquals(12, json.get("receipts").size());
      assertEquals(22, json.get("processing").size());
      var counts = new TreeMap<String, Integer>();
      int late = 0;
      for (var row : json.get("processing")) {
        counts.merge(row.get("disposition").asString(), 1, Integer::sum);
        if (row.get("lateEvent").booleanValue()) late++;
      }
      assertEquals(Map.of("APPLIED", 12, "DUPLICATE", 3, "REJECTED", 7), counts);
      assertEquals(1, late);
      assertEquals(3, json.get("inventory").get(0).get("balance").longValue());
      assertEquals(4, json.get("inventory").get(1).get("balance").longValue());
      assertEquals(2, json.get("inventory").get(2).get("balance").longValue());
      var rows = new HashMap<String, tools.jackson.databind.JsonNode>();
      for (var row : json.get("processing"))
        rows.put(row.get("partition").intValue() + ":" + row.get("offset").longValue(), row);
      for (var expected : fixture.expected().get("records")) {
        var ack = manifest.records().get(expected.get("index").intValue());
        var actual = rows.get(ack.partition() + ":" + ack.offset());
        for (String field : List.of("disposition", "postBalance", "ordinal", "lateEvent"))
          assertEquals(expected.get(field), actual.get(field));
      }
      assertEquals(manifest.boundary().end(), runtime.committed(cfg.groupId(), cfg.topic()));
    }
  }

  @Test
  void poison_then_valid_continues() {
    var cfg = config();
    new SchemaManager(broker.database.dataSource).createFresh(cfg, fixture.seed());
    var store = store(cfg, ProcessingHooks.NONE);
    try (var runtime =
        new KafkaRuntime(
            broker.container.getBootstrapServers(),
            fixture.sha256(),
            store,
            ProcessingHooks.NONE)) {
      runtime.createTopic(cfg.topic());
      var manifest =
          new FixtureProducer(runtime)
              .publishRecords(
                  List.of(fixture.records().get(17), fixture.records().get(0)),
                  fixture.sha256(),
                  cfg.topic());
      assertEquals(2, manifest.records().size());
      runtime.consumeTo(cfg, manifest.boundary());
      var json = new JsonMapper().readTree(store.snapshot().canonicalJson());
      assertEquals("REJECTED", json.get("processing").get(0).get("disposition").asString());
      assertEquals("APPLIED", json.get("processing").get(1).get("disposition").asString());
      assertEquals(-1, json.get("inventory").get(0).get("balance").longValue());
    }
  }

  @Test
  void persistent_database_failure_stops_consumer() {
    var cfg = config();
    new SchemaManager(broker.database.dataSource).createFresh(cfg, fixture.seed());
    var attempts = new AtomicInteger();
    ProcessingHooks hooks =
        new ProcessingHooks() {
          @Override
          public void beforeCommit(RawRecord r) {
            attempts.incrementAndGet();
            throw new TransientDataAccessResourceException("injected database failure");
          }
        };
    var failing = store(cfg, hooks);
    SourceBoundary boundary;
    try (var runtime =
        new KafkaRuntime(
            broker.container.getBootstrapServers(), fixture.sha256(), failing, hooks)) {
      runtime.createTopic(cfg.topic());
      var manifest =
          new FixtureProducer(runtime)
              .publishRecords(List.of(fixture.records().get(0)), fixture.sha256(), cfg.topic());
      assertEquals(1, manifest.records().size());
      boundary = manifest.boundary();
      assertThrows(RuntimeException.class, () -> runtime.consumeTo(cfg, boundary));
      assertEquals(3, attempts.get());
      assertEquals(
          0, new JsonMapper().readTree(failing.snapshot().canonicalJson()).get("receipts").size());
      Long committed = runtime.committed(cfg.groupId(), cfg.topic()).get(0);
      assertTrue(committed == null || committed == 0);
    }
    var healthy = store(cfg, ProcessingHooks.NONE);
    try (var runtime =
        new KafkaRuntime(
            broker.container.getBootstrapServers(),
            fixture.sha256(),
            healthy,
            ProcessingHooks.NONE)) {
      runtime.consumeTo(cfg, boundary);
      assertEquals(
          1, new JsonMapper().readTree(healthy.snapshot().canonicalJson()).get("receipts").size());
    }
  }
}
