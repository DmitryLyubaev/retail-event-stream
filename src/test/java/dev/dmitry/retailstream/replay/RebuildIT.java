package dev.dmitry.retailstream.replay;

import static org.junit.jupiter.api.Assertions.*;

import dev.dmitry.retailstream.contract.RawRecord;
import dev.dmitry.retailstream.fixture.FrozenFixture;
import dev.dmitry.retailstream.kafka.*;
import dev.dmitry.retailstream.projection.*;
import dev.dmitry.retailstream.support.TestBroker;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.jdbc.core.JdbcTemplate;

class RebuildIT {
  static TestBroker broker;
  final FrozenFixture fixture = FrozenFixture.load(Path.of("fixtures"));
  ProjectionConfig live, target;
  ProjectionStore original;
  SourceBoundary source;
  KafkaRuntime runtime;
  ProjectionSnapshot originalBefore;
  Map<Integer, Long> offsetsBefore;

  @BeforeAll
  static void start() {
    broker = new TestBroker();
  }

  @AfterAll
  static void stop() {
    if (broker != null) broker.close();
  }

  @BeforeEach
  void setup() {
    String id = UUID.randomUUID().toString().replace("-", "").substring(0, 12);
    live =
        new ProjectionConfig(
            "p22_l" + id, "p22.replay." + id, "p22_l" + id, fixture.sha256(), fixture.seedSha256());
    target =
        new ProjectionConfig(
            "p22_r" + id, live.topic(), "p22_r" + id, fixture.sha256(), fixture.seedSha256());
    new SchemaManager(broker.database.dataSource).createFresh(live, fixture.seed());
    original =
        new ProjectionStore(
            broker.database.dataSource, live, fixture.seed().keySet(), ProcessingHooks.NONE);
    runtime =
        new KafkaRuntime(
            broker.container.getBootstrapServers(),
            fixture.sha256(),
            original,
            ProcessingHooks.NONE);
    runtime.createTopic(live.topic());
    source = new FixtureProducer(runtime).publish(fixture, live.topic()).boundary();
    runtime.consumeTo(live, source);
    originalBefore = original.snapshot();
    offsetsBefore = runtime.committed(live.groupId(), live.topic());
  }

  @AfterEach
  void close() {
    if (runtime != null) runtime.close();
  }

  boolean targetExists() {
    return new JdbcTemplate(broker.database.dataSource)
            .queryForObject(
                "select count(*) from information_schema.schemata where schema_name=?",
                Integer.class,
                target.schema())
        > 0;
  }

  RebuildCommand command() {
    return new RebuildCommand(broker.database.dataSource, broker.container.getBootstrapServers());
  }

  void originalUnchanged() {
    assertEquals(originalBefore, original.snapshot());
    assertEquals(offsetsBefore, runtime.committed(live.groupId(), live.topic()));
  }

  @Test
  void fresh_group_and_schema_rebuild() {
    var rebuilt = command().run(target, source, fixture.seed());
    assertEquals(originalBefore, rebuilt);
    originalUnchanged();
    assertTrue(targetExists());
    assertEquals(source.end(), runtime.committed(target.groupId(), target.topic()));
  }

  @Test
  void existing_schema_or_group_is_refused() {
    assertThrows(IllegalArgumentException.class, () -> command().run(live, source, fixture.seed()));
    var reusedGroup =
        new ProjectionConfig(
            target.schema(),
            target.topic(),
            live.groupId(),
            target.fixtureSha256(),
            target.seedSha256());
    assertThrows(
        IllegalArgumentException.class, () -> command().run(reusedGroup, source, fixture.seed()));
    assertFalse(targetExists());
    originalUnchanged();
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void missing_retention_or_changed_end_is_refused(boolean truncate) throws Exception {
    if (truncate) {
      try (var admin =
          AdminClient.create(Map.of("bootstrap.servers", broker.container.getBootstrapServers()))) {
        admin
            .deleteRecords(
                Map.of(new TopicPartition(live.topic(), 0), RecordsToDelete.beforeOffset(1)))
            .all()
            .get(10, TimeUnit.SECONDS);
      }
    } else appendOne();
    assertThrows(RuntimeException.class, () -> command().run(target, source, fixture.seed()));
    assertFalse(targetExists());
    originalUnchanged();
  }

  @Test
  void wrong_fixture_hash_is_refused() {
    var wrong =
        new SourceBoundary(source.topic(), source.beginning(), source.end(), "0".repeat(64));
    assertThrows(
        IllegalArgumentException.class, () -> command().run(target, wrong, fixture.seed()));
    assertFalse(targetExists());
    originalUnchanged();
  }

  @Test
  void unsafe_identifier_is_refused() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            new ProjectionConfig(
                "p22_x;delete from inventory",
                target.topic(),
                target.groupId(),
                target.fixtureSha256(),
                target.seedSha256()));
    assertFalse(targetExists());
    originalUnchanged();
  }

  @Test
  void source_change_after_preflight_aborts_and_preserves_original() {
    var triggered = new AtomicBoolean();
    ProcessingHooks hook =
        new ProcessingHooks() {
          @Override
          public void beforeCommit(RawRecord record) {
            if (triggered.compareAndSet(false, true)) appendOne();
          }
        };
    var command =
        new RebuildCommand(
            broker.database.dataSource, broker.container.getBootstrapServers(), hook);
    assertThrows(RuntimeException.class, () -> command.run(target, source, fixture.seed()));
    assertTrue(triggered.get());
    assertTrue(targetExists());
    originalUnchanged();
  }

  void appendOne() {
    var props = new HashMap<String, Object>();
    props.put("bootstrap.servers", broker.container.getBootstrapServers());
    props.put("key.serializer", ByteArraySerializer.class);
    props.put("value.serializer", ByteArraySerializer.class);
    props.put("delivery.timeout.ms", 10000);
    props.put("request.timeout.ms", 5000);
    try (var producer = new KafkaProducer<byte[], byte[]>(props)) {
      var record = fixture.records().get(0);
      producer
          .send(
              new ProducerRecord<byte[], byte[]>(
                  live.topic(),
                  Base64.getDecoder().decode(record.keyBase64()),
                  Base64.getDecoder().decode(record.valueBase64())))
          .get(10, TimeUnit.SECONDS);
    } catch (Exception ex) {
      throw new IllegalStateException("Controlled append failed", ex);
    }
  }
}
