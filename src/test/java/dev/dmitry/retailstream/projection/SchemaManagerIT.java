package dev.dmitry.retailstream.projection;

import static org.junit.jupiter.api.Assertions.*;

import dev.dmitry.retailstream.cli.StreamCommands;
import dev.dmitry.retailstream.fixture.FrozenFixture;
import dev.dmitry.retailstream.support.TestDatabase;
import java.nio.file.*;
import java.util.*;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.jdbc.core.JdbcTemplate;
import tools.jackson.databind.json.JsonMapper;

class SchemaManagerIT {
  static TestDatabase db;
  final FrozenFixture fixture = FrozenFixture.load(Path.of("fixtures"));

  @BeforeAll
  static void start() {
    db = new TestDatabase();
  }

  @AfterAll
  static void stop() {
    if (db != null) db.close();
  }

  ProjectionConfig config() {
    String id = UUID.randomUUID().toString().replace("-", "").substring(0, 12);
    return new ProjectionConfig(
        "p22_s" + id, "p22.schema." + id, "p22_g" + id, fixture.sha256(), fixture.seedSha256());
  }

  @Test
  void seed_and_namespace_binding_are_immutable() {
    var cfg = config();
    var manager = new SchemaManager(db.dataSource);
    manager.createFresh(cfg, fixture.seed());
    var store =
        new ProjectionStore(db.dataSource, cfg, fixture.seed().keySet(), ProcessingHooks.NONE);
    var before = store.snapshot();
    assertThrows(IllegalArgumentException.class, () -> manager.createFresh(cfg, fixture.seed()));
    for (var bad :
        List.of(
            new ProjectionConfig(
                cfg.schema(), "p22.other", cfg.groupId(), cfg.fixtureSha256(), cfg.seedSha256()),
            new ProjectionConfig(
                cfg.schema(), cfg.topic(), "p22_other", cfg.fixtureSha256(), cfg.seedSha256()),
            new ProjectionConfig(
                cfg.schema(), cfg.topic(), cfg.groupId(), "0".repeat(64), cfg.seedSha256()),
            new ProjectionConfig(
                cfg.schema(), cfg.topic(), cfg.groupId(), cfg.fixtureSha256(), "1".repeat(64))))
      assertThrows(IllegalArgumentException.class, () -> manager.requireMatching(bad));
    new JdbcTemplate(db.dataSource)
        .update("update \"" + cfg.schema() + "\".projection_meta set seed_version=2");
    assertThrows(IllegalArgumentException.class, () -> manager.requireMatching(cfg));
    new JdbcTemplate(db.dataSource)
        .update("update \"" + cfg.schema() + "\".projection_meta set seed_version=1");
    manager.requireMatching(cfg);
    assertEquals(before, store.snapshot());
    assertThrows(
        IllegalArgumentException.class,
        () ->
            new ProjectionConfig(
                "p22_x;drop schema public",
                cfg.topic(),
                cfg.groupId(),
                cfg.fixtureSha256(),
                cfg.seedSha256()));
  }

  @Test
  void wrong_seed_is_refused_before_schema_creation() {
    var cfg = config();
    var changed = new HashMap<>(fixture.seed());
    changed.put("P22_A_5", 99L);
    assertThrows(
        IllegalArgumentException.class,
        () -> new SchemaManager(db.dataSource).createFresh(cfg, changed));
    assertEquals(
        0,
        new JdbcTemplate(db.dataSource)
            .queryForObject(
                "select count(*) from information_schema.schemata where schema_name=?",
                Integer.class,
                cfg.schema()));
  }

  @Test
  void cli_init_and_snapshot_resume(@TempDir Path directory) throws Exception {
    String id = "r" + UUID.randomUUID().toString().replace("-", "").substring(0, 12);
    Path manifest = directory.resolve("publish.json");
    var mapper = new JsonMapper();
    var boundary =
        Map.of(
            "topic",
            "p22.events." + id + ".normal",
            "fixtureSha256",
            fixture.sha256(),
            "beginning",
            Map.of("0", 0, "1", 0, "2", 0),
            "end",
            Map.of("0", 22, "1", 0, "2", 0));
    Files.writeString(
        manifest, mapper.writeValueAsString(Map.of("boundary", boundary, "records", List.of())));
    var command = new StreamCommands(db.dataSource);
    String[] args = {
      "init",
      "--run-id",
      id,
      "--scenario",
      "normal",
      "--projection",
      "live",
      "--manifest",
      manifest.toString()
    };
    assertEquals(0, command.execute(args));
    args[0] = "snapshot";
    assertEquals(0, command.execute(args));
    var cfg =
        new ProjectionConfig(
            "p22_" + id + "_normal_live",
            "p22.events." + id + ".normal",
            "p22_" + id + "_normal_live",
            fixture.sha256(),
            fixture.seedSha256());
    var store =
        new ProjectionStore(db.dataSource, cfg, fixture.seed().keySet(), ProcessingHooks.NONE);
    assertEquals(
        2,
        new JsonMapper()
            .readTree(store.snapshot().canonicalJson())
            .get("inventory")
            .get(0)
            .get("balance")
            .longValue());
    assertEquals(2, command.execute(new String[] {"init", "--run-id", "../unsafe"}));
  }
}
