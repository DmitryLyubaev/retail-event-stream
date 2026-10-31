package dev.dmitry.retailstream.cli;

import static org.junit.jupiter.api.Assertions.*;

import dev.dmitry.retailstream.support.TestBroker;
import java.nio.file.*;
import java.util.UUID;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;
import tools.jackson.databind.json.JsonMapper;

class CommandKafkaIT {
  static TestBroker broker;

  @BeforeAll
  static void start() {
    broker = new TestBroker();
  }

  @AfterAll
  static void stop() {
    if (broker != null) broker.close();
  }

  @Test
  void publish_consume_and_snapshot_commands_use_the_same_manifest(@TempDir Path directory)
      throws Exception {
    String runId = "r" + UUID.randomUUID().toString().replace("-", "").substring(0, 12);
    Path manifest = directory.resolve("published.json");
    var command =
        new StreamCommands(broker.database.dataSource, broker.container.getBootstrapServers());
    String[] args = {
      "publish", "--run-id", runId, "--scenario", "normal", "--manifest", manifest.toString()
    };
    assertEquals(0, command.execute(args));
    assertTrue(Files.isRegularFile(manifest));
    String original = Files.readString(manifest);
    assertEquals(22, new JsonMapper().readTree(original).get("records").size());
    assertEquals(2, command.execute(args));
    assertEquals(original, Files.readString(manifest));
    args[0] = "init";
    assertEquals(0, command.execute(args));
    args[0] = "consume";
    assertEquals(0, command.execute(args));
    args[0] = "snapshot";
    assertEquals(0, command.execute(args));
    args[0] = "offsets";
    assertEquals(0, command.execute(args));
  }
}
