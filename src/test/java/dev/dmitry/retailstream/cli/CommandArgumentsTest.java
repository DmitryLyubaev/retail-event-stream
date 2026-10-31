package dev.dmitry.retailstream.cli;

import static org.junit.jupiter.api.Assertions.*;

import dev.dmitry.retailstream.fixture.FrozenFixture;
import java.nio.file.*;
import java.sql.Connection;
import java.util.*;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.jdbc.datasource.AbstractDataSource;
import tools.jackson.databind.json.JsonMapper;

class CommandArgumentsTest {
  static final String RUN = "r0123456789abc";
  static final String TARGET = "P22_C_2/00000000-0000-0000-0000-000000000009";
  final StreamCommands commands =
      new StreamCommands(
          new AbstractDataSource() {
            @Override
            public Connection getConnection() {
              throw new AssertionError("Database reached by invalid command");
            }

            @Override
            public Connection getConnection(String user, String password) {
              return getConnection();
            }
          },
          "invalid.invalid:1");

  static Stream<Arguments> invalidSyntax() {
    return Stream.of(
            new String[] {},
            new String[] {"unknown"},
            new String[] {"init", "--run-id", "../unsafe"},
            new String[] {
              "snapshot",
              "--run-id",
              RUN,
              "--scenario",
              "normal",
              "--manifest",
              "missing.json",
              "--projection",
              "../unsafe"
            },
            new String[] {"validate", "--fixture", "fixtures", "--unsupported", "yes"},
            new String[] {"validate", "--fixture", "fixtures", "--fixture", "fixtures"})
        .map(value -> Arguments.of((Object) value));
  }

  @ParameterizedTest
  @MethodSource("invalidSyntax")
  void invalid_arguments_refuse_before_external_access(String[] args) {
    assertEquals(2, commands.execute(args));
  }

  Path manifest(Path folder, String topic) throws Exception {
    var file = folder.resolve("manifest.json");
    var fixture = FrozenFixture.load(Path.of("fixtures"));
    Files.writeString(
        file,
        new JsonMapper()
            .writeValueAsString(
                Map.of(
                    "boundary",
                    Map.of(
                        "topic",
                        topic,
                        "fixtureSha256",
                        fixture.sha256(),
                        "beginning",
                        Map.of("0", 0, "1", 0, "2", 0),
                        "end",
                        Map.of("0", 0, "1", 0, "2", 0)))));
    return file;
  }

  @Test
  void wrong_manifest_refuses_before_external_access(@TempDir Path folder) throws Exception {
    assertEquals(
        2,
        commands.execute(
            new String[] {
              "init",
              "--run-id",
              RUN,
              "--scenario",
              "normal",
              "--manifest",
              manifest(folder, "p22.events.other.normal").toString()
            }));
  }

  @Test
  void conflicting_fault_settings_refuse_before_external_access(@TempDir Path folder)
      throws Exception {
    assertEquals(
        2,
        commands.execute(
            new String[] {
              "consume",
              "--run-id",
              RUN,
              "--scenario",
              "normal",
              "--manifest",
              manifest(folder, "p22.events." + RUN + ".normal").toString(),
              "--fault-stage",
              "after-commit",
              "--fail-first",
              "2",
              "--fault-event",
              TARGET
            }));
  }
}
