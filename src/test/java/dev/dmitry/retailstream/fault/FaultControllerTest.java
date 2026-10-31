package dev.dmitry.retailstream.fault;

import static org.junit.jupiter.api.Assertions.*;

import dev.dmitry.retailstream.contract.RawRecord;
import dev.dmitry.retailstream.fixture.FrozenFixture;
import java.nio.file.Path;
import java.util.*;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.dao.TransientDataAccessResourceException;

class FaultControllerTest {
  private final FrozenFixture fixture = FrozenFixture.load(Path.of("fixtures"));
  private static final String TARGET = "P22_C_2/00000000-0000-0000-0000-000000000009";

  RawRecord raw(int index) {
    var f = fixture.records().get(index);
    return new RawRecord(
        "p22.test",
        2,
        0,
        Base64.getDecoder().decode(f.keyBase64()),
        Base64.getDecoder().decode(f.valueBase64()));
  }

  static Stream<Map<String, String>> invalid() {
    return Stream.of(
        Map.of("--fault-stage", "after-commit"),
        Map.of("--fault-event", TARGET),
        Map.of("--fail-first", "2"),
        Map.of("--fault-stage", "unknown", "--fault-event", TARGET),
        Map.of("--fail-first", "0", "--fault-event", TARGET),
        Map.of("--fail-first", "4", "--fault-event", TARGET),
        Map.of("--fail-first", "x", "--fault-event", TARGET),
        Map.of("--fail-first", "2", "--fault-stage", "after-commit", "--fault-event", TARGET),
        Map.of(
            "--fail-first", "2", "--fault-event", "UNKNOWN/00000000-0000-0000-0000-000000000009"),
        Map.of("--fail-first", "2", "--fault-event", "P22_C_2/not-a-uuid"));
  }

  @ParameterizedTest
  @MethodSource("invalid")
  void invalid_or_incomplete_fault_arguments_refuse_startup(Map<String, String> options) {
    assertThrows(
        IllegalArgumentException.class,
        () -> FaultController.fromOptions(options, fixture.seed().keySet()));
  }

  @Test
  void default_hooks_return_immediately() {
    var hooks = FaultController.fromOptions(Map.of(), fixture.seed().keySet());
    assertTimeoutPreemptively(
        java.time.Duration.ofMillis(200),
        () -> {
          hooks.beforeCommit(raw(2));
          hooks.afterCommit(raw(2));
        });
  }

  @Test
  void explicit_fail_first_is_bounded_and_targeted() {
    var hooks =
        FaultController.fromOptions(
            Map.of("--fail-first", "2", "--fault-event", TARGET), fixture.seed().keySet());
    assertDoesNotThrow(() -> hooks.beforeCommit(raw(0)));
    assertThrows(TransientDataAccessResourceException.class, () -> hooks.beforeCommit(raw(2)));
    assertThrows(TransientDataAccessResourceException.class, () -> hooks.beforeCommit(raw(2)));
    assertDoesNotThrow(() -> hooks.beforeCommit(raw(2)));
  }
}
