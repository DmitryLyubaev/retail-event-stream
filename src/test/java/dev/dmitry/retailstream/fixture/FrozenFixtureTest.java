package dev.dmitry.retailstream.fixture;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Map;
import org.apache.kafka.common.utils.Utils;
import org.junit.jupiter.api.Test;

class FrozenFixtureTest {
  @Test
  void frozen_fixture_has_independent_expected_results() {
    var fixture = FrozenFixture.load(Path.of("fixtures"));
    assertEquals(22, fixture.records().size());
    assertEquals(Map.of("P22_A_5", 2L, "P22_B_0", 10L, "P22_C_2", 0L), fixture.seed());
    assertEquals(12, fixture.expected().get("counts").get("APPLIED").intValue());
    assertEquals(3, fixture.expected().get("counts").get("DUPLICATE").intValue());
    assertEquals(7, fixture.expected().get("counts").get("REJECTED").intValue());
    assertEquals(3, fixture.expected().get("balances").get("P22_A_5").intValue());
    assertEquals(4, fixture.expected().get("balances").get("P22_B_0").intValue());
    assertEquals(2, fixture.expected().get("balances").get("P22_C_2").intValue());
    assertEquals(-1, fixture.expected().get("records").get(0).get("postBalance").intValue());
    assertEquals(0, fixture.expected().get("records").get(3).get("postBalance").intValue());
    assertTrue(fixture.expected().get("records").get(5).get("lateEvent").booleanValue());
    assertNull(fixture.records().get(21).valueBase64());
    assertTrue(fixture.sha256().matches("[0-9a-f]{64}"));
  }

  @Test
  void chosen_keys_use_three_distinct_client_partitions() {
    var keys = java.util.List.of("P22_A_5", "P22_B_0", "P22_C_2");
    for (int part = 0; part < 3; part++)
      assertEquals(
          part,
          Utils.toPositive(Utils.murmur2(keys.get(part).getBytes(StandardCharsets.UTF_8))) % 3);
  }
}
