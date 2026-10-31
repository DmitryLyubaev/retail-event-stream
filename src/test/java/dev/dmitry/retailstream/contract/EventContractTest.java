package dev.dmitry.retailstream.contract;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.*;

import java.util.Set;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

class EventContractTest {
  private static final String GOOD =
      "{\"eventId\":\"00000000-0000-0000-0000-000000000001\",\"version\":1,\"productId\":\"A\",\"type\":\"SALE\",\"quantity\":3,\"occurredAt\":\"2026-10-01T00:00:00Z\"}";
  private final EventContract contract = new EventContract(Set.of("A", "B", "C"));

  @Test
  void valid_sale_returns_valid_event() {
    var result =
        assertInstanceOf(
            EventContract.Valid.class,
            contract.validate("A".getBytes(UTF_8), GOOD.getBytes(UTF_8)));
    assertEquals(3, result.event().quantity());
    assertEquals(EventContract.Kind.SALE, result.event().type());
  }

  static Stream<String> invalidPayloads() {
    return Stream.of(
        "",
        "{",
        GOOD.replace("\"version\":1,", ""),
        GOOD.replace("\"version\":1", "\"version\":2"),
        GOOD.replace("\"version\":1", "\"version\":1,\"version\":1"),
        GOOD.replace("\"version\":1", "\"version\":1,\"extra\":true"),
        GOOD.replace("\"SALE\"", "\"UNKNOWN\""),
        GOOD.replace("\"A\"", "\"UNKNOWN\""),
        GOOD.replace(":3,", ":3.0,"),
        GOOD.replace(":3,", ":\"3\","),
        GOOD.replace(":3,", ":0,"),
        GOOD.replace(":3,", ":-1,"),
        GOOD.replace(":3,", ":1001,"),
        GOOD.replace("SALE", "ADJUSTMENT").replace(":3,", ":0,"),
        GOOD.replace("SALE", "ADJUSTMENT").replace(":3,", ":-1001,"),
        GOOD.replace("00:00:00Z", "00:00:00+10:00"),
        GOOD.replace("00000000-0000-0000-0000-000000000001", "0-0-0-0-1"),
        " ".repeat(65537));
  }

  @ParameterizedTest
  @MethodSource("invalidPayloads")
  void invalid_contract_is_rejected(String body) {
    var result =
        assertInstanceOf(
            EventContract.Invalid.class,
            contract.validate("A".getBytes(UTF_8), body.getBytes(UTF_8)));
    assertFalse(result.code().isBlank());
  }

  @Test
  void invalid_encoding_keys_and_tombstones_are_rejected() {
    for (byte[] value : new byte[][] {null, new byte[] {(byte) 0xc3, 0x28}}) {
      assertInstanceOf(EventContract.Invalid.class, contract.validate("A".getBytes(UTF_8), value));
    }
    assertInstanceOf(EventContract.Invalid.class, contract.validate(null, GOOD.getBytes(UTF_8)));
    assertInstanceOf(
        EventContract.Invalid.class, contract.validate("B".getBytes(UTF_8), GOOD.getBytes(UTF_8)));
    assertInstanceOf(
        EventContract.Invalid.class,
        contract.validate(new byte[] {(byte) 0xff}, GOOD.getBytes(UTF_8)));
  }

  @Test
  void raw_identity_is_unambiguous() {
    assertNotEquals(
        RawIdentity.digest(null, new byte[] {1}), RawIdentity.digest(new byte[0], new byte[] {1}));
    assertNotEquals(
        RawIdentity.digest(new byte[] {1}, new byte[] {2, 3}),
        RawIdentity.digest(new byte[] {1, 2}, new byte[] {3}));
  }

  @Test
  void raw_record_preserves_original_bytes() {
    byte[] key = {1};
    byte[] value = {2};
    var record = new RawRecord("topic", 0, 0, key, value);
    key[0] = 3;
    value[0] = 4;
    assertArrayEquals(new byte[] {1}, record.key());
    assertArrayEquals(new byte[] {2}, record.value());
    record.value()[0] = 5;
    assertArrayEquals(new byte[] {2}, record.value());
  }
}
