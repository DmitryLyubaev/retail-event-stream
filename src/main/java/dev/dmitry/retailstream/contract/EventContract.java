package dev.dmitry.retailstream.contract;

import java.nio.ByteBuffer;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Set;
import java.util.UUID;
import tools.jackson.core.StreamReadFeature;
import tools.jackson.databind.json.JsonMapper;

public final class EventContract {
  public enum Kind {
    SALE,
    RETURN,
    ADJUSTMENT
  }

  public record Event(
      String productId, UUID eventId, int version, Kind type, int quantity, Instant occurredAt) {}

  public sealed interface Validation permits Valid, Invalid {}

  public record Valid(Event event) implements Validation {}

  public record Invalid(String code, String detail) implements Validation {}

  private final Set<String> products;
  private final JsonMapper mapper =
      JsonMapper.builder().enable(StreamReadFeature.STRICT_DUPLICATE_DETECTION).build();
  private static final Set<String> FIELDS =
      Set.of("eventId", "version", "productId", "type", "quantity", "occurredAt");

  public EventContract(Set<String> products) {
    this.products = Set.copyOf(products);
  }

  public Validation validate(byte[] key, byte[] value) {
    if (value == null) return new Invalid("TOMBSTONE", "Null payload");
    if (value.length > 65536) return new Invalid("OVERSIZE", "Payload exceeds 64 KiB");
    try {
      String productKey = decode(key);
      var node = mapper.readTree(decode(value));
      if (node == null || !node.isObject() || !Set.copyOf(node.propertyNames()).equals(FIELDS))
        return new Invalid("FIELDS", "Expected exactly the version 1 fields");
      if (!node.get("version").isIntegralNumber()
          || !node.get("version").canConvertToInt()
          || node.get("version").intValue() != 1)
        return new Invalid("VERSION", "Only integer version 1 is supported");
      for (String name : Set.of("eventId", "productId", "type", "occurredAt"))
        if (!node.get(name).isString()) return new Invalid("TYPE", "Expected string " + name);
      String product = node.get("productId").asString();
      if (!products.contains(product)) return new Invalid("PRODUCT", "Unknown product");
      if (!product.equals(productKey)) return new Invalid("KEY_MISMATCH", "Key and product differ");
      String idText = node.get("eventId").asString();
      UUID id = UUID.fromString(idText);
      if (!id.toString().equals(idText))
        return new Invalid("EVENT_ID", "Use canonical lower-case UUID");
      Kind kind = Kind.valueOf(node.get("type").asString());
      var quantity = node.get("quantity");
      if (!quantity.isIntegralNumber() || !quantity.canConvertToInt())
        return new Invalid("QUANTITY", "Expected bounded integer quantity");
      int q = quantity.intValue();
      if (q == 0 || q < -1000 || q > 1000 || (kind != Kind.ADJUSTMENT && q < 1))
        return new Invalid("QUANTITY", "Quantity is outside the event type range");
      String timestamp = node.get("occurredAt").asString();
      if (!timestamp.endsWith("Z")) return new Invalid("TIMESTAMP", "Use a UTC Z timestamp");
      Instant time = Instant.parse(timestamp);
      return new Valid(new Event(product, id, 1, kind, q, time));
    } catch (Exception ex) {
      return new Invalid("MALFORMED", ex.getClass().getSimpleName());
    }
  }

  private static String decode(byte[] bytes) throws java.nio.charset.CharacterCodingException {
    if (bytes == null) throw new IllegalArgumentException("Null key");
    return StandardCharsets.UTF_8
        .newDecoder()
        .onMalformedInput(CodingErrorAction.REPORT)
        .onUnmappableCharacter(CodingErrorAction.REPORT)
        .decode(ByteBuffer.wrap(bytes))
        .toString();
  }
}
