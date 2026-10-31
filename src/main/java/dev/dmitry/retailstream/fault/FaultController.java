package dev.dmitry.retailstream.fault;

import dev.dmitry.retailstream.contract.RawRecord;
import dev.dmitry.retailstream.projection.ProcessingHooks;
import java.nio.charset.StandardCharsets;
import java.time.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import org.springframework.dao.TransientDataAccessResourceException;
import tools.jackson.databind.json.JsonMapper;

public final class FaultController implements ProcessingHooks {
  private final String stage, product, eventId;
  private final int failFirst;
  private final AtomicInteger attempts = new AtomicInteger();

  private FaultController(String stage, String product, String eventId, int failFirst) {
    this.stage = stage;
    this.product = product;
    this.eventId = eventId;
    this.failFirst = failFirst;
  }

  public static FaultController fromOptions(Map<String, String> options, Set<String> products) {
    String stage = options.get("--fault-stage"),
        target = options.get("--fault-event"),
        fail = options.get("--fail-first");
    if (stage == null && target == null && fail == null)
      return new FaultController(null, null, null, 0);
    if (target == null || (stage == null && fail == null) || (stage != null && fail != null))
      throw new IllegalArgumentException(
          "Fault mode and event must be supplied together, with one mode only");
    if (stage != null && !Set.of("before-commit", "after-commit").contains(stage))
      throw new IllegalArgumentException("Unknown fault stage");
    String[] parts = target.split("/", -1);
    if (parts.length != 2
        || !products.contains(parts[0])
        || !UUID.fromString(parts[1]).toString().equals(parts[1]))
      throw new IllegalArgumentException("Invalid fault event identity");
    int count = fail == null ? 0 : Integer.parseInt(fail);
    if (fail != null && (count < 1 || count > 3))
      throw new IllegalArgumentException("Fail-first must be between 1 and 3");
    return new FaultController(stage, parts[0], parts[1], count);
  }

  private boolean matches(RawRecord record) {
    if (product == null
        || record == null
        || !Arrays.equals(record.key(), product.getBytes(StandardCharsets.UTF_8))
        || record.value() == null) return false;
    try {
      return eventId.equals(new JsonMapper().readTree(record.value()).get("eventId").asString());
    } catch (RuntimeException ex) {
      return false;
    }
  }

  @Override
  public void beforeCommit(RawRecord record) {
    if (!matches(record)) return;
    if (failFirst > 0) {
      int attempt = attempts.incrementAndGet();
      if (attempt <= failFirst) {
        var message = identity(record);
        message.put("phase", "injected-database-failure");
        message.put("attempt", attempt);
        emit(message);
        throw new TransientDataAccessResourceException(
            "Explicit injected before-commit database failure");
      }
    }
    if ("before-commit".equals(stage)) barrier(record);
  }

  @Override
  public void afterCommit(RawRecord record) {
    if ("after-commit".equals(stage) && matches(record)) barrier(record);
  }

  private void barrier(RawRecord record) {
    var message = identity(record);
    message.put("phase", "fault-barrier");
    message.put("stage", stage);
    emit(message);
    long deadline = System.nanoTime() + Duration.ofSeconds(60).toNanos();
    try {
      while (System.nanoTime() < deadline) Thread.sleep(50);
    } catch (InterruptedException ex) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("Fault barrier interrupted", ex);
    }
    throw new IllegalStateException("Fault barrier expired without an actual process kill");
  }

  private Map<String, Object> identity(RawRecord record) {
    Map<String, Object> data = new TreeMap<>();
    data.put("runId", System.getenv().getOrDefault("P22_RUN_ID", "unrecorded"));
    data.put("sourceRevision", System.getenv().getOrDefault("P22_SOURCE_REVISION", "unrecorded"));
    data.put("jarSha256", System.getenv().getOrDefault("P22_JAR_SHA256", "unrecorded"));
    data.put("pid", ProcessHandle.current().pid());
    data.put("container", System.getenv().getOrDefault("HOSTNAME", "unrecorded"));
    data.put("topic", record.topic());
    data.put("partition", record.partition());
    data.put("offset", record.offset());
    data.put("productId", product);
    data.put("eventId", eventId);
    data.put("at", Instant.now().toString());
    return data;
  }

  private static void emit(Map<String, Object> message) {
    System.out.println(new JsonMapper().writeValueAsString(message));
    System.out.flush();
  }
}
