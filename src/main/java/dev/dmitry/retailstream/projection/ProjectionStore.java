package dev.dmitry.retailstream.projection;

import static dev.dmitry.retailstream.projection.ProcessingResult.Disposition.*;

import dev.dmitry.retailstream.contract.EventContract;
import dev.dmitry.retailstream.contract.RawIdentity;
import dev.dmitry.retailstream.contract.RawRecord;
import java.time.Instant;
import java.util.*;
import java.util.Set;
import javax.sql.DataSource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.support.JdbcTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import tools.jackson.databind.json.JsonMapper;

public final class ProjectionStore {
  private final ProjectionConfig config;
  private final JdbcTemplate jdbc;
  private final TransactionTemplate transaction;
  private final EventContract contract;
  private final ProcessingHooks hooks;

  public ProjectionStore(
      DataSource dataSource, ProjectionConfig config, Set<String> products, ProcessingHooks hooks) {
    new SchemaManager(dataSource).requireMatching(config);
    this.config = config;
    this.jdbc = new JdbcTemplate(dataSource);
    this.transaction = new TransactionTemplate(new JdbcTransactionManager(dataSource));
    transaction.setTimeout(5);
    this.contract = new EventContract(products);
    this.hooks = Objects.requireNonNull(hooks);
  }

  public ProcessingResult process(RawRecord record) {
    if (!record.topic().equals(config.topic()) || record.partition() > 2)
      throw new IllegalArgumentException("Record is outside projection source");
    return Objects.requireNonNull(transaction.execute(status -> processTransaction(record)));
  }

  private ProcessingResult processTransaction(RawRecord record) {
    String fingerprint = RawIdentity.digest(record.key(), record.value());
    var previous =
        jdbc.queryForList(
            "select * from "
                + config.table("processing")
                + " where topic=? and partition_id=? and record_offset=?",
            record.topic(),
            record.partition(),
            record.offset());
    if (!previous.isEmpty()) {
      var row = previous.getFirst();
      if (!fingerprint.equals(row.get("fingerprint"))
          || !Arrays.equals(record.key(), (byte[]) row.get("raw_key"))
          || !Arrays.equals(record.value(), (byte[]) row.get("raw_value")))
        throw new IllegalStateException("Payload changed at a committed log coordinate");
      return new ProcessingResult(
          ProcessingResult.Disposition.valueOf((String) row.get("disposition")),
          (String) row.get("reason"),
          (Boolean) row.get("late_event"),
          (Long) row.get("post_balance"),
          (Long) row.get("ordinal"),
          true);
    }
    var validation = contract.validate(record.key(), record.value());
    EventContract.Event event = null;
    ProcessingResult result;
    if (validation instanceof EventContract.Invalid invalid)
      result = new ProcessingResult(REJECTED, invalid.code(), false, null, null, false);
    else {
      event = ((EventContract.Valid) validation).event();
      var inventory =
          jdbc.queryForMap(
              "select * from " + config.table("inventory") + " where product_id=? for update",
              event.productId());
      var receipts =
          jdbc.queryForList(
              "select * from " + config.table("receipts") + " where product_id=? and event_id=?",
              event.productId(),
              event.eventId());
      if (!receipts.isEmpty()) {
        var receipt = receipts.getFirst();
        boolean equal =
            fingerprint.equals(receipt.get("fingerprint"))
                && Arrays.equals(record.key(), (byte[]) receipt.get("raw_key"))
                && Arrays.equals(record.value(), (byte[]) receipt.get("raw_value"));
        result =
            new ProcessingResult(
                equal ? DUPLICATE : REJECTED,
                equal ? null : "CONFLICTING_ID",
                false,
                null,
                null,
                false);
      } else {
        long delta = event.type() == EventContract.Kind.SALE ? -event.quantity() : event.quantity();
        Long next = null;
        try {
          next = Math.addExact((Long) inventory.get("balance"), delta);
        } catch (ArithmeticException ex) {
          /* Rejection is persisted without an effect. */
        }
        if (next == null)
          result = new ProcessingResult(REJECTED, "OVERFLOW", false, null, null, false);
        else {
          long ordinal = Math.addExact((Long) inventory.get("ordinal"), 1L);
          String maxText = (String) inventory.get("max_occurred_at");
          Instant maximum = maxText == null ? null : Instant.parse(maxText);
          boolean late = maximum != null && event.occurredAt().isBefore(maximum);
          Instant updated =
              maximum == null || event.occurredAt().isAfter(maximum) ? event.occurredAt() : maximum;
          jdbc.update(
              "update "
                  + config.table("inventory")
                  + " set balance=?,ordinal=?,max_occurred_at=? where product_id=?",
              next,
              ordinal,
              updated.toString(),
              event.productId());
          jdbc.update(
              "insert into " + config.table("receipts") + " values (?,?,?,?,?,?)",
              event.productId(),
              event.eventId(),
              record.key(),
              record.value(),
              fingerprint,
              delta);
          result = new ProcessingResult(APPLIED, null, late, next, ordinal, false);
        }
      }
    }
    jdbc.update(
        "insert into " + config.table("processing") + " values (?,?,?,?,?,?,?,?,?,?,?,?,?)",
        record.topic(),
        record.partition(),
        record.offset(),
        event == null ? null : event.productId(),
        event == null ? null : event.eventId(),
        record.key(),
        record.value(),
        fingerprint,
        result.disposition().name(),
        result.reason(),
        result.lateEvent(),
        result.postBalance(),
        result.ordinal());
    hooks.beforeCommit(record);
    return result;
  }

  public ProjectionSnapshot snapshot() {
    var result = new TreeMap<String, Object>();
    result.put(
        "inventory",
        read(
            "select product_id as \"productId\",balance,(balance<0) as \"negativeStock\",ordinal,max_occurred_at as \"maxOccurredAt\" from "
                + config.table("inventory")
                + " order by product_id"));
    result.put(
        "receipts",
        read(
            "select product_id as \"productId\",event_id as \"eventId\",raw_key as \"rawKey\",raw_value as \"rawValue\",delta from "
                + config.table("receipts")
                + " order by product_id,event_id"));
    result.put(
        "processing",
        read(
            "select topic,partition_id as partition,record_offset as offset,product_id as \"productId\",event_id as \"eventId\",raw_key as \"rawKey\",raw_value as \"rawValue\",fingerprint,disposition,reason,late_event as \"lateEvent\",post_balance as \"postBalance\",ordinal from "
                + config.table("processing")
                + " order by topic,partition_id,record_offset"));
    return new ProjectionSnapshot(new JsonMapper().writeValueAsString(result));
  }

  private List<Map<String, Object>> read(String sql) {
    return jdbc.queryForList(sql).stream()
        .map(
            row -> {
              Map<String, Object> normalized = new TreeMap<>();
              row.forEach(
                  (key, value) ->
                      normalized.put(
                          key,
                          value instanceof byte[] bytes
                              ? Base64.getEncoder().encodeToString(bytes)
                              : value instanceof UUID id ? id.toString() : value));
              return normalized;
            })
        .toList();
  }
}
