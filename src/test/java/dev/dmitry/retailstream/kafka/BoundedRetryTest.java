package dev.dmitry.retailstream.kafka;

import static org.junit.jupiter.api.Assertions.*;

import dev.dmitry.retailstream.contract.RawRecord;
import dev.dmitry.retailstream.projection.ProcessingResult;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.dao.TransientDataAccessResourceException;
import org.springframework.transaction.CannotCreateTransactionException;

class BoundedRetryTest {
  private final RawRecord record = new RawRecord("p22.test", 0, 0, null, null);

  @Test
  void transient_retry_budget() {
    var attempts = new AtomicInteger();
    var waits = new ArrayList<Long>();
    var retry =
        new BoundedRetry(
            r -> {
              attempts.incrementAndGet();
              throw new TransientDataAccessResourceException("injected");
            },
            waits::add);
    assertThrows(TransientDataAccessResourceException.class, () -> retry.execute(record));
    assertEquals(3, attempts.get());
    assertEquals(java.util.List.of(100L, 500L), waits);
  }

  @Test
  void transient_then_success() {
    var attempts = new AtomicInteger();
    var waits = new ArrayList<Long>();
    var retry =
        new BoundedRetry(
            r -> {
              if (attempts.incrementAndGet() < 3)
                throw new TransientDataAccessResourceException("injected");
              return new ProcessingResult(
                  ProcessingResult.Disposition.APPLIED, null, false, 1L, 1L, false);
            },
            waits::add);
    assertEquals(
        ProcessingResult.Disposition.APPLIED,
        assertDoesNotThrow(() -> retry.execute(record)).disposition());
    assertEquals(3, attempts.get());
    assertEquals(java.util.List.of(100L, 500L), waits);
  }

  @Test
  void unexpected_failure_has_one_attempt() {
    var attempts = new AtomicInteger();
    var waits = new ArrayList<Long>();
    var retry =
        new BoundedRetry(
            r -> {
              attempts.incrementAndGet();
              throw new IllegalStateException("invariant");
            },
            waits::add);
    assertThrows(IllegalStateException.class, () -> retry.execute(record));
    assertEquals(1, attempts.get());
    assertTrue(waits.isEmpty());
  }

  @ParameterizedTest
  @ValueSource(strings = {"08001", "08006", "53300", "57P03"})
  void transient_connection_start_is_retried(String sqlState) {
    var attempts = new AtomicInteger();
    var waits = new ArrayList<Long>();
    var failure =
        new CannotCreateTransactionException(
            "Cannot open connection",
            new java.sql.SQLException("injected connection refusal", sqlState));
    var retry =
        new BoundedRetry(
            r -> {
              attempts.incrementAndGet();
              throw failure;
            },
            waits::add);
    assertSame(
        failure, assertThrows(CannotCreateTransactionException.class, () -> retry.execute(record)));
    assertEquals(3, attempts.get());
    assertEquals(java.util.List.of(100L, 500L), waits);
  }

  @ParameterizedTest
  @ValueSource(strings = {"28P01", "3D000", "XX000"})
  void permanent_or_unknown_transaction_start_has_one_attempt(String sqlState) {
    var attempts = new AtomicInteger();
    var waits = new ArrayList<Long>();
    var retry =
        new BoundedRetry(
            r -> {
              attempts.incrementAndGet();
              throw new CannotCreateTransactionException(
                  "cannot start", new java.sql.SQLException("permanent or unknown", sqlState));
            },
            waits::add);
    assertThrows(CannotCreateTransactionException.class, () -> retry.execute(record));
    assertEquals(1, attempts.get());
    assertTrue(waits.isEmpty());
  }
}
