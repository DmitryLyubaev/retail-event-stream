package dev.dmitry.retailstream.kafka;

import static org.junit.jupiter.api.Assertions.*;

import dev.dmitry.retailstream.contract.RawRecord;
import dev.dmitry.retailstream.projection.ProcessingResult;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.springframework.dao.TransientDataAccessResourceException;

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
}
