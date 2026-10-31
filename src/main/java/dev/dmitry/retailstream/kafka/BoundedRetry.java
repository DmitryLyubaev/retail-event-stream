package dev.dmitry.retailstream.kafka;

import dev.dmitry.retailstream.contract.RawRecord;
import dev.dmitry.retailstream.projection.ProcessingResult;
import java.util.function.Function;

public final class BoundedRetry {
  @FunctionalInterface
  public interface Sleeper {
    void sleep(long millis) throws InterruptedException;
  }

  private final Function<RawRecord, ProcessingResult> operation;
  private final Sleeper sleeper;

  public BoundedRetry(Function<RawRecord, ProcessingResult> operation, Sleeper sleeper) {
    this.operation = operation;
    this.sleeper = sleeper;
  }

  public ProcessingResult execute(RawRecord record) {
    for (int attempt = 1; attempt <= 3; attempt++) {
      System.out.println(
          new tools.jackson.databind.json.JsonMapper()
              .writeValueAsString(
                  java.util.Map.of(
                      "phase",
                      "attempt",
                      "attempt",
                      attempt,
                      "topic",
                      record.topic(),
                      "partition",
                      record.partition(),
                      "offset",
                      record.offset(),
                      "at",
                      java.time.Instant.now().toString())));
      try {
        return operation.apply(record);
      } catch (RuntimeException ex) {
        if (!retryable(ex) || attempt == 3) throw ex;
        try {
          sleeper.sleep(attempt == 1 ? 100 : 500);
        } catch (InterruptedException interrupted) {
          Thread.currentThread().interrupt();
          throw new IllegalStateException("Retry interrupted", interrupted);
        }
      }
    }
    throw new IllegalStateException("Unreachable retry state");
  }

  private static boolean retryable(RuntimeException failure) {
    if (failure instanceof org.springframework.dao.TransientDataAccessException
        || failure instanceof org.springframework.dao.DataAccessResourceFailureException)
      return true;
    if (failure instanceof org.springframework.transaction.CannotCreateTransactionException) {
      for (Throwable cause = failure.getCause(); cause != null; cause = cause.getCause()) {
        if (cause instanceof java.sql.SQLException sql) {
          String state = sql.getSQLState();
          return state != null
              && (state.startsWith("08") || state.equals("53300") || state.equals("57P03"));
        }
      }
    }
    return false;
  }
}
