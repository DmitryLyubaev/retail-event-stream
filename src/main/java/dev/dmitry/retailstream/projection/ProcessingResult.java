package dev.dmitry.retailstream.projection;

public record ProcessingResult(
    Disposition disposition,
    String reason,
    boolean lateEvent,
    Long postBalance,
    Long ordinal,
    boolean redelivery) {
  public enum Disposition {
    APPLIED,
    DUPLICATE,
    REJECTED
  }
}
