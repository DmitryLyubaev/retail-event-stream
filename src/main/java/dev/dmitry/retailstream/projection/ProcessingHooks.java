package dev.dmitry.retailstream.projection;

import dev.dmitry.retailstream.contract.RawRecord;

public interface ProcessingHooks {
  ProcessingHooks NONE = new ProcessingHooks() {};

  default void beforeCommit(RawRecord record) {}

  default void afterCommit(RawRecord record) {}
}
