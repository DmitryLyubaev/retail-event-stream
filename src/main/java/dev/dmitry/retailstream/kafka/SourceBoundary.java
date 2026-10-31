package dev.dmitry.retailstream.kafka;

import java.util.Map;
import java.util.Set;

public record SourceBoundary(
    String topic, Map<Integer, Long> beginning, Map<Integer, Long> end, String fixtureSha256) {
  public SourceBoundary {
    beginning = Map.copyOf(beginning);
    end = Map.copyOf(end);
    if (topic == null
        || !topic.startsWith("p22.")
        || fixtureSha256 == null
        || !fixtureSha256.matches("[0-9a-f]{64}")
        || !beginning.keySet().equals(Set.of(0, 1, 2))
        || !end.keySet().equals(Set.of(0, 1, 2)))
      throw new IllegalArgumentException("Invalid source boundary");
    for (int p = 0; p < 3; p++)
      if (beginning.get(p) < 0 || end.get(p) < beginning.get(p))
        throw new IllegalArgumentException("Invalid offset range");
  }
}
