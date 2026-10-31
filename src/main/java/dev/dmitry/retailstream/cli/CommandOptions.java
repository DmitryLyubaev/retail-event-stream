package dev.dmitry.retailstream.cli;

import java.nio.file.Path;
import java.util.*;

public record CommandOptions(String command, Map<String, String> options) {
  public CommandOptions {
    options = Map.copyOf(options);
  }

  public static CommandOptions parse(String[] args) {
    if (args.length == 0
        || !Set.of("validate", "init", "snapshot", "publish", "consume", "offsets", "rebuild")
            .contains(args[0])
        || args.length % 2 != 1)
      throw new IllegalArgumentException("Expected a supported command and option/value pairs");
    var values = new HashMap<String, String>();
    Set<String> allowed =
        args[0].equals("validate")
            ? Set.of("--fixture")
            : args[0].equals("publish")
                ? Set.of("--fixture", "--run-id", "--scenario", "--manifest", "--sample")
                : args[0].equals("consume")
                    ? Set.of(
                        "--fixture",
                        "--run-id",
                        "--scenario",
                        "--projection",
                        "--manifest",
                        "--fault-stage",
                        "--fault-event",
                        "--fail-first")
                    : Set.of("--fixture", "--run-id", "--scenario", "--projection", "--manifest");
    for (int i = 1; i < args.length; i += 2)
      if (!allowed.contains(args[i])
          || args[i + 1].isBlank()
          || args[i + 1].startsWith("--")
          || values.putIfAbsent(args[i], args[i + 1]) != null)
        throw new IllegalArgumentException("Unknown, missing or duplicate option");
    var result = new CommandOptions(args[0], values);
    if (args[0].equals("validate")) result.required("--fixture");
    else {
      result.runId();
      result.scenario();
      result.projection();
      result.required("--manifest");
    }
    if (args[0].equals("publish")) {
      String sample = values.getOrDefault("--sample", "full");
      if (!Set.of("full", "retry").contains(sample)
          || (sample.equals("retry")
              && !Set.of("retryok", "retryfail").contains(result.scenario())))
        throw new IllegalArgumentException("Unknown or misplaced sample");
    }
    return result;
  }

  public String required(String name) {
    String value = options.get(name);
    if (value == null) throw new IllegalArgumentException("Missing " + name);
    return value;
  }

  public Path fixture() {
    return Path.of(options.getOrDefault("--fixture", "fixtures"));
  }

  public String runId() {
    String id = required("--run-id");
    if (!id.matches("r[0-9a-f]{12}")) throw new IllegalArgumentException("Unsafe run ID");
    return id;
  }

  public String scenario() {
    String value = required("--scenario");
    if (!Set.of("normal", "before", "after", "retryok", "retryfail").contains(value))
      throw new IllegalArgumentException("Unknown scenario");
    return value;
  }

  public String projection() {
    String value =
        options.getOrDefault("--projection", command.equals("rebuild") ? "rebuild" : "live");
    if (!Set.of("live", "rebuild").contains(value)
        || (command.equals("rebuild") && !value.equals("rebuild")))
      throw new IllegalArgumentException("Unknown projection");
    return value;
  }

  public String topic() {
    return "p22.events." + runId() + "." + scenario();
  }

  public String schema() {
    return "p22_" + runId() + "_" + scenario() + "_" + projection();
  }
}
