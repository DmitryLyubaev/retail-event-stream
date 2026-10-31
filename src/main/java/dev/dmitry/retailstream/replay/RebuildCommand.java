package dev.dmitry.retailstream.replay;

import dev.dmitry.retailstream.kafka.SourceBoundary;
import dev.dmitry.retailstream.projection.*;
import java.util.Map;
import javax.sql.DataSource;

public final class RebuildCommand {
  private final DataSource dataSource;
  private final String bootstrap;
  private final ProcessingHooks hooks;

  public RebuildCommand(DataSource dataSource, String bootstrap) {
    this(dataSource, bootstrap, ProcessingHooks.NONE);
  }

  public RebuildCommand(DataSource dataSource, String bootstrap, ProcessingHooks hooks) {
    this.dataSource = dataSource;
    this.bootstrap = bootstrap;
    this.hooks = java.util.Objects.requireNonNull(hooks);
  }

  public ProjectionSnapshot run(
      ProjectionConfig freshTarget, SourceBoundary source, Map<String, Long> seed) {
    if (!freshTarget.topic().equals(source.topic())
        || !freshTarget.fixtureSha256().equals(source.fixtureSha256()))
      throw new IllegalArgumentException("Rebuild source/fixture differs from target");
    var schemas = new SchemaManager(dataSource);
    if (schemas.schemaExists(freshTarget))
      throw new IllegalArgumentException("Rebuild requires a fresh schema");
    try (var observer =
        new dev.dmitry.retailstream.kafka.KafkaRuntime(bootstrap, freshTarget.fixtureSha256())) {
      observer.requireUnchanged(source);
      if (observer.groupExists(freshTarget.groupId()))
        throw new IllegalArgumentException("Rebuild requires a fresh consumer group");
      observer.requireUnchanged(source);
      schemas.createFresh(freshTarget, seed);
    }
    var store = new ProjectionStore(dataSource, freshTarget, seed.keySet(), hooks);
    try (var runtime =
        new dev.dmitry.retailstream.kafka.KafkaRuntime(
            bootstrap, freshTarget.fixtureSha256(), store, hooks)) {
      runtime.consumeTo(freshTarget, source);
    }
    return store.snapshot();
  }
}
