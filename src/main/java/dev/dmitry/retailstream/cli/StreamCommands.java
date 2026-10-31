package dev.dmitry.retailstream.cli;

import dev.dmitry.retailstream.contract.EventContract;
import dev.dmitry.retailstream.fixture.FrozenFixture;
import dev.dmitry.retailstream.kafka.*;
import dev.dmitry.retailstream.projection.*;
import java.io.IOException;
import java.nio.file.*;
import java.util.*;
import javax.sql.DataSource;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

public final class StreamCommands {
  private final DataSource injected;
  private final String injectedBootstrap;

  public StreamCommands() {
    this(null, null);
  }

  public StreamCommands(DataSource dataSource) {
    this(dataSource, null);
  }

  public StreamCommands(DataSource dataSource, String bootstrap) {
    this.injected = dataSource;
    this.injectedBootstrap = bootstrap;
  }

  public int execute(String[] args) {
    try {
      var options = CommandOptions.parse(args);
      FrozenFixture fixture;
      try {
        fixture = FrozenFixture.load(options.fixture());
      } catch (RuntimeException ex) {
        throw new IllegalArgumentException("Invalid frozen fixture", ex);
      }
      if (options.command().equals("validate")) return validate(fixture);
      Path manifestPath = Path.of(options.required("--manifest"));
      if (options.command().equals("publish")) {
        if (Files.exists(manifestPath))
          throw new IllegalArgumentException("Refusing to overwrite existing manifest");
        Path parent = manifestPath.toAbsolutePath().getParent();
        if (parent != null) Files.createDirectories(parent);
        try (var runtime = new KafkaRuntime(bootstrap(), fixture.sha256())) {
          runtime.createTopic(options.topic());
          var producer = new FixtureProducer(runtime);
          var manifest =
              options.options().getOrDefault("--sample", "full").equals("retry")
                  ? producer.publishRecords(
                      List.of(fixture.records().get(2)), fixture.sha256(), options.topic())
                  : producer.publish(fixture, options.topic());
          Files.writeString(
              manifestPath,
              new JsonMapper().writeValueAsString(manifest),
              StandardOpenOption.CREATE_NEW);
          System.out.println(new JsonMapper().writeValueAsString(manifest));
        }
        return 0;
      }
      var source = sourceBoundary(manifestPath);
      if (!options.topic().equals(source.topic())
          || !fixture.sha256().equals(source.fixtureSha256()))
        throw new IllegalArgumentException("Manifest differs from run/fixture");
      var config =
          new ProjectionConfig(
              options.schema(),
              options.topic(),
              options.schema(),
              fixture.sha256(),
              fixture.seedSha256());
      if (options.command().equals("offsets")) {
        try (var runtime = new KafkaRuntime(bootstrap(), fixture.sha256())) {
          var output = new TreeMap<String, Object>();
          output.put("groupId", config.groupId());
          output.put("committed", runtime.committed(config.groupId(), config.topic()));
          output.put("boundary", runtime.boundary(config.topic()));
          System.out.println(new JsonMapper().writeValueAsString(output));
        }
        return 0;
      }
      var hooks =
          options.command().equals("consume")
              ? dev.dmitry.retailstream.fault.FaultController.fromOptions(
                  options.options(), fixture.seed().keySet())
              : ProcessingHooks.NONE;
      var dataSource = injected == null ? environmentDataSource() : injected;
      if (options.command().equals("rebuild")) {
        var rebuilt =
            new dev.dmitry.retailstream.replay.RebuildCommand(dataSource, bootstrap())
                .run(config, source, fixture.seed());
        System.out.println(rebuilt.canonicalJson());
        return 0;
      }
      if (options.command().equals("init")) {
        new SchemaManager(dataSource).createFresh(config, fixture.seed());
        System.out.println(
            new JsonMapper()
                .writeValueAsString(Map.of("schema", config.schema(), "initialized", true)));
      } else {
        var store = new ProjectionStore(dataSource, config, fixture.seed().keySet(), hooks);
        if (options.command().equals("consume")) {
          try (var runtime = new KafkaRuntime(bootstrap(), fixture.sha256(), store, hooks)) {
            runtime.consumeTo(config, source);
            System.out.println(
                new JsonMapper()
                    .writeValueAsString(
                        Map.of(
                            "phase",
                            "caught-up",
                            "groupId",
                            config.groupId(),
                            "committed",
                            runtime.committed(config.groupId(), config.topic()))));
          }
        } else System.out.println(store.snapshot().canonicalJson());
      }
      return 0;
    } catch (IllegalArgumentException | IOException ex) {
      System.err.println(ex.getMessage());
      return 2;
    } catch (RuntimeException ex) {
      System.err.println(ex.getClass().getSimpleName() + ": " + ex.getMessage());
      return 1;
    }
  }

  private String bootstrap() {
    String value = injectedBootstrap == null ? System.getenv("P22_BOOTSTRAP") : injectedBootstrap;
    if (value == null || value.isBlank())
      throw new IllegalArgumentException("Set P22_BOOTSTRAP for the owned broker");
    return value;
  }

  public static SourceBoundary sourceBoundary(Path manifest) {
    return new JsonMapper().treeToValue(readBoundary(manifest), SourceBoundary.class);
  }

  public static JsonNode readBoundary(Path manifest) {
    try {
      var node = new JsonMapper().readTree(Files.readAllBytes(manifest)).get("boundary");
      if (node == null
          || !node.isObject()
          || !node.has("topic")
          || !node.get("topic").isString()
          || !node.has("fixtureSha256")
          || !node.get("fixtureSha256").isString())
        throw new IllegalArgumentException("Missing boundary metadata");
      for (String name : List.of("beginning", "end")) {
        var offsets = node.get(name);
        if (offsets == null
            || !offsets.isObject()
            || !Set.copyOf(offsets.propertyNames()).equals(Set.of("0", "1", "2")))
          throw new IllegalArgumentException("Expected all three partition boundaries");
        for (var entry : offsets.properties())
          if (!entry.getValue().isIntegralNumber()
              || !entry.getValue().canConvertToLong()
              || entry.getValue().longValue() < 0)
            throw new IllegalArgumentException("Invalid partition offset");
      }
      for (String part : List.of("0", "1", "2"))
        if (node.get("beginning").get(part).longValue() > node.get("end").get(part).longValue())
          throw new IllegalArgumentException("Reversed partition boundaries");
      return node;
    } catch (Exception ex) {
      throw new IllegalArgumentException("Invalid manifest boundary", ex);
    }
  }

  public static DataSource environmentDataSource() {
    String url = System.getenv("P22_DB_URL"),
        user = System.getenv("P22_DB_USER"),
        password = System.getenv("P22_DB_PASSWORD");
    if (url == null || user == null || password == null)
      throw new IllegalArgumentException(
          "Set P22_DB_URL, P22_DB_USER and P22_DB_PASSWORD for the owned database");
    var source = new DriverManagerDataSource(url, user, password);
    var properties = new Properties();
    properties.setProperty("connectTimeout", "5");
    properties.setProperty("socketTimeout", "5");
    properties.setProperty("options", "-c statement_timeout=2000 -c lock_timeout=1000");
    source.setConnectionProperties(properties);
    return source;
  }

  private int validate(FrozenFixture fixture) {
    var contract = new EventContract(fixture.seed().keySet());
    for (var record : fixture.records()) {
      boolean valid =
          contract.validate(decode(record.keyBase64()), decode(record.valueBase64()))
              instanceof EventContract.Valid;
      if (valid != record.validContract())
        throw new IllegalArgumentException("Fixture classification mismatch at " + record.index());
    }
    System.out.println(
        new JsonMapper()
            .writeValueAsString(
                Map.of(
                    "validatedRecords",
                    fixture.records().size(),
                    "fixtureSha256",
                    fixture.sha256())));
    return 0;
  }

  private static byte[] decode(String value) {
    return value == null ? null : Base64.getDecoder().decode(value);
  }
}
