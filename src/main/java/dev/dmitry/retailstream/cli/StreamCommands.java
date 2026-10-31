package dev.dmitry.retailstream.cli;

import dev.dmitry.retailstream.contract.EventContract;
import dev.dmitry.retailstream.fixture.FrozenFixture;
import dev.dmitry.retailstream.projection.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import javax.sql.DataSource;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

public final class StreamCommands {
  private final javax.sql.DataSource injected;

  public StreamCommands() {
    this(null);
  }

  public StreamCommands(javax.sql.DataSource dataSource) {
    this.injected = dataSource;
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
      var boundary = readBoundary(Path.of(options.required("--manifest")));
      if (!options.topic().equals(boundary.get("topic").asString())
          || !fixture.sha256().equals(boundary.get("fixtureSha256").asString()))
        throw new IllegalArgumentException("Manifest differs from run/fixture");
      var config =
          new ProjectionConfig(
              options.schema(),
              options.topic(),
              options.schema(),
              fixture.sha256(),
              fixture.seedSha256());
      var dataSource = injected == null ? environmentDataSource() : injected;
      if (options.command().equals("init")) {
        new SchemaManager(dataSource).createFresh(config, fixture.seed());
        System.out.println(
            new JsonMapper()
                .writeValueAsString(Map.of("schema", config.schema(), "initialized", true)));
      } else {
        var store =
            new ProjectionStore(dataSource, config, fixture.seed().keySet(), ProcessingHooks.NONE);
        System.out.println(store.snapshot().canonicalJson());
      }
      return 0;
    } catch (IllegalArgumentException ex) {
      System.err.println(ex.getMessage());
      return 2;
    } catch (RuntimeException ex) {
      System.err.println(ex.getClass().getSimpleName() + ": " + ex.getMessage());
      return 1;
    }
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

  private static byte[] decode(String encoded) {
    return encoded == null ? null : Base64.getDecoder().decode(encoded);
  }
}
