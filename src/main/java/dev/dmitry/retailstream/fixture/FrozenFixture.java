package dev.dmitry.retailstream.fixture;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

public record FrozenFixture(
    List<FixtureRecord> records, Map<String, Long> seed, JsonNode expected, String sha256) {
  public FrozenFixture {
    records = List.copyOf(records);
    seed = Map.copyOf(seed);
    expected = expected.deepCopy();
  }

  @Override
  public JsonNode expected() {
    return expected.deepCopy();
  }

  public record FixtureRecord(
      int index, String keyBase64, String valueBase64, boolean validContract) {}

  public static FrozenFixture load(Path directory) {
    try {
      var mapper = new JsonMapper();
      var hash = java.security.MessageDigest.getInstance("SHA-256");
      var trees = new java.util.HashMap<String, JsonNode>();
      for (String file : List.of("seed.json", "events.json", "expected.json")) {
        byte[] bytes = java.nio.file.Files.readAllBytes(directory.resolve(file));
        hash.update(java.nio.ByteBuffer.allocate(8).putLong(bytes.length).array());
        hash.update(bytes);
        trees.put(file, mapper.readTree(bytes));
      }
      var seedNode = trees.get("seed.json");
      if (seedNode.get("version").intValue() != 1)
        throw new IllegalArgumentException("Unknown seed version");
      var balances = new java.util.TreeMap<String, Long>();
      seedNode
          .get("balances")
          .properties()
          .forEach(e -> balances.put(e.getKey(), e.getValue().longValue()));
      var records = new java.util.ArrayList<FixtureRecord>();
      for (var r : trees.get("events.json")) {
        int index = r.get("index").intValue();
        if (index != records.size())
          throw new IllegalArgumentException("Nonsequential fixture index");
        records.add(
            new FixtureRecord(
                index,
                r.get("keyBase64").isNull() ? null : r.get("keyBase64").asString(),
                r.get("valueBase64").isNull() ? null : r.get("valueBase64").asString(),
                r.get("validContract").booleanValue()));
      }
      return new FrozenFixture(
          records,
          balances,
          trees.get("expected.json"),
          java.util.HexFormat.of().formatHex(hash.digest()));
    } catch (java.io.IOException | java.security.NoSuchAlgorithmException ex) {
      throw new IllegalArgumentException("Cannot load frozen fixture", ex);
    }
  }

  public String seedSha256() {
    return dev.dmitry.retailstream.contract.RawIdentity.digest(
        null, new JsonMapper().writeValueAsBytes(new java.util.TreeMap<>(seed)));
  }
}
