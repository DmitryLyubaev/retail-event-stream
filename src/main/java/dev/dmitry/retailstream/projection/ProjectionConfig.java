package dev.dmitry.retailstream.projection;

public record ProjectionConfig(
    String schema, String topic, String groupId, String fixtureSha256, String seedSha256) {
  public ProjectionConfig {
    if (schema == null
        || !schema.matches("p22_[a-z0-9_]{1,48}")
        || groupId == null
        || !groupId.matches("p22_[a-z0-9_]{1,48}"))
      throw new IllegalArgumentException("Unsafe projection namespace");
    if (topic == null || !topic.startsWith("p22.") || !topic.matches("[A-Za-z0-9._-]{1,249}"))
      throw new IllegalArgumentException("Unsafe source topic");
    if (fixtureSha256 == null
        || !fixtureSha256.matches("[0-9a-f]{64}")
        || seedSha256 == null
        || !seedSha256.matches("[0-9a-f]{64}"))
      throw new IllegalArgumentException("Invalid fixture/seed hash");
  }

  String table(String name) {
    if (!java.util.Set.of("inventory", "receipts", "processing", "projection_meta").contains(name))
      throw new IllegalArgumentException("Unknown projection table");
    return "\"" + schema + "\"." + name;
  }
}
