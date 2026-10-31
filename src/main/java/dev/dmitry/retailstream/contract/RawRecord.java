package dev.dmitry.retailstream.contract;

public record RawRecord(String topic, int partition, long offset, byte[] key, byte[] value) {
  public RawRecord {
    if (topic == null || topic.isBlank() || partition < 0 || offset < 0)
      throw new IllegalArgumentException("Invalid log coordinate");
    key = copy(key);
    value = copy(value);
  }

  @Override
  public byte[] key() {
    return copy(key);
  }

  @Override
  public byte[] value() {
    return copy(value);
  }

  private static byte[] copy(byte[] bytes) {
    return bytes == null ? null : bytes.clone();
  }
}
