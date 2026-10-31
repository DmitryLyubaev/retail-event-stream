package dev.dmitry.retailstream.contract;

public final class RawIdentity {
  private RawIdentity() {}

  public static String digest(byte[] key, byte[] value) {
    try {
      var hash = java.security.MessageDigest.getInstance("SHA-256");
      for (byte[] bytes : new byte[][] {key, value}) {
        hash.update(
            java.nio.ByteBuffer.allocate(4).putInt(bytes == null ? -1 : bytes.length).array());
        if (bytes != null) hash.update(bytes);
      }
      return java.util.HexFormat.of().formatHex(hash.digest());
    } catch (java.security.NoSuchAlgorithmException ex) {
      throw new IllegalStateException(ex);
    }
  }
}
