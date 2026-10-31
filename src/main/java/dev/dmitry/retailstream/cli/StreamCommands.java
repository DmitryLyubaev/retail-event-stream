package dev.dmitry.retailstream.cli;

import dev.dmitry.retailstream.contract.EventContract;
import dev.dmitry.retailstream.fixture.FrozenFixture;
import java.nio.file.Path;
import java.util.Base64;

public final class StreamCommands {
  public int execute(String[] args) {
    try {
      if (args.length != 3 || !args[0].equals("validate") || !args[1].equals("--fixture")) return 2;
      var fixture = FrozenFixture.load(Path.of(args[2]));
      var contract = new EventContract(fixture.seed().keySet());
      for (var record : fixture.records()) {
        boolean valid =
            contract.validate(decode(record.keyBase64()), decode(record.valueBase64()))
                instanceof EventContract.Valid;
        if (valid != record.validContract())
          throw new IllegalArgumentException(
              "Fixture classification mismatch at " + record.index());
      }
      System.out.println(
          "{\"validatedRecords\":"
              + fixture.records().size()
              + ",\"fixtureSha256\":\""
              + fixture.sha256()
              + "\"}");
      return 0;
    } catch (IllegalArgumentException ex) {
      System.err.println(ex.getMessage());
      return 2;
    }
  }

  private static byte[] decode(String encoded) {
    return encoded == null ? null : Base64.getDecoder().decode(encoded);
  }
}
