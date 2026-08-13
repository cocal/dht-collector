package com.cocal.dht;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import java.nio.file.Files;
import org.junit.jupiter.api.Test;

class CliTest {
  @Test void validatesInfoHashes() {
    assertEquals("abcdef0123456789abcdef0123456789abcdef01",
        Cli.normalizeHash("ABCDEF0123456789ABCDEF0123456789ABCDEF01"));
    assertThrows(IllegalArgumentException.class, () -> Cli.normalizeHash("bad"));
  }

  @Test void loadsOnlyUniqueAuthorizedTargets() throws Exception {
    var input = Files.createTempFile("approved-targets", ".jsonl");
    Files.writeString(input, "{\"info_hash\":\"0123456789abcdef0123456789abcdef01234567\",\"authorization_ref\":\"release\"}\n");
    var targets = Cli.loadApprovedTargets(input);
    assertEquals(1, targets.size());
    assertEquals("release", targets.getFirst().authorizationRef());
  }
}
