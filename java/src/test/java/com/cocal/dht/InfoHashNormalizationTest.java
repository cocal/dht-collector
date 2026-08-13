package com.cocal.dht;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Locale;
import lbms.plugins.mldht.kad.Key;
import org.junit.jupiter.api.Test;

class InfoHashNormalizationTest {
  @Test
  void mldhtHexKeysUseTheRepositoryLowercaseConvention() {
    assertEquals("abcdef0123456789abcdef0123456789abcdef01",
        new Key("abcdef0123456789abcdef0123456789abcdef01").toString(false).toLowerCase(Locale.ROOT));
  }
}
