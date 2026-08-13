package com.cocal.dht;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import org.junit.jupiter.api.Test;

class CatalogTest {
  @Test void validatesSearchLength() {
    assertEquals("ubuntu", Catalog.validatedSearch(" ubuntu "));
    assertEquals("中文词", Catalog.validatedSearch("中文词"));
    assertThrows(IllegalArgumentException.class, () -> Catalog.validatedSearch("ab"));
    assertThrows(IllegalArgumentException.class, () -> Catalog.validatedSearch("x".repeat(101)));
  }
}
