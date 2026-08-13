package com.cocal.dht;

import static org.junit.jupiter.api.Assertions.*;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class BencodeTest {
  @Test void roundTripsBinaryDictionary() {
    Map<String,Object> value = Map.of("t", new byte[]{1,2}, "y", "r", "r", Map.of("id", new byte[20]));
    Map<String,Object> decoded = Bencode.dict(Bencode.decode(Bencode.encode(value)));
    assertArrayEquals(new byte[]{1,2}, Bencode.bytes(decoded.get("t")));
    assertEquals("r", Bencode.text(decoded.get("y")));
    assertEquals(20, Bencode.bytes(Bencode.dict(decoded.get("r")).get("id")).length);
  }

  @Test void decodesListsAndIntegers() {
    assertEquals(List.of(1L, 2L), Bencode.decode("li1ei2ee".getBytes()));
  }
}
