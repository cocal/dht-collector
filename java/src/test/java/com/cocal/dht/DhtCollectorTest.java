package com.cocal.dht;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.List;
import org.junit.jupiter.api.Test;

final class DhtCollectorTest {
  @Test
  void keepsUdpSourceAsImpliedPortFallback() throws Exception {
    InetAddress address = InetAddress.getLoopbackAddress();
    List<InetSocketAddress> endpoints = DhtCollector.announceEndpoints(
        new InetSocketAddress(address, 49001), 51413);

    assertEquals(List.of(
        new InetSocketAddress(address, 51413),
        new InetSocketAddress(address, 49001)), endpoints);
  }

  @Test
  void doesNotDuplicateMatchingAnnounceAndSourcePorts() throws Exception {
    InetAddress address = InetAddress.getLoopbackAddress();
    assertEquals(List.of(new InetSocketAddress(address, 51413)),
        DhtCollector.announceEndpoints(new InetSocketAddress(address, 51413), 51413));
  }

  @Test
  void livePeerHintsStayAheadOfRecoveredHints() throws Exception {
    InetAddress address = InetAddress.getLoopbackAddress();
    InetSocketAddress live = new InetSocketAddress(address, 51001);
    InetSocketAddress recovered = new InetSocketAddress(address, 51002);

    assertEquals(List.of(live, recovered),
        DhtCollector.mergePeerHints(List.of(live), List.of(live, recovered)));
  }
}
