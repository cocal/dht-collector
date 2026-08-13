package the8472.mldht;

import java.net.InetSocketAddress;

/** Exposes TorrentFetcher's package-scoped peer hint hook to the collector. */
public final class FetchTaskPeerHints {
  private FetchTaskPeerHints() {}

  public static void add(TorrentFetcher.FetchTask task, InetSocketAddress peer) {
    if (peer == null || peer.getAddress() == null) return;
    task.addCandidate(peer.getAddress(), peer);
  }
}
