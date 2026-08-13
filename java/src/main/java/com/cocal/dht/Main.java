package com.cocal.dht;
public final class Main {
  public static void main(String[] args) throws Exception {
    Config config = Config.from(args);
    try (Catalog catalog = new Catalog(config.dbUrl(), config.dbUser(), config.dbPassword(), config.poolSize())) {
      catalog.initialize();
      DhtCollector collector = new DhtCollector(config, catalog);
      collector.start();
      System.out.printf("Java DHT collector listening from %s:%d (%d mldht nodes), cached resources=%d%n",
          config.address(), config.port(), config.dhtNodes(), collector.cachedResources());
      Thread.currentThread().join();
    }
  }
}
