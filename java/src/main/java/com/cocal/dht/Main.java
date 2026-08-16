package com.cocal.dht;
import redis.clients.jedis.JedisPooled;
public final class Main {
  public static void main(String[] args) throws Exception {
    String requestedMode = modeArgument(args);
    if (requestedMode != null && !requestedMode.equals("collector") && !requestedMode.equals("dashboard") && !requestedMode.equals("db-writer")) {
      Cli.run(args);
      return;
    }
    Config config = Config.from(args);
    try (Catalog catalog = new Catalog(config.dbUrl(), config.dbUser(), config.dbPassword(), config.poolSize())) {
      // Dashboard is read-only; schema migration belongs to the writer/collector startup path.
      if (!config.mode().equals("dashboard")) catalog.initialize();
      if (config.mode().equals("dashboard")) {
        try (DashboardServer dashboard = new DashboardServer(catalog, config)) {
          dashboard.start();
          Thread.currentThread().join();
        }
      } else if (config.mode().equals("db-writer")) {
        JedisPooled redis = connectRedis(config.redisUrl());
        if (redis == null) throw new IllegalStateException("REDIS_URL is required for db-writer mode");
        try (DbWriter writer = new DbWriter(catalog, redis)) {
          writer.run();
        }
      } else {
        try (DhtCollector collector = new DhtCollector(config, catalog)) {
          collector.start();
          System.out.printf("Java DHT collector listening from %s:%d (%d mldht nodes), cached resources=%d%n",
              config.address(), config.port(), config.dhtNodes(), collector.cachedResources());
          Thread.currentThread().join();
        }
      }
    }
  }

  private static JedisPooled connectRedis(String url) {
    if (url == null || url.isBlank()) return null;
    try { JedisPooled client = new JedisPooled(java.net.URI.create(url)); client.ping(); return client; }
    catch (RuntimeException error) { System.err.println("db writer Redis unavailable: " + error.getMessage()); return null; }
  }

  private static String modeArgument(String[] args) {
    for (int index = 0; index + 1 < args.length; index++) if (args[index].equals("--mode")) return args[index + 1];
    return null;
  }
}
