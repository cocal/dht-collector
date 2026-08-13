package com.cocal.dht;
public final class Main {
  public static void main(String[] args) throws Exception {
    String requestedMode = modeArgument(args);
    if (requestedMode != null && !requestedMode.equals("collector") && !requestedMode.equals("dashboard")) {
      Cli.run(args);
      return;
    }
    Config config = Config.from(args);
    try (Catalog catalog = new Catalog(config.dbUrl(), config.dbUser(), config.dbPassword(), config.poolSize())) {
      catalog.initialize();
      if (config.mode().equals("dashboard")) {
        try (DashboardServer dashboard = new DashboardServer(catalog, config)) {
          dashboard.start();
          Thread.currentThread().join();
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

  private static String modeArgument(String[] args) {
    for (int index = 0; index + 1 < args.length; index++) if (args[index].equals("--mode")) return args[index + 1];
    return null;
  }
}
