package com.cocal.dht;

import java.nio.file.Path;

record Config(String address, int port, int dhtNodes, int maxConcurrent, int maxResources,
              String dbUrl, String dbUser, String dbPassword, int poolSize, Path storagePath) {
  static Config from(String[] args) {
    String address = "0.0.0.0";
    String url = System.getenv("DATABASE_URL");
    String user = System.getenv("PGUSER");
    String password = System.getenv("PGPASSWORD");
    Path storage = Path.of("var", "java-dht");
    int port = 51413, nodes = 1, concurrent = 160, max = 0, pool = 5;
    for (int index = 0; index < args.length; index++) {
      String argument = args[index];
      if (argument.equals("--address")) address = args[++index];
      else if (argument.equals("--port")) port = Integer.parseInt(args[++index]);
      else if (argument.equals("--dht-nodes")) nodes = Integer.parseInt(args[++index]);
      else if (argument.equals("--max-concurrent")) concurrent = Integer.parseInt(args[++index]);
      else if (argument.equals("--max-resources")) max = Integer.parseInt(args[++index]);
      else if (argument.equals("--db-url")) url = args[++index];
      else if (argument.equals("--db-user")) user = args[++index];
      else if (argument.equals("--db-password")) password = args[++index];
      else if (argument.equals("--pool-size")) pool = Integer.parseInt(args[++index]);
      else if (argument.equals("--storage-path")) storage = Path.of(args[++index]);
      else throw new IllegalArgumentException("unknown option: " + argument);
    }
    if (url == null || url.isBlank()) throw new IllegalArgumentException("DATABASE_URL or --db-url is required");
    if (port < 1 || port + nodes - 1 > 65535) throw new IllegalArgumentException("invalid DHT port range");
    if (nodes < 1 || concurrent < 1 || pool < 1) throw new IllegalArgumentException("node, concurrency and pool sizes must be positive");
    return new Config(address, port, nodes, concurrent, max, url, user, password, pool, storage);
  }
}
