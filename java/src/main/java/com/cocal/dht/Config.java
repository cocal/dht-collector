package com.cocal.dht;

import java.nio.file.Path;

record Config(String mode, String address, int port, int dhtNodes, int maxConcurrent, int maxResources,
              String dbUrl, String dbUser, String dbPassword, int poolSize, Path storagePath,
              String httpHost, int httpPort, int metadataConcurrent, int metadataTimeoutSeconds,
              Path staticPath, String redisUrl) {
  static Config from(String[] args) {
    String address = "0.0.0.0";
    String url = System.getenv("DATABASE_URL");
    String user = System.getenv("PGUSER");
    String password = System.getenv("PGPASSWORD");
    Path storage = Path.of("var", "java-dht");
    Path staticPath = Path.of("web", "public");
    String redisUrl = System.getenv("REDIS_URL");
    String mode = "collector";
    String httpHost = "127.0.0.1";
    int port = 51413, nodes = 1, concurrent = 160, max = 0, pool = 5, httpPort = 4173, metadataConcurrent = 24, metadataTimeout = 30;
    for (int index = 0; index < args.length; index++) {
      String argument = args[index];
      if (argument.equals("--mode")) mode = args[++index];
      else if (argument.equals("--address")) address = args[++index];
      else if (argument.equals("--port")) port = Integer.parseInt(args[++index]);
      else if (argument.equals("--dht-nodes")) nodes = Integer.parseInt(args[++index]);
      else if (argument.equals("--max-concurrent")) concurrent = Integer.parseInt(args[++index]);
      else if (argument.equals("--max-resources")) max = Integer.parseInt(args[++index]);
      else if (argument.equals("--db-url")) url = args[++index];
      else if (argument.equals("--db-user")) user = args[++index];
      else if (argument.equals("--db-password")) password = args[++index];
      else if (argument.equals("--pool-size")) pool = Integer.parseInt(args[++index]);
      else if (argument.equals("--storage-path")) storage = Path.of(args[++index]);
      else if (argument.equals("--http-host")) httpHost = args[++index];
      else if (argument.equals("--http-port")) httpPort = Integer.parseInt(args[++index]);
      else if (argument.equals("--metadata-concurrent")) metadataConcurrent = Integer.parseInt(args[++index]);
      else if (argument.equals("--metadata-timeout-seconds")) metadataTimeout = Integer.parseInt(args[++index]);
      else if (argument.equals("--static-path")) staticPath = Path.of(args[++index]);
      else if (argument.equals("--redis-url")) redisUrl = args[++index];
      else throw new IllegalArgumentException("unknown option: " + argument);
    }
    if (!mode.equals("collector") && !mode.equals("dashboard") && !mode.equals("db-writer") && !Cli.supports(mode)) throw new IllegalArgumentException("unknown --mode: " + mode);
    if ((mode.equals("collector") || mode.equals("dashboard") || mode.equals("db-writer")) && (url == null || url.isBlank())) throw new IllegalArgumentException("DATABASE_URL or --db-url is required");
    if (port < 1 || port + nodes - 1 > 65535) throw new IllegalArgumentException("invalid DHT port range");
    if (max < 0) throw new IllegalArgumentException("--max-resources must not be negative");
    if (nodes < 1 || concurrent < 1 || pool < 1 || httpPort < 1 || httpPort > 65535 || metadataConcurrent < 1 || metadataTimeout < 1) throw new IllegalArgumentException("node, concurrency, pool, HTTP and timeout sizes must be positive");
    return new Config(mode, address, port, nodes, concurrent, max, url, user, password, pool, storage, httpHost, httpPort, metadataConcurrent, metadataTimeout, staticPath, redisUrl);
  }
}
