package com.cocal.dht;

import java.time.Duration;
import java.util.List;
import java.util.UUID;
import redis.clients.jedis.JedisPooled;
import redis.clients.jedis.params.SetParams;

/** Short advisory lease that suppresses duplicate metadata work across collectors. */
final class TaskLease implements AutoCloseable {
  private static final long TTL_MILLIS = Duration.ofSeconds(90).toMillis();
  private static final String RELEASE = "if redis.call('get', KEYS[1]) == ARGV[1] then return redis.call('del', KEYS[1]) else return 0 end";
  private final JedisPooled redis;
  private final String key;
  private final String token;

  private TaskLease(JedisPooled redis, String key, String token) {
    this.redis = redis; this.key = key; this.token = token;
  }

  static TaskLease tryAcquire(JedisPooled redis, String hash) {
    if (redis == null || hash == null || hash.isBlank()) return null;
    String key = "dht:task-lock:" + hash;
    String token = UUID.randomUUID().toString();
    try {
      return "OK".equals(redis.set(key, token, SetParams.setParams().nx().px(TTL_MILLIS)))
          ? new TaskLease(redis, key, token) : null;
    } catch (RuntimeException ignored) { return null; }
  }

  @Override public void close() {
    try { redis.eval(RELEASE, List.of(key), List.of(token)); }
    catch (RuntimeException ignored) { }
  }
}
