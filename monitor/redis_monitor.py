#!/usr/bin/env python3
"""Emit monitor.v1 health metrics for the optional Redis/KeyDB instance."""

import json
import os
import time
import uuid

import redis


REDIS_URL = os.getenv("REDIS_URL", "redis://127.0.0.1:6379/0")
STREAM = os.getenv("DHT_EVENT_STREAM", "dht:events")
INTERVAL = max(5, int(os.getenv("REDIS_MONITOR_INTERVAL", "30")))
SERVICE = os.getenv("REDIS_MONITOR_SERVICE", "dht-redis-monitor")


def emit(metric, value, **extra):
    event = {
        "schema": "monitor.v1",
        "event_id": str(uuid.uuid4()),
        "service": SERVICE,
        "metric": metric,
        "occurred_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "value": max(1, int(value)),
    }
    event.update(extra)
    print(json.dumps(event, separators=(",", ":")), flush=True)
    return event


def publish(client, event):
    try:
        client.hset("dht:summary", event["metric"], event["value"])
        client.publish("dht:summary:update", json.dumps(event, separators=(",", ":")))
    except redis.RedisError:
        pass


def main():
    client = redis.Redis.from_url(REDIS_URL, decode_responses=True,
                                  socket_connect_timeout=3, socket_timeout=3)
    while True:
        started = time.monotonic()
        try:
            client.ping()
            latency_ms = round((time.monotonic() - started) * 1000)
            info = client.info()
            for metric, value in (("redis.up", 1), ("redis.latency_ms", latency_ms),
                                  ("redis.used_memory_bytes", info.get("used_memory", 0)),
                                  ("redis.connected_clients", info.get("connected_clients", 0)),
                                  ("redis.events_stream_length", client.xlen(STREAM))):
                publish(client, emit(metric, value))
        except Exception as error:
            print(f"redis monitor error: {error}", flush=True)
            emit("redis.error", 1, message=str(error)[:200])
        time.sleep(max(0, INTERVAL - (time.monotonic() - started)))


if __name__ == "__main__":
    main()
