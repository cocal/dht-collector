#!/usr/bin/env python3
"""Optional journal -> Redis monitor bridge for distributed DHT nodes."""

import json
import os
import subprocess
import threading
import time
import uuid

import redis


REDIS_URL = os.getenv("REDIS_URL", "redis://127.0.0.1:6379/0")
JOURNAL_UNIT = os.getenv("JOURNAL_UNIT", "dht-passive-collector.service")
NODE_ID = os.getenv("DHT_NODE_ID", os.uname().nodename)
STREAM = os.getenv("DHT_EVENT_STREAM", "dht:events")
SUMMARY = os.getenv("DHT_SUMMARY_KEY", "dht:summary")
STREAM_MAXLEN = int(os.getenv("DHT_EVENT_MAXLEN", "1000000"))
DEDUPE_TTL = int(os.getenv("DHT_DEDUPE_TTL", "86400"))

APPLY_EVENT = """
if redis.call('SET', KEYS[1], '1', 'NX', 'EX', ARGV[5]) then
  redis.call('HINCRBY', KEYS[2], ARGV[1], ARGV[2])
  redis.call('HINCRBY', KEYS[3], ARGV[1], ARGV[2])
  redis.call('HSET', KEYS[2], 'last_event_at', ARGV[4])
  redis.call('HSET', KEYS[3], 'node_id', ARGV[3], 'last_event_at', ARGV[4])
  return 1
end
return 0
"""


def payload_from_journal(raw):
    message = raw.get("MESSAGE")
    if not isinstance(message, str):
        return None
    try:
        event = json.loads(message)
    except json.JSONDecodeError:
        return None
    if event.get("schema") != "monitor.v1":
        return None
    metric = event.get("metric")
    event_id = event.get("event_id")
    occurred_at = event.get("occurred_at")
    value = event.get("value")
    if not isinstance(metric, str) or not isinstance(event_id, str):
        return None
    if not isinstance(occurred_at, str) or not isinstance(value, int) or value <= 0:
        return None
    if not metric or len(metric) > 96:
        return None
    return event


def heartbeat(client):
    key = f"dht:node:{NODE_ID}:heartbeat"
    while True:
        try:
            client.set(key, str(int(time.time())), ex=30)
        except redis.RedisError as error:
            print(f"redis heartbeat failed: {error}", flush=True)
        time.sleep(10)


def main():
    client = redis.Redis.from_url(REDIS_URL, decode_responses=True)
    client.ping()
    boot_id = str(uuid.uuid4())
    sequence = 0
    apply_event = client.register_script(APPLY_EVENT)
    threading.Thread(target=heartbeat, args=(client,), daemon=True).start()
    journal = subprocess.Popen(
        ["journalctl", "--unit", JOURNAL_UNIT, "--follow", "--output", "json",
         "--no-pager", "--since", "now"],
        stdout=subprocess.PIPE, stderr=None, text=True, bufsize=1,
    )
    try:
        for line in journal.stdout:
            try:
                event = payload_from_journal(json.loads(line))
            except json.JSONDecodeError:
                continue
            if event is None:
                continue
            sequence += 1
            event = dict(event)
            event.update({"node_id": NODE_ID, "boot_id": boot_id, "sequence": sequence})
            event_json = json.dumps(event, separators=(",", ":"), ensure_ascii=False)
            client.xadd(STREAM, {"event": event_json, "event_id": event["event_id"],
                                 "node_id": NODE_ID, "metric": event["metric"]},
                        maxlen=STREAM_MAXLEN, approximate=True)
            node_key = f"dht:node:{NODE_ID}"
            applied = apply_event(keys=[f"dht:dedupe:{event['event_id']}", SUMMARY, node_key],
                                  args=[event["metric"], event["value"], NODE_ID,
                                        event["occurred_at"], DEDUPE_TTL])
            if applied:
                client.publish("dht:summary:update", event_json)
    finally:
        journal.terminate()


if __name__ == "__main__":
    main()
