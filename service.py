"""Douyin Cloud container relay. Run only behind the configured Douyin gateway.

Header values are NOT authentication on the public Internet. The live callback
must be internal-only in the cloud console. No AppSecret is used by this service.
"""
import hashlib
import json
import os
import threading
import time
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.request import Request, urlopen

APP_ID = "tt19616fdc8719e41710"
TYPES = ("live_comment", "live_like", "live_gift")
TASK_URL = "http://webcast-bytedance-com.openapi.dyc.ivolces.com/api/live_data/task/start"
PUSH_URL = "http://ws-push.dyc.ivolces.com/ws/live_interaction/push_data"
MAX_BODY = 1048576


def resolve_environment(environ):
    cloud = environ.get("CLOUD_ENV", "").strip().lower()
    custom = environ.get("DY_ENV", "").strip().lower()
    if cloud and cloud not in ("dev", "prod"):
        raise ValueError("invalid CLOUD_ENV")
    if custom and custom not in ("dev", "prod"):
        raise ValueError("invalid DY_ENV")
    if cloud and custom and cloud != custom:
        raise ValueError("cloud and application environments differ")
    environment = cloud or custom
    if not environment:
        raise ValueError("cloud environment required")
    return environment


def identifier(value, maximum=512):
    return isinstance(value, str) and 0 < len(value) <= maximum and not any(ord(c) < 32 for c in value)


def encode_event(room, kind, row):
    if kind not in TYPES or not identifier(room, 256) or not isinstance(row, dict):
        raise ValueError("invalid event")
    if not identifier(row.get("msg_id"), 256) or not identifier(row.get("sec_openid")):
        raise ValueError("missing event identity")
    if not isinstance(row.get("test", False), bool):
        raise ValueError("invalid test flag")
    count = row.get("gift_num" if kind == "live_gift" else "like_num", 1)
    if type(count) is not int or not 1 <= count <= 10000:
        raise ValueError("invalid incremental count")
    if kind == "live_gift" and not identifier(row.get("sec_gift_id")):
        raise ValueError("missing gift identity")
    for field in ("nickname", "content"):
        if field in row and (not isinstance(row[field], str) or len(row[field]) > 2048):
            raise ValueError("invalid text")
    # Every event retains its own message ID; never repeat a whole batch per ID.
    return {"room_id": room, "msg_id": row["msg_id"], "msg_type": kind,
            "data": json.dumps([row], ensure_ascii=False, separators=(",", ":"))}


def post_json(url, payload, headers=None):
    req = Request(url, json.dumps(payload, ensure_ascii=False).encode("utf-8"),
                  {"Content-Type": "application/json", **(headers or {})}, method="POST")
    with urlopen(req, timeout=8) as response:
        raw = response.read(MAX_BODY + 1)
        if len(raw) > MAX_BODY:
            raise ValueError("upstream response too large")
        return json.loads(raw)


class RedisOutbox:
    # Dedup and enqueue are one transaction. Redis must use persistence and noeviction.
    ADD = """
    if redis.call('EXISTS', KEYS[1]) == 1 then return 0 end
    if redis.call('LLEN', KEYS[2]) >= tonumber(ARGV[2]) then return -1 end
    redis.call('RPUSH', KEYS[2], ARGV[1])
    redis.call('SET', KEYS[1], '1', 'EX', 2592000)
    return 1
    """
    POP = """
    if redis.call('GET', KEYS[1]) ~= ARGV[1] then return 0 end
    if redis.call('LINDEX', KEYS[2], 0) ~= ARGV[2] then return 0 end
    redis.call('LPOP', KEYS[2]); return 1
    """
    UNLOCK = """
    if redis.call('GET', KEYS[1]) == ARGV[1] then return redis.call('DEL', KEYS[1]) end
    return 0
    """

    def __init__(self, redis, environment):
        if environment not in ("dev", "prod"):
            raise ValueError("DY_ENV must be dev or prod")
        self.redis = redis
        self.prefix = f"mw:{APP_ID}:{environment}:"
        self.queue = self.prefix + "outbox"
        self.lock = self.prefix + "worker"

    def put(self, anchor, event):
        identity = json.dumps([event["room_id"], event["msg_type"], event["msg_id"]])
        key = self.prefix + "seen:" + hashlib.sha256(identity.encode()).hexdigest()
        body = json.dumps({"anchor": anchor, "event": event}, ensure_ascii=False)
        result = self.redis.eval(self.ADD, 2, key, self.queue, body, 100000)
        if result < 0:
            raise RuntimeError("outbox capacity reached")
        return result

    def drain_one(self, send):
        owner = str(uuid.uuid4())
        if not self.redis.set(self.lock, owner, nx=True, ex=30):
            return False
        try:
            raw = self.redis.lindex(self.queue, 0)
            if raw is None:
                return False
            item = json.loads(raw)
            send(item["anchor"], item["event"])
            # No deletion when gateway rejects or process dies. Retry is at-least-once.
            return bool(self.redis.eval(self.POP, 2, self.lock, self.queue, owner, raw))
        finally:
            self.redis.eval(self.UNLOCK, 1, self.lock, owner)


class Relay:
    def __init__(self, store, callback_source, environment, post=post_json):
        if not callback_source or environment not in ("dev", "prod"):
            raise ValueError("verified callback source and environment required")
        self.store, self.source, self.environment, self.post = store, callback_source, environment, post

    def route(self, path, headers, body):
        if path == "/healthz":
            self.store.redis.ping()
            return 200, {"ok": True}
        if path == "/live_data_callback":
            if headers.get("x-tt-source") != self.source:
                return 403, {"ok": False, "reason": "source"}
            if headers.get("x-tt-appid", APP_ID) != APP_ID:
                return 403, {"ok": False, "reason": "application"}
            room = headers.get("x-roomid") or headers.get("x-room-id")
            anchor, kind = headers.get("x-anchor-openid"), headers.get("x-msg-type")
            if not identifier(anchor) or not identifier(room, 256) or kind not in TYPES:
                return 400, {"ok": False, "reason": "callback headers"}
            if not isinstance(body, list) or not 1 <= len(body) <= 1000:
                return 400, {"ok": False, "reason": "event array"}
            # Validate before mutating. Partial queue writes can safely retry by ID.
            events = [encode_event(room, kind, row) for row in body]
            for event in events:
                self.store.put(anchor, event)
            return 200, {"ok": True, "accepted": len(events)}
        # These identity headers must be injected by the authenticated cloud gateway.
        if headers.get("x-tt-appid") != APP_ID or not identifier(headers.get("x-room-id"), 256) or not identifier(headers.get("x-anchor-openid")):
            return 403, {"ok": False, "reason": "gateway identity"}
        if path == "/start_game":
            result = {}
            for kind in TYPES:
                response = self.post(TASK_URL, {"appid": APP_ID, "roomid": headers["x-room-id"], "msg_type": kind})
                result[kind] = response.get("err_no") == 0
            ok = all(result.values())
            return (200 if ok else 502), {"ok": ok, "tasks": result}
        if path == "/websocket_callback":
            if headers.get("x-tt-event-type") not in ("connect", "disconnect", "uplink"):
                return 400, {"ok": False}
            # Uplink cannot inject gifts, contributions, or settlement scores.
            return 200, {"ok": True}
        return 404, {"ok": False}

    def push(self, anchor, event):
        # The gateway documents extra_data, not arbitrary top-level fields.
        payload = {key: event[key] for key in ("msg_id", "msg_type", "data")}
        payload["extra_data"] = json.dumps({"room_id": event["room_id"]}, separators=(",", ":"))
        response = self.post(PUSH_URL, payload, {"X-TT-WS-OPENIDS": json.dumps([anchor])})
        # Keep unknown response formats pending until real cloud response is verified.
        if not isinstance(response, dict) or type(response.get("err_no")) is not int or response["err_no"] != 0:
            raise RuntimeError("gateway acceptance not verified")
        data = response.get("data")
        # The gateway can report recipient failures even when err_no is zero.
        # Only an explicit empty failure list permits removal from the outbox.
        if not isinstance(data, dict) or data.get("failed_open_id_list") != []:
            raise RuntimeError("gateway acceptance not verified")


def main():
    import redis
    if os.environ.get("DY_INTERNAL_CALLBACK_CONFIRMED") != "1":
        raise RuntimeError("Configure internal-only callbacks before enabling this service")
    redis_url = os.environ.get("REDIS_URL")
    if redis_url:
        database = redis.Redis.from_url(redis_url, decode_responses=True,
                                        socket_timeout=5, socket_connect_timeout=5)
    else:
        address = os.environ.get("REDIS_ADDRESS")
        username = os.environ.get("REDIS_USERNAME", "default")
        password = os.environ.get("REDIS_PASSWORD")
        if not address or not password:
            raise RuntimeError("REDIS_ADDRESS/REDIS_PASSWORD required")
        host, separator, port_text = address.rpartition(":")
        if not separator:
            host, port_text = address, "6379"
        database = redis.Redis(host=host, port=int(port_text), username=username,
                               password=password, decode_responses=True,
                               socket_timeout=5, socket_connect_timeout=5)
    database.ping()
    environment = resolve_environment(os.environ)
    store = RedisOutbox(database, environment)
    relay = Relay(store, os.environ["DY_CALLBACK_SOURCE"], environment)
    stop = threading.Event()

    def worker():
        while not stop.is_set():
            try:
                if store.drain_one(relay.push):
                    continue
            except Exception as exc:
                # Never log request bodies, tokens, audience identifiers, or Redis URL.
                print("outbox_retry", type(exc).__name__, flush=True)
            stop.wait(1)

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *args):
            pass

        def do_GET(self):
            self.do_POST()

        def do_POST(self):
            self.connection.settimeout(10)
            try:
                length = int(self.headers.get("Content-Length", "0"))
                if length < 0 or length > MAX_BODY:
                    self.send_error(413)
                    return
                raw = self.rfile.read(length)
                body = json.loads(raw) if raw else {}
                status, result = relay.route(self.path, {k.lower(): v for k, v in self.headers.items()}, body)
            except (ValueError, TypeError):
                status, result = 400, {"ok": False, "reason": "invalid data"}
            except Exception as exc:
                print("request_failed", type(exc).__name__, flush=True)
                status, result = 503, {"ok": False, "reason": "retry later"}
            payload = json.dumps(result).encode()
            self.send_response(status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

    threading.Thread(target=worker, daemon=True).start()
    server = ThreadingHTTPServer(("0.0.0.0", int(os.environ.get("PORT", "8000"))), Handler)
    try:
        server.serve_forever()
    finally:
        stop.set()
        server.server_close()


if __name__ == "__main__":
    main()
