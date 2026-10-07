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
TYPES = ("live_comment", "live_like", "live_gift", "live_follow")
TASK_URL = "http://webcast-bytedance-com.openapi.dyc.ivolces.com/api/live_data/task/start"
STOP_TASK_URL = "http://webcast-bytedance-com.openapi.dyc.ivolces.com/api/live_data/task/stop"
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
    if kind == 'live_follow' and (type(row.get('user_follow_action')) is not int or row['user_follow_action'] not in (1, 2, 3)):
        raise ValueError('invalid follow action')
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


class PushRejected(RuntimeError):
    def __init__(self, reason, code=None, failed_count=0):
        super().__init__(reason)
        self.reason, self.code, self.failed_count = reason, code, failed_count


class RoomRedisOutbox(RedisOutbox):
    """Preserve old pending packets while isolating failed rooms from live rooms."""
    MIGRATE = """
    if redis.call('GET', KEYS[4]) ~= ARGV[2] then return 0 end
    if redis.call('LINDEX', KEYS[1], -1) ~= ARGV[1] then return 0 end
    redis.call('LPUSH', KEYS[3], ARGV[1])
    if ARGV[3] == 'room' then redis.call('SADD', KEYS[2], KEYS[3]) end
    redis.call('RPOP', KEYS[1])
    return 1
    """
    ROOM_ADD = """
    if redis.call('EXISTS', KEYS[1]) == 1 then return 0 end
    if redis.call('LLEN', KEYS[2]) >= tonumber(ARGV[2]) then return -1 end
    redis.call('RPUSH', KEYS[2], ARGV[1])
    redis.call('SADD', KEYS[3], KEYS[2])
    redis.call('SET', KEYS[1], '1', 'EX', 2592000)
    return 1
    """
    ROOM_POP = """
    if redis.call('GET', KEYS[1]) ~= ARGV[1] then return 0 end
    if redis.call('LINDEX', KEYS[2], 0) ~= ARGV[2] then return 0 end
    redis.call('LPOP', KEYS[2])
    if redis.call('LLEN', KEYS[2]) == 0 then redis.call('SREM', KEYS[3], KEYS[2]) end
    return 1
    """

    def __init__(self, redis, environment):
        super().__init__(redis, environment)
        self.rooms = self.prefix + 'pending_rooms'
        self.quarantine = self.prefix + 'quarantine'
        self.cursor = 0

    def room_queue(self, anchor, room):
        identity = str(len(anchor.encode())) + ':' + anchor + room
        return self.prefix + 'room:' + hashlib.sha1(identity.encode()).hexdigest()

    def put(self, anchor, event):
        identity = json.dumps([event['room_id'], event['msg_type'], event['msg_id']])
        key = self.prefix + 'seen:' + hashlib.sha256(identity.encode()).hexdigest()
        raw = json.dumps({'anchor': anchor, 'event': event}, ensure_ascii=False)
        result = self.redis.eval(self.ROOM_ADD, 3, key, self.room_queue(anchor, event['room_id']),
                                 self.rooms, raw, 100000)
        if result < 0:
            raise RuntimeError('outbox capacity reached')
        return result

    def drain_one(self, send):
        # Reverse-transfer legacy stock so older packets prepend ahead of new
        # packets, atomically and without a delete/reset of users' pending gifts.
        migration_owner = str(uuid.uuid4())
        if not self.redis.set(self.lock, migration_owner, nx=True, ex=30):
            return False
        try:
            for raw in reversed(self.redis.lrange(self.queue, -128, -1)):
                target, kind = self.quarantine, 'quarantine'
                try:
                    item = json.loads(raw)
                    if identifier(item.get('anchor')) and isinstance(item.get('event'), dict) and identifier(item['event'].get('room_id')):
                        target = self.room_queue(item['anchor'], item['event']['room_id'])
                        kind = 'room'
                except (ValueError, TypeError, AttributeError):
                    pass
                if not self.redis.eval(self.MIGRATE, 4, self.queue, self.rooms, target, self.lock, raw, migration_owner, kind):
                    return False
        finally:
            self.redis.eval(self.UNLOCK, 1, self.lock, migration_owner)
        if self.redis.llen(self.queue):
            return False
        queues = sorted(self.redis.smembers(self.rooms))
        if not queues:
            return False
        queue = queues[self.cursor % len(queues)]
        self.cursor += 1  # Advances even on rejection, so another room gets a turn.
        lock = queue + ':worker'
        owner = str(uuid.uuid4())
        if not self.redis.set(lock, owner, nx=True, ex=30):
            return False
        try:
            raw = self.redis.lindex(queue, 0)
            if raw is None:
                return False
            item = json.loads(raw)
            send(item['anchor'], item['event'])
            return bool(self.redis.eval(self.ROOM_POP, 3, lock, queue, self.rooms, owner, raw))
        finally:
            self.redis.eval(self.UNLOCK, 1, lock, owner)


class Relay:
    ROUND_WRITE = """
    local current=tonumber(redis.call('HGET',KEYS[1],'round_id') or '0')
    local incoming=tonumber(ARGV[1])
    if incoming < current then return -1 end
    if ARGV[2] == 'group' then
      if incoming ~= current or redis.call('HGET',KEYS[1],'status') ~= '1' then return -1 end
      redis.call('HSET',KEYS[2],ARGV[3],ARGV[4])
    else
      if incoming == current and redis.call('HGET',KEYS[1],'status') == '2' and ARGV[3] == '1' then return -1 end
      if incoming > current then redis.call('DEL',KEYS[2]) end
      redis.call('HSET',KEYS[1],'round_id',ARGV[1],'status',ARGV[3])
    end
    redis.call('EXPIRE',KEYS[1],2592000);redis.call('EXPIRE',KEYS[2],2592000)
    return 1
    """

    def round_keys(self, room):
        key = self.store.prefix + 'round:' + hashlib.sha256(room.encode()).hexdigest()
        return key, key + ':groups'

    def save_round(self, room, body):
        if not isinstance(body, dict) or type(body.get('round_id')) is not int or not 0 < body['round_id'] < 2**53:
            return 400, {'ok': False, 'reason': 'round identity'}
        keys = self.round_keys(room)
        if 'group_id' in body:
            if body['group_id'] not in ('cat', 'dog') or not identifier(body.get('open_id')):
                return 400, {'ok': False, 'reason': 'group identity'}
            args = (body['round_id'], 'group', body['open_id'], body['group_id'])
        else:
            if type(body.get('status')) is not int or body['status'] not in (1, 2):
                return 400, {'ok': False, 'reason': 'round status'}
            args = (body['round_id'], 'status', body['status'])
        result = self.store.redis.eval(self.ROUND_WRITE, 2, *keys, *args)
        return (200, {'ok': True}) if result == 1 else (409, {'ok': False, 'reason': 'stale round'})

    def query_group(self, room, body):
        if not isinstance(body, dict) or body.get('app_id') != APP_ID or body.get('room_id') != room or not identifier(body.get('open_id')):
            return 200, {'errcode': 40001, 'errmsg': 'invalid identity'}
        keys = self.round_keys(room)
        # One transaction keeps round and membership consistent during a new round.
        with self.store.redis.pipeline(transaction=True) as pipe:
            pipe.hgetall(keys[0]); pipe.hget(keys[1], body['open_id'])
            state, group = pipe.execute()
        group = group if group in ('cat', 'dog') else ''
        return 200, {'errcode': 0, 'errmsg': 'success', 'data': {
            'round_id': int(state.get('round_id', 0)), 'round_status': int(state.get('status', 2)),
            'user_group_status': 1 if group else 0, 'group_id': group}}

    def __init__(self, store, callback_source, environment, post=post_json, diagnostic=None):
        if not callback_source or environment not in ("dev", "prod"):
            raise ValueError("verified callback source and environment required")
        self.store, self.source, self.environment, self.post = store, callback_source, environment, post
        self.diagnostic = diagnostic

    def record_tasks(self, operation, results, errors):
        # Only fixed names, booleans and integer error codes. No response text,
        # room, anchor, audience, token, key or raw callback enters this record.
        if self.diagnostic is not None:
            try:
                self.diagnostic({"event": "tasks_" + operation, "tasks": results,
                                 "errors": errors, "environment": self.environment})
            except Exception:
                # Diagnostic output cannot turn completed task operations into
                # a false failure. No request body or secret is used to recover.
                pass

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
            if kind == 'user_group' and identifier(room, 256):
                return self.query_group(room, body)
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
        if path in ("/start_game", "/stop_game"):
            if path == '/start_game' and isinstance(body, dict) and body.get('operation') == 'round_state':
                return self.save_round(headers['x-room-id'], body)
            # Both task operations are platform-idempotent. Never cache success:
            # partial failure or a lost response can safely retry all three types.
            url = TASK_URL if path == "/start_game" else STOP_TASK_URL
            result, errors = {}, {}
            for kind in TYPES:
                try:
                    response = self.post(url, {"appid": APP_ID, "roomid": headers["x-room-id"], "msg_type": kind})
                    result[kind] = (isinstance(response, dict) and
                                    type(response.get("err_no")) is int and response["err_no"] == 0)
                    errors[kind] = response["err_no"] if isinstance(response, dict) and type(response.get("err_no")) is int else None
                except Exception:
                    # Still attempt the other types, without logging credentials.
                    result[kind] = False
                    errors[kind] = None
            ok = all(result.values())
            self.record_tasks("start" if path == "/start_game" else "stop", result, errors)
            return (200 if ok else 502), {"ok": ok, "tasks": result, "errors": errors}
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
            code = response.get("err_no") if isinstance(response, dict) else None
            raise PushRejected("gateway_error", code if type(code) is int else None)
        data = response.get("data")
        # Live cloud evidence: the gateway wraps this object in a JSON string.
        # Normalize that representation, then require the same explicit receipt.
        encoded_data = isinstance(data, str)
        if encoded_data:
            try:
                data = json.loads(data)
            except (ValueError, TypeError):
                pass
        # The gateway can report recipient failures even when err_no is zero.
        # Only an explicit empty failure list permits removal from the outbox.
        if not isinstance(data, dict) or not isinstance(data.get("failed_open_id_list"), list):
            error = PushRejected("response_shape", 0)
            # Describe only field existence/types. Never record body values or IDs.
            error.response_shape = {
                "dataType": type(data).__name__,
                "hasData": "data" in response,
                "hasFailedOpenIds": isinstance(data, dict) and "failed_open_id_list" in data,
                "failedOpenIdsType": type(data.get("failed_open_id_list")).__name__ if isinstance(data, dict) else "absent",
                "hasFailedSessionIds": isinstance(data, dict) and "failed_session_id_list" in data,
                "hasFailuresAtRoot": "failed_open_id_list" in response,
                "dataEmpty": data in (None, "", {}) if not isinstance(data, list) else not data,
                "successMessage": response.get("err_msg") == "success",
            }
            raise error
        if data["failed_open_id_list"]:
            raise PushRejected("recipient_offline", 0, len(data["failed_open_id_list"]))
        if self.diagnostic is not None:
            try:
                self.diagnostic({"event": "push_receipt", "gatewayAccepted": True,
                                 "encodedData": encoded_data, "clientConsumptionVerified": False})
            except Exception:
                pass


def create_handler(relay):
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *args):
            pass

        def do_HEAD(self):
            # Platform domain validation and liveness only: no body parsing,
            # Redis access, or task/callback routing. POST authentication is unchanged.
            probe = self.path in ("/", "/healthz", "/live_data_callback")
            self.send_response(200 if probe else 405)
            self.send_header("Content-Length", "0")
            if not probe:
                self.send_header("Allow", "GET, POST")
            self.end_headers()

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

    return Handler


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
    store = RoomRedisOutbox(database, environment)
    relay = Relay(store, os.environ["DY_CALLBACK_SOURCE"], environment,
                  diagnostic=lambda record: print(json.dumps(record, separators=(",", ":")), flush=True))
    stop = threading.Event()

    def worker():
        while not stop.is_set():
            try:
                if store.drain_one(relay.push):
                    continue
            except Exception as exc:
                # Never log request bodies, tokens, audience identifiers, or Redis URL.
                record = {'event': 'outbox_retry', 'exception': type(exc).__name__}
                if isinstance(exc, PushRejected):
                    record.update(reason=exc.reason, errorCode=exc.code, failedRecipients=exc.failed_count)
                    if hasattr(exc, "response_shape"):
                        record["responseShape"] = exc.response_shape
                print(json.dumps(record, separators=(',', ':')), flush=True)
            stop.wait(1)

    threading.Thread(target=worker, daemon=True).start()
    server = ThreadingHTTPServer(("0.0.0.0", int(os.environ.get("PORT", "8000"))), create_handler(relay))
    try:
        server.serve_forever()
    finally:
        stop.set()
        server.server_close()


if __name__ == "__main__":
    main()
