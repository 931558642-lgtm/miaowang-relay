import json
import threading
import unittest
from http.client import HTTPConnection
from http.server import ThreadingHTTPServer
import fakeredis
from service import APP_ID, STOP_TASK_URL, RedisOutbox, Relay, create_handler, encode_event, resolve_environment


class EnvironmentTests(unittest.TestCase):
    def test_system_environment_needs_no_custom_duplicate(self):
        self.assertEqual(resolve_environment({"CLOUD_ENV": "DEV"}), "dev")
        self.assertEqual(resolve_environment({"CLOUD_ENV": "PROD"}), "prod")

    def test_legacy_explicit_environment(self):
        self.assertEqual(resolve_environment({"DY_ENV": "dev"}), "dev")

    def test_cross_environment_configuration_rejected(self):
        with self.assertRaises(ValueError):
            resolve_environment({"CLOUD_ENV": "PROD", "DY_ENV": "dev"})

    def test_missing_or_invalid_environment_rejected(self):
        for config in ({}, {"CLOUD_ENV": "preview"}, {"DY_ENV": "unknown"}):
            with self.subTest(config=config), self.assertRaises(ValueError):
                resolve_environment(config)


class RelayTests(unittest.TestCase):
    def setUp(self):
        self.redis = fakeredis.FakeRedis(decode_responses=True)
        self.store = RedisOutbox(self.redis, "dev")
        self.calls = []
        def post(url, data, headers=None):
            self.calls.append((url, data, headers))
            return {"err_no": 0, "data": {"failed_open_id_list": []}}
        self.relay = Relay(self.store, "test-only-source", "dev", post)
        self.headers = {"x-tt-appid": APP_ID, "x-room-id": "room1", "x-anchor-openid": "anchor1",
                        "x-tt-source": "test-only-source", "x-msg-type": "live_gift"}
        self.gift = {"msg_id": "m1", "sec_openid": "u1", "gift_num": 3, "sec_gift_id": "official-id", "test": True}

    def callback(self, rows):
        return self.relay.route("/live_data_callback", self.headers, rows)

    def test_batch_one_envelope_per_event(self):
        self.assertEqual(self.callback([self.gift, {**self.gift, "msg_id": "m2"}])[0], 200)
        self.store.drain_one(self.relay.push)
        self.store.drain_one(self.relay.push)
        self.assertEqual([c[1]["msg_id"] for c in self.calls], ["m1", "m2"])
        self.assertTrue(all(len(json.loads(c[1]["data"])) == 1 for c in self.calls))
        self.assertTrue(json.loads(self.calls[0][1]["data"])[0]["test"])
        self.assertEqual(json.loads(self.calls[0][1]["extra_data"])["room_id"], "room1")
        self.assertNotIn("room_id", self.calls[0][1])

    def test_retry_does_not_duplicate_outbox(self):
        self.callback([self.gift]); self.callback([self.gift])
        self.assertEqual(self.redis.llen(self.store.queue), 1)

    def test_same_id_different_room_is_independent(self):
        self.callback([self.gift]); self.headers["x-room-id"] = "room2"
        self.callback([self.gift])
        self.assertEqual(self.redis.llen(self.store.queue), 2)

    def test_failed_gateway_keeps_head_and_order(self):
        self.callback([self.gift, {**self.gift, "msg_id": "m2"}])
        def fail(*args): raise TimeoutError()
        with self.assertRaises(TimeoutError): self.store.drain_one(fail)
        self.assertEqual(self.redis.llen(self.store.queue), 2)
        self.store.drain_one(self.relay.push)
        self.assertEqual(self.calls[0][1]["msg_id"], "m1")

    def test_unknown_gateway_response_is_not_success(self):
        self.callback([self.gift])
        self.relay.post = lambda *args: {"unexpected": "response"}
        with self.assertRaises(RuntimeError): self.store.drain_one(self.relay.push)
        self.assertEqual(self.redis.llen(self.store.queue), 1)

    def test_recipient_failure_keeps_message_until_success(self):
        self.callback([self.gift])
        original = self.redis.lindex(self.store.queue, 0)
        self.relay.post = lambda *args: {"err_no": 0, "data": {"failed_open_id_list": ["anchor1"]}}
        with self.assertRaises(RuntimeError): self.store.drain_one(self.relay.push)
        self.assertEqual(self.redis.lindex(self.store.queue, 0), original)
        self.assertIsNone(self.redis.get(self.store.lock))
        self.relay.post = lambda *args: {"err_no": 0, "data": {"failed_open_id_list": []}}
        self.assertTrue(self.store.drain_one(self.relay.push))
        self.assertEqual(self.redis.llen(self.store.queue), 0)

    def test_ambiguous_gateway_acceptance_keeps_message(self):
        self.callback([self.gift])
        for response in (None, [], {"err_no": 0}, {"err_no": False, "data": {"failed_open_id_list": []}},
                         {"err_no": 0, "data": None}, {"err_no": 0, "data": {}},
                         {"err_no": 0, "data": {"failed_open_id_list": ""}},
                         {"err_no": 0, "data": {"failed_open_id_list": ["other-anchor"]}}):
            with self.subTest(response=response):
                self.relay.post = lambda *args: response
                with self.assertRaises(RuntimeError): self.store.drain_one(self.relay.push)
                self.assertEqual(self.redis.llen(self.store.queue), 1)

    def test_missing_source_rejected(self):
        del self.headers["x-tt-source"]
        self.assertEqual(self.callback([self.gift])[0], 403)
        self.assertEqual(self.redis.llen(self.store.queue), 0)

    def test_other_application_rejected(self):
        self.headers["x-tt-appid"] = "other"
        self.assertEqual(self.callback([self.gift])[0], 403)

    def test_invalid_batch_has_no_partial_write(self):
        with self.assertRaises(ValueError): self.callback([self.gift, {"msg_id": "bad"}])
        self.assertEqual(self.redis.llen(self.store.queue), 0)

    def test_zero_negative_and_boolean_counts_rejected(self):
        for value in (0, -1, 10001, True):
            with self.assertRaises(ValueError): encode_event("room", "live_gift", {**self.gift, "gift_num": value})

    def test_task_failure_not_reported_as_success(self):
        self.relay.post = lambda *args: {"err_no": 400}
        code, body = self.relay.route("/start_game", self.headers, {})
        self.assertEqual(code, 502); self.assertFalse(body["ok"])

    def test_task_start_opens_five_required_types(self):
        self.assertEqual(self.relay.route("/start_game", self.headers, {})[0], 200)
        self.assertEqual([c[1]["msg_type"] for c in self.calls], ["live_comment", "live_like", "live_gift", "live_follow", "live_fansclub"])

    def test_task_stop_can_repeat_without_changing_identity_or_queue(self):
        self.callback([self.gift])
        before = self.redis.lrange(self.store.queue, 0, -1)
        for _ in range(2):
            code, body = self.relay.route("/stop_game", self.headers, {})
            self.assertEqual(code, 200)
            self.assertTrue(body["ok"])
        self.assertEqual([c[0] for c in self.calls], [STOP_TASK_URL] * 10)
        expected = [{"appid": APP_ID, "roomid": "room1", "msg_type": kind}
                    for kind in ("live_comment", "live_like", "live_gift", "live_follow", "live_fansclub")] * 2
        self.assertEqual([c[1] for c in self.calls], expected)
        self.assertEqual(self.redis.lrange(self.store.queue, 0, -1), before)

    def test_task_stop_partial_failure_reports_failure_and_retries_all_types(self):
        def partial(url, data, headers=None):
            self.calls.append((url, data, headers))
            return {"err_no": 10001 if data["msg_type"] == "live_like" else 0}
        self.relay.post = partial
        code, body = self.relay.route("/stop_game", self.headers, {})
        self.assertEqual(code, 502)
        self.assertFalse(body["ok"])
        self.assertEqual(body["tasks"], {"live_comment": True, "live_like": False, "live_gift": True, "live_follow": True, "live_fansclub": True})
        self.relay.post = lambda url, data, headers=None: (
            self.calls.append((url, data, headers)) or {"err_no": 0})
        self.assertTrue(self.relay.route("/stop_game", self.headers, {})[1]["ok"])
        self.assertEqual(len(self.calls), 10)

    def test_task_stop_transport_failure_still_attempts_remaining_types(self):
        def timeout(url, data, headers=None):
            self.calls.append((url, data, headers))
            if data["msg_type"] == "live_comment":
                raise TimeoutError()
            return {"err_no": 0}
        self.relay.post = timeout
        code, body = self.relay.route("/stop_game", self.headers, {})
        self.assertEqual(code, 502)
        self.assertFalse(body["ok"])
        self.assertEqual(len(self.calls), 5)
        self.assertTrue(body["tasks"]["live_gift"])

    def test_task_diagnostics_preserve_codes_and_omit_private_values(self):
        records = []
        self.relay.diagnostic = records.append
        self.relay.post = lambda url, data, headers=None: {
            "err_no": 20001 if data["msg_type"] == "live_comment" else 0,
            "err_msg": "do-not-save-private-response",
            "token": "do-not-save-token"}
        status, body = self.relay.route("/start_game", self.headers, {})
        self.assertEqual(status, 502)
        self.assertEqual(body["errors"], {"live_comment": 20001, "live_like": 0, "live_gift": 0, "live_follow": 0, "live_fansclub": 0})
        self.assertEqual(records[0]["tasks"], body["tasks"])
        raw = json.dumps(records)
        for private in ("do-not-save", "room1", "anchor1", "err_msg", "token"):
            self.assertNotIn(private, raw)

    def test_task_diagnostics_use_null_for_ambiguous_or_transport_results(self):
        def post(url, data, headers=None):
            if data["msg_type"] == "live_comment":
                raise TimeoutError()
            return {"err_no": False}
        self.relay.post = post
        status, body = self.relay.route("/start_game", self.headers, {})
        self.assertEqual(status, 502)
        self.assertEqual(body["errors"], dict.fromkeys(("live_comment", "live_like", "live_gift", "live_follow", "live_fansclub")))
        self.assertFalse(any(body["tasks"].values()))

    def test_diagnostic_sink_failure_cannot_change_task_outcome(self):
        def broken_sink(record):
            raise OSError("fixture log writer is unavailable")
        self.relay.diagnostic = broken_sink
        status, body = self.relay.route("/start_game", self.headers, {})
        self.assertEqual(status, 200)
        self.assertTrue(body["ok"])

    def test_task_operations_reject_ambiguous_success_responses(self):
        for path in ("/start_game", "/stop_game"):
            for response in (None, [], {}, {"err_no": False}, {"err_no": "0"}, {"err_no": 0.0}):
                with self.subTest(path=path, response=response):
                    self.relay.post = lambda *args: response
                    code, body = self.relay.route(path, self.headers, {})
                    self.assertEqual(code, 502)
                    self.assertFalse(body["ok"])

    def test_task_stop_requires_gateway_identity(self):
        for field in ("x-tt-appid", "x-room-id", "x-anchor-openid"):
            headers = dict(self.headers)
            del headers[field]
            with self.subTest(field=field):
                self.assertEqual(self.relay.route("/stop_game", headers, {})[0], 403)
        self.assertEqual(self.calls, [])

    def test_client_uplink_cannot_inject_gifts(self):
        self.headers["x-tt-event-type"] = "uplink"
        self.assertEqual(self.relay.route("/websocket_callback", self.headers, [self.gift])[0], 200)
        self.assertEqual(self.redis.llen(self.store.queue), 0)

    def test_dev_prod_queues_are_separate(self):
        prod = RedisOutbox(self.redis, "prod")
        self.assertNotEqual(prod.queue, self.store.queue)

    def test_restart_can_resume_persisted_outbox(self):
        self.callback([self.gift])
        replacement = RedisOutbox(self.redis, "dev")
        self.assertTrue(replacement.drain_one(self.relay.push))
        self.assertEqual(self.calls[0][1]["msg_id"], "m1")

    def test_worker_lock_prevents_concurrent_consumption(self):
        self.callback([self.gift]); self.redis.set(self.store.lock, "other", ex=30)
        self.assertFalse(self.store.drain_one(self.relay.push))
        self.assertEqual(self.calls, [])


class HeadProbeTests(unittest.TestCase):
    def setUp(self):
        self.routes = []
        class NoRouteRelay:
            def route(inner, *args):
                self.routes.append(args)
                raise AssertionError("HEAD must not invoke application routes")
        self.server = ThreadingHTTPServer(("127.0.0.1", 0), create_handler(NoRouteRelay()))
        self.worker = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.worker.start()

    def tearDown(self):
        self.server.shutdown()
        self.server.server_close()
        self.worker.join(timeout=2)

    def head(self, path):
        connection = HTTPConnection("127.0.0.1", self.server.server_port, timeout=2)
        try:
            connection.request("HEAD", path, headers={"Content-Length": "9999"})
            response = connection.getresponse()
            result = response.status, response.read()
            self.assertEqual(response.getheader("Content-Length"), "0")
            return result
        finally:
            connection.close()

    def test_probe_head_is_empty_and_does_not_touch_routes_or_body(self):
        for path in ("/", "/healthz", "/live_data_callback"):
            with self.subTest(path=path):
                self.assertEqual(self.head(path), (200, b""))
        self.assertEqual(self.routes, [])

    def test_head_cannot_start_stop_or_access_unknown_routes(self):
        for path in ("/start_game", "/stop_game", "/websocket_callback", "/unknown"):
            with self.subTest(path=path):
                self.assertEqual(self.head(path), (405, b""))
        self.assertEqual(self.routes, [])


if __name__ == "__main__":
    unittest.main(verbosity=2)

