import json
import unittest
import fakeredis
from service import APP_ID, RedisOutbox, Relay, encode_event, resolve_environment


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
        self.assertEqual(self.calls[0][1]["room_id"], "room1")

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

    def test_task_start_opens_only_three_required_types(self):
        self.assertEqual(self.relay.route("/start_game", self.headers, {})[0], 200)
        self.assertEqual([c[1]["msg_type"] for c in self.calls], ["live_comment", "live_like", "live_gift"])

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


if __name__ == "__main__":
    unittest.main(verbosity=2)
