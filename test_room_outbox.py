import json
import unittest
import fakeredis
from service import RoomRedisOutbox, RedisOutbox, encode_event, PushRejected, Relay


class RoomOutboxTests(unittest.TestCase):
    def setUp(self):
        self.db = fakeredis.FakeRedis(decode_responses=True)
        self.store = RoomRedisOutbox(self.db, 'dev')

    def event(self, room, msg):
        return encode_event(room, 'live_comment', {'msg_id': msg, 'sec_openid': 'fixture', 'content': '1'})

    def test_offline_room_does_not_block_another_and_keeps_own_fifo(self):
        for room, msg in [('offline', 'a'), ('offline', 'b'), ('online', 'c')]:
            self.store.put('anchor', self.event(room, msg))
        delivered = []
        def send(anchor, event):
            if event['room_id'] == 'offline':
                raise PushRejected('recipient_offline', 0, 1)
            delivered.append(event['msg_id'])
        for _ in range(4):
            try:
                self.store.drain_one(send)
            except PushRejected:
                pass
        self.assertEqual(delivered, ['c'])
        queued = self.db.lrange(self.store.room_queue('anchor', 'offline'), 0, -1)
        self.assertEqual([json.loads(raw)['event']['msg_id'] for raw in queued], ['a', 'b'])
        while self.store.drain_one(lambda a, e: delivered.append(e['msg_id'])):
            pass
        self.assertEqual(delivered, ['c', 'a', 'b'])

    def test_legacy_stock_migrates_before_new_events_and_dedup_survives(self):
        old = RedisOutbox(self.db, 'dev')
        for i in range(270):
            old.put('anchor', self.event('room', str(i)))
        self.assertEqual(self.store.put('anchor', self.event('room', '0')), 0)
        self.store.put('anchor', self.event('room', 'new'))
        sent = []
        for _ in range(300):
            self.store.drain_one(lambda a, e: sent.append(e['msg_id']))
        self.assertEqual(sent, [str(i) for i in range(270)] + ['new'])
        self.assertEqual(self.db.llen(old.queue), 0)

    def test_malformed_legacy_stock_is_preserved_without_blocking(self):
        self.db.rpush(self.store.queue, 'invalid-json')
        self.store.put('anchor', self.event('room', 'good'))
        sent = []
        self.store.drain_one(lambda a, e: sent.append(e['msg_id']))
        self.assertEqual(sent, ['good'])
        self.assertEqual(self.db.lrange(self.store.quarantine, 0, -1), ['invalid-json'])

    def test_migration_respects_old_worker_lock(self):
        old = RedisOutbox(self.db, 'dev')
        old.put('anchor', self.event('room', 'old'))
        self.db.set(old.lock, 'old-worker', ex=30)
        self.assertFalse(self.store.drain_one(lambda *a: self.fail('must not push')))
        self.assertEqual(self.db.llen(old.queue), 1)

    def test_room_keys_cannot_collide_at_separator(self):
        self.assertNotEqual(self.store.room_queue('a:b', 'c'), self.store.room_queue('a', 'b:c'))

    def test_diagnostics_do_not_include_recipient_ids_or_payload(self):
        relay = Relay(self.store, 'fixture', 'dev', lambda *a: {'err_no': 0, 'data': {'failed_open_id_list': ['private-anchor']}})
        with self.assertRaises(PushRejected) as raised:
            relay.push('private-anchor', self.event('private-room', 'private-message'))
        self.assertEqual(raised.exception.reason, 'recipient_offline')
        self.assertEqual(raised.exception.failed_count, 1)
        self.assertNotIn('private', str(raised.exception))

    def test_cloud_encoded_success_is_normalized_before_acknowledgement(self):
        receipts = []
        relay = Relay(self.store, 'fixture', 'dev', lambda *a: {
            'err_no': 0, 'err_msg': 'success',
            'data': json.dumps({'failed_open_id_list': []})}, diagnostic=receipts.append)
        self.store.put('private-anchor', self.event('private-room', 'message'))
        self.assertTrue(self.store.drain_one(relay.push))
        self.assertEqual(self.db.scard(self.store.rooms), 0)
        self.assertEqual(receipts, [{'event': 'push_receipt', 'gatewayAccepted': True,
                                    'encodedData': True, 'clientConsumptionVerified': False}])

    def test_encoded_offline_recipient_and_malformed_responses_stay_pending(self):
        for data in [json.dumps({'failed_open_id_list': ['private-anchor']}),
                     'invalid-json', json.dumps({}), json.dumps(None)]:
            relay = Relay(self.store, 'fixture', 'dev', lambda *a: {'err_no': 0, 'data': data})
            with self.assertRaises(PushRejected):
                relay.push('private-anchor', self.event('private-room', 'private-message'))


if __name__ == '__main__':
    unittest.main()
