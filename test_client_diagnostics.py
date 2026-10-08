import json
import unittest
import fakeredis
from service import APP_ID, TYPES, RedisOutbox, Relay


class ClientDiagnosticsTests(unittest.TestCase):
    def setUp(self):
        self.redis = fakeredis.FakeRedis(decode_responses=True)
        self.records, self.calls = [], []
        self.relay = Relay(RedisOutbox(self.redis, 'dev'), 'source', 'dev', lambda *a: self.calls.append(a), self.records.append)
        self.headers = {'x-tt-appid': APP_ID, 'x-room-id': 'private-room', 'x-anchor-openid': 'private-anchor'}
        self.body = dict(operation='client_diagnostics', schema=1, clientVersion='0.0.53', stage='Connected', protocolFailure='None', active=True,
                         canApply=True, canRender=True, connected=True, selfCheck=True, lastStartHttp=200,
                         **dict.fromkeys(('packets', 'outsideSession', 'malformed', 'queued', 'pendingAck', 'acks', 'ackRetries', 'startAttempts'), 0),
                         decoded=dict.fromkeys(TYPES, 0), applied=dict.fromkeys(TYPES, 0), tasks=dict.fromkeys(TYPES, 1),
                         results=dict.fromkeys(('Applied', 'Duplicate', 'WrongRoom', 'Invalid', 'TestRejected', 'NotPlaying', 'NotJoined', 'UnknownGift', 'Ignored', 'CapacityReached'), 0))

    def send(self, body=None, headers=None):
        return self.relay.route('/start_game', self.headers if headers is None else headers, self.body if body is None else body)

    def test_heartbeat_never_starts_tasks_or_mutates_rounds(self):
        self.assertEqual(self.send()[0], 200)
        self.assertEqual(self.calls, [])
        self.assertIsNone(self.redis.hget(self.relay.round_keys('private-room')[0], 'status'))
        self.assertEqual(self.records[0]['event'], 'client_diagnostics')
        self.assertNotIn('private', json.dumps(self.records))

    def test_identity_required_and_other_app_rejected(self):
        for key in self.headers:
            headers = dict(self.headers); del headers[key]
            self.assertEqual(self.send(headers=headers)[0], 403)
        self.assertEqual(self.send(headers={**self.headers, 'x-tt-appid': 'other'})[0], 403)
        self.assertEqual(self.records, [])

    def test_invalid_or_sensitive_fields_rejected_without_logging(self):
        for body in ({**self.body, 'token': 'SECRET'}, {**self.body, 'stage': 'SECRET'},
                     {**self.body, 'clientVersion': 'SECRET'}, {**self.body, 'packets': True},
                     {**self.body, 'acks': -1}, {**self.body, 'queued': 2**53},
                     {**self.body, 'tasks': {**self.body['tasks'], 'live_gift': 'SECRET'}},
                     {**self.body, 'decoded': {'private-user': 1}}, {**self.body, 'schema': True}):
            with self.subTest(body=body):
                self.assertEqual(self.send(body)[0], 400)
        self.assertEqual(self.calls, [])
        self.assertEqual(self.records, [])

    def test_room_rate_limit_and_diagnostic_sink_failure(self):
        for _ in range(10): self.assertEqual(self.send()[0], 200)
        self.assertEqual(len(self.records), 1)
        self.headers['x-room-id'] = 'other-room'
        self.assertEqual(self.send()[0], 200)
        self.assertEqual(len(self.records), 2)
        self.headers['x-room-id'] = 'third-room'
        self.relay.diagnostic = lambda r: (_ for _ in ()).throw(OSError('SECRET'))
        self.assertEqual(self.send()[0], 200)

    def test_unknown_operation_cannot_accidentally_start_tasks(self):
        for body in ([], {'operation': 'typo'}, {'operation': 'client_diagnostics'}):
            self.assertEqual(self.send(body)[0], 400)
        self.assertEqual(self.calls, [])


if __name__ == '__main__': unittest.main()
