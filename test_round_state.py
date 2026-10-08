import unittest
import fakeredis
from service import APP_ID, Relay, RoomRedisOutbox, encode_event

class RoundStateTests(unittest.TestCase):
    def setUp(self):
        self.store = RoomRedisOutbox(fakeredis.FakeRedis(decode_responses=True), 'dev')
        self.relay = Relay(self.store, 'internal-callback', 'dev', post=lambda *a: {'err_no': 0})
        self.gateway = {'x-tt-appid': APP_ID, 'x-room-id': 'fixture-room', 'x-anchor-openid': 'fixture-anchor'}
        self.callback = {'x-tt-source': 'internal-callback', 'x-msg-type': 'user_group', 'x-roomid': 'fixture-room'}
    def write(self, **data):
        return self.relay.route('/start_game', self.gateway, {'operation': 'round_state', **data})
    def query(self, user='fixture-user'):
        return self.relay.route('/live_data_callback', self.callback, {'app_id': APP_ID, 'room_id': 'fixture-room', 'open_id': user})
    def test_start_join_finish_and_last_finished_round(self):
        self.assertEqual(self.query()[1]['data']['round_id'], 0)
        self.assertTrue(self.write(round_id=101, status=1)[1]['ok'])
        self.assertTrue(self.write(round_id=101, open_id='fixture-user', group_id='cat')[1]['ok'])
        self.assertEqual(self.query()[1]['data'], {'round_id':101,'round_status':1,'group_id':'cat','user_group_status':1})
        self.write(round_id=101, status=2)
        self.assertEqual(self.query()[1]['data']['round_status'], 2)
    def test_new_round_clears_membership_and_rejects_delayed_updates(self):
        self.write(round_id=101, status=1)
        self.write(round_id=101, open_id='fixture-user', group_id='dog')
        self.write(round_id=102, status=1)
        self.assertEqual(self.write(round_id=101, open_id='fixture-user', group_id='cat')[0], 409)
        self.assertEqual(self.query()[1]['data']['user_group_status'], 0)
        self.write(round_id=102, status=2)
        self.assertEqual(self.write(round_id=102, status=1)[0], 409)
    def test_identity_cannot_cross_rooms_apps_or_gateway(self):
        self.assertEqual(self.relay.route('/start_game', {}, {'operation':'round_state','round_id':1,'status':1})[0],403)
        self.assertEqual(self.relay.route('/live_data_callback', {}, {})[0],403)
        self.assertNotEqual(self.relay.route('/live_data_callback', self.callback, {'app_id':APP_ID,'room_id':'wrong','open_id':'fixture-user'})[1]['errcode'],0)
        self.assertEqual(self.write(round_id=101,status=1.0)[0],400)
        self.assertEqual(self.write(round_id=101,open_id='fixture-user',group_id='wrong')[0],400)
    def test_follow_validation_and_all_four_tasks(self):
        event=encode_event('fixture-room','live_follow',{'msg_id':'fixture','sec_openid':'fixture-user','user_follow_action':1})
        self.assertEqual(event['msg_type'],'live_follow')
        with self.assertRaises(ValueError):
            encode_event('fixture-room','live_follow',{'msg_id':'fixture','sec_openid':'fixture-user','user_follow_action':9})
        self.assertTrue(self.relay.route('/start_game',self.gateway,{})[1]['tasks']['live_follow'])

    def test_fansclub_validation_and_subscription(self):
        row={'msg_id':'fixture-fans','sec_openid':'fixture-user','fansclub_reason_type':2,'fansclub_level':1}
        self.assertEqual(encode_event('fixture-room','live_fansclub',row)['msg_type'],'live_fansclub')
        self.assertTrue(self.relay.route('/start_game',self.gateway,{})[1]['tasks']['live_fansclub'])
        for value in (0,True,1.5,101):
            with self.assertRaises(ValueError):
                encode_event('fixture-room','live_fansclub',{**row,'fansclub_level':value})

if __name__ == '__main__': unittest.main()

