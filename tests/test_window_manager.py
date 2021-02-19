from unittest.mock import patch, MagicMock

from event_service_utils.tests.base_test_case import MockedServiceStreamTestCase
from event_service_utils.tests.json_msg_helper import prepare_event_msg_tuple

from window_manager.service import WindowManager

from window_manager.conf import (
    SERVICE_STREAM_KEY,
    SERVICE_CMD_KEY,
    MATCHER_STREAM_KEY,
)


class TestWindowManager(MockedServiceStreamTestCase):
    GLOBAL_SERVICE_CONFIG = {
        'service_stream_key': SERVICE_STREAM_KEY,
        'service_cmd_key': SERVICE_CMD_KEY,
        'matcher_stream_key': MATCHER_STREAM_KEY,
        'logging_level': 'ERROR',
        'tracer_configs': {'reporting_host': None, 'reporting_port': None},
    }
    SERVICE_CLS = WindowManager
    MOCKED_STREAMS_DICT = {
        SERVICE_STREAM_KEY: [],
        SERVICE_CMD_KEY: [],
    }

    @patch('window_manager.service.WindowManager.process_action')
    def test_process_cmd_should_call_process_action(self, mocked_process_action):
        action = 'someAction'
        event_data = {
            'id': 1,
            'action': action,
            'some': 'stuff'
        }
        msg_tuple = prepare_event_msg_tuple(event_data)
        mocked_process_action.__name__ = 'process_action'

        self.service.service_cmd.mocked_values = [msg_tuple]
        self.service.process_cmd()
        self.assertTrue(mocked_process_action.called)
        self.service.process_action.assert_called_once_with(action=action, event_data=event_data, json_msg=msg_tuple[1])

    @patch('window_manager.service.WindowManager.add_query_window_action')
    def test_process_action_should_call_add_query_window_with_proper_parameters(self, mocked_add_query_w):
        event_data = {
            'id': 1,
            'action': 'addQueryWindow',
            'query_id': 'query_id',
            'window': {
                'window_type': 'TUMBLING_COUNT_WINDOW',
                'args': [2]
            }
        }
        action = event_data['action']
        json_msg = prepare_event_msg_tuple(event_data)[1]
        self.service.process_action(action, event_data, json_msg)
        mocked_add_query_w.assert_called_once_with(query_id=event_data['query_id'], window=event_data['window'])

    def test_add_query_window_action_instantiate_window_controller_with_correct_parameters(self):
        query_id = 'query_id'
        window = {
            'window_type': 'TUMBLING_COUNT_WINDOW',
            'args': ['some', 'args']
        }
        mocked_window_controller = MagicMock()
        self.service.window_controllers = {
            'TUMBLING_COUNT_WINDOW': mocked_window_controller,
        }
        self.service.add_query_window_action(query_id, window)

        mocked_window_controller.assert_called_once_with(query_id, *['some', 'args'])

    def test_add_query_window_action_should_update_datastructure_correctly(self):
        query_id = 'query_id'
        window = {
            'window_type': 'TUMBLING_COUNT_WINDOW',
            'args': ['some', 'args']
        }
        mocked_window_controller = MagicMock(return_value='instance_of_controller')

        self.service.window_controllers = {
            'TUMBLING_COUNT_WINDOW': mocked_window_controller,
        }
        self.service.add_query_window_action(query_id, window)

        self.assertIn(query_id, self.service.query_windows)
        self.assertEquals('instance_of_controller', self.service.query_windows[query_id])

    def test_add_query_window_action_shouldnt_do_anything_if_non_supported_window_type(self):
        query_id = 'query_id'
        window = {
            'window_type': 'no_support',
            'args': ['some', 'args']
        }
        mocked_window_controller = MagicMock()

        self.service.window_controllers = {
            'TUMBLING_COUNT_WINDOW': mocked_window_controller,
        }
        self.service.add_query_window_action(query_id, window)
        self.assertFalse(mocked_window_controller.called)
        self.assertNotIn(query_id, self.service.query_windows)

    def test_add_query_window_action_shouldnt_do_anything_if_query_window_duplicate(self):
        query_id = 'query_id'
        window = {
            'window_type': 'TUMBLING_COUNT_WINDOW',
            'args': ['some', 'args']
        }
        self.service.query_windows = {query_id: 'old_controller'}

        mocked_window_controller = MagicMock(return_value='new_controller')
        self.service.window_controllers = {
            'TUMBLING_COUNT_WINDOW': mocked_window_controller,
        }

        self.service.add_query_window_action(query_id, window)

        self.assertFalse(mocked_window_controller.called)
        self.assertIn(query_id, self.service.query_windows)
        self.assertEqual('old_controller', self.service.query_windows[query_id])

    @patch('window_manager.service.WindowManager.add_event_to_query_windows')
    def test_process_data_event_should_call_add_event_to_query_windows(self, mocked_update_query_w):
        event_data = {
            'id': 'event-id-1',
            'vekg': {},
            'query_ids': ['query_id1'],
            'buffer_stream_key': '12345',
        }
        json_msg = prepare_event_msg_tuple(event_data)[1]

        self.service.process_data_event(event_data, json_msg)
        self.assertTrue(mocked_update_query_w.called)

    def test_add_event_to_query_windows_should_call_window_controller_for_each_query_id(self):
        query_1_window_controller = MagicMock()
        query_2_window_controller = MagicMock()

        self.service.query_windows = {
            'query_id1': query_1_window_controller,
            'query_id2': query_2_window_controller,
        }
        event_data = {
            'id': 'event-id-1',
            'vekg': {},
            'query_ids': ['query_id1', 'query_id2'],
            'buffer_stream_key': '12345',
        }

        self.service.add_event_to_query_windows(event_data)
        query_1_window_controller.update_windows.assert_called_once_with(event_data)
        query_2_window_controller.update_windows.assert_called_once_with(event_data)

    @patch('window_manager.service.WindowManager.send_finished_windows')
    @patch('window_manager.service.WindowManager.add_event_to_query_windows')
    def test_process_data_event_should_call_send_finished_windows(self, mocked_add_event, mocked_send_windows):
        event_data = {
            'id': 'event-id-1',
            'vekg': {},
            'query_ids': ['query_id1'],
            'buffer_stream_key': '12345',
        }
        json_msg = prepare_event_msg_tuple(event_data)[1]

        self.service.process_data_event(event_data, json_msg)
        self.assertTrue(mocked_send_windows.called)
        self.assertTrue(mocked_add_event.called)

    @patch('window_manager.service.WindowManager.send_window_to_matcher')
    def test_send_finished_windows_should_call_send_windows_to_matcher_for_each_window(self, mocked_send_to_matcher):
        query_1_window_controller = MagicMock()
        query_2_window_controller = MagicMock()

        query_1_window_controller.get_and_reset_finished_bufferstream_windows.return_value = ([1, 2, 3],)
        query_2_window_controller.get_and_reset_finished_bufferstream_windows.return_value = ([4, 5, 6],)
        self.service.query_windows = {
            'query_id1': query_1_window_controller,
            'query_id2': query_2_window_controller,
        }
        self.service.send_finished_windows()
        self.assertEqual(2, mocked_send_to_matcher.call_count)
        self.assertListEqual(['query_id1', [1, 2, 3]], list((mocked_send_to_matcher.mock_calls[0])[1]))
        self.assertListEqual(['query_id2', [4, 5, 6]], list((mocked_send_to_matcher.mock_calls[1])[1]))
