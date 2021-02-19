from unittest import TestCase
from unittest.mock import patch, MagicMock

from window_manager.window_controllers import TumblingCountWindowController


class TumblingCountWindowControllerTestCase(TestCase):
    def setUp(self):
        self.window_controller_class = TumblingCountWindowController
        self.window_controller = self.window_controller_class('query_id1', 3)
        self.event_data1 = {
            'id': 'event-id-1',
            'vekg': {},
            'query_ids': ['query_id1'],
            'buffer_stream_key': '12345',
        }
        self.event_data2 = {
            'id': 'event-id-2',
            'vekg': {},
            'query_ids': ['query_id1'],
            'buffer_stream_key': '12345',
        }
        self.event_data3 = {
            'id': 'event-id-3',
            'vekg': {},
            'query_ids': ['query_id1'],
            'buffer_stream_key': '12345',
        }
        self.other_buffer_event_data4 = {
            'id': 'event-id-4',
            'vekg': {},
            'query_ids': ['query_id1'],
            'buffer_stream_key': '6789',
        }

    def test_repr_is_correct(self):
        self.assertEquals('TumblingCountWindowController(query_id1, *(3,))', self.window_controller.__repr__())

    def test_update_windows_correctly_updates_datastructure_if_first_event(self):
        buffer_stream_key = self.event_data1['buffer_stream_key']
        self.window_controller.update_windows(self.event_data1)
        self.assertIn(buffer_stream_key, self.window_controller.bufferstream_to_window_map)
        self.assertEqual([self.event_data1], self.window_controller.bufferstream_to_window_map[buffer_stream_key])

    def test_update_windows_correctly_updates_datastructure_if_nth_event(self):
        buffer_stream_key = self.event_data1['buffer_stream_key']
        self.window_controller.update_windows(self.event_data1)
        self.window_controller.update_windows(self.event_data2)
        self.assertIn(buffer_stream_key, self.window_controller.bufferstream_to_window_map)
        self.assertListEqual(
            [self.event_data1, self.event_data2],
            self.window_controller.bufferstream_to_window_map[buffer_stream_key]
        )

    def test_update_windows_correctly_updates_datastructure_if_different_buffer(self):
        buffer_stream_key1 = self.event_data1['buffer_stream_key']
        buffer_stream_key2 = self.other_buffer_event_data4['buffer_stream_key']
        self.window_controller.update_windows(self.event_data1)
        self.window_controller.update_windows(self.other_buffer_event_data4)
        self.window_controller.update_windows(self.event_data2)
        self.assertListEqual(
            [self.event_data1, self.event_data2],
            self.window_controller.bufferstream_to_window_map[buffer_stream_key1]
        )
        self.assertListEqual(
            [self.other_buffer_event_data4],
            self.window_controller.bufferstream_to_window_map[buffer_stream_key2]
        )

    def test_update_windows_correctly_updates_finished_windows_datastructure(self):
        buffer_stream_key1 = self.event_data1['buffer_stream_key']
        self.window_controller.update_windows(self.event_data1)
        self.window_controller.update_windows(self.event_data2)
        self.window_controller.update_windows(self.event_data3)
        self.assertListEqual(
            [],
            self.window_controller.bufferstream_to_window_map[buffer_stream_key1]
        )
        expected_finished_window = [self.event_data1, self.event_data2, self.event_data3]
        self.assertListEqual(
            expected_finished_window,
            self.window_controller.finished_bufferstream_to_window_map[buffer_stream_key1]
        )

    def test_update_windows_correctly_updates_finished_windows_datastructure_and_start_new_window(self):
        buffer_stream_key1 = self.event_data1['buffer_stream_key']
        self.window_controller.update_windows(self.event_data1)
        self.window_controller.update_windows(self.event_data2)
        self.window_controller.update_windows(self.event_data3)
        self.window_controller.update_windows(self.event_data1)
        self.assertListEqual(
            [self.event_data1],
            self.window_controller.bufferstream_to_window_map[buffer_stream_key1]
        )
        expected_finished_window = [self.event_data1, self.event_data2, self.event_data3]
        self.assertListEqual(
            expected_finished_window,
            self.window_controller.finished_bufferstream_to_window_map[buffer_stream_key1]
        )

    def test_get_and_reset_finished_bufferstream_windows_get_and_cleans_up_datastructure(self):
        self.window_controller.finished_bufferstream_to_window_map = {
            '123': [self.event_data1],
            '456': [self.event_data2, self.event_data3],
        }
        ret = self.window_controller.get_and_reset_finished_bufferstream_windows()
        self.assertListEqual([[self.event_data1], [self.event_data2, self.event_data3]], list(ret))
        self.assertEqual(self.window_controller.finished_bufferstream_to_window_map, {})
