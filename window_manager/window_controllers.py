
class TumblingCountWindowController(object):

    def __init__(self, query_id, *args):
        self.query_id = query_id
        self.args = args
        self.num_frames = self.args[0]
        self.bufferstream_to_window_map = {}
        self.finished_bufferstream_to_window_map = {}

    def __repr__(self):
        class_name = self.__class__.__name__
        text = f'{class_name}({self.query_id}, *{self.args})'
        return text

    def update_windows(self, event_data):
        buffer_stream_key = event_data['buffer_stream_key']
        window_list = self.bufferstream_to_window_map.setdefault(buffer_stream_key, [])
        window_list.append(event_data)
        if len(window_list) >= self.num_frames:
            self.finished_bufferstream_to_window_map[buffer_stream_key] = window_list
            self.bufferstream_to_window_map[buffer_stream_key] = []

    def get_and_reset_finished_bufferstream_windows(self):
        windows = self.finished_bufferstream_to_window_map.values()
        self.finished_bufferstream_to_window_map = {}
        return windows
