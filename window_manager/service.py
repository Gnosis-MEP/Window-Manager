import threading

from event_service_utils.logging.decorators import timer_logger
from event_service_utils.services.event_driven import BaseEventDrivenCMDService
from event_service_utils.tracing.jaeger import init_tracer
from window_manager.window_controllers import TumblingCountWindowController


class WindowManager(BaseEventDrivenCMDService):
    def __init__(self,
                 service_stream_key, service_cmd_key_list,
                 pub_event_list, service_details,
                 matcher_stream_key,
                 stream_factory,
                 logging_level,
                 tracer_configs):
        tracer = init_tracer(self.__class__.__name__, **tracer_configs)
        super(WindowManager, self).__init__(
            name=self.__class__.__name__,
            service_stream_key=service_stream_key,
            service_cmd_key_list=service_cmd_key_list,
            pub_event_list=pub_event_list,
            service_details=service_details,
            stream_factory=stream_factory,
            logging_level=logging_level,
            tracer=tracer,
        )
        self.cmd_validation_fields = ['id']
        self.data_validation_fields = ['id']
        self.matcher_stream_key = matcher_stream_key
        self.matcher_stream = self.stream_factory.create(key=matcher_stream_key, stype='streamOnly')

        self.window_controllers = {
            'TUMBLING_COUNT_WINDOW': TumblingCountWindowController,
        }

        self.query_windows = {}

    def add_event_to_query_windows(self, event_data):
        for query_id in event_data['query_ids']:
            self.query_windows[query_id].update_windows(event_data)

    def send_finished_windows(self):
        for query_id, window_controler in self.query_windows.items():
            finished_windows = window_controler.get_and_reset_finished_bufferstream_windows()
            for window in finished_windows:
                self.send_window_to_matcher(query_id, window)

    def send_window_to_matcher(self, query_id, window):
        new_event_data = {
            'id': self.service_based_random_event_id(),
            'vekg_stream': window,
            'query_id': query_id,
        }
        self.logger.debug(f'Sending window to Matcher: {new_event_data}')
        self.write_event_with_trace(new_event_data, self.matcher_stream)

    @timer_logger
    def process_data_event(self, event_data, json_msg):
        if not super(WindowManager, self).process_data_event(event_data, json_msg):
            return False
        self.add_event_to_query_windows(event_data)
        self.send_finished_windows()

    def add_query_window_action(self, query_id, window):
        window_type = window['window_type'].upper()
        if window_type not in self.window_controllers.keys():
            self.logger.error(
                (
                    f'Window type "{window_type}" not present in the list of supported windows: '
                    f'"{list(self.window_controllers.keys())}".'
                    f'Will ignore this window for query id: "{query_id}".'
                )
            )
            return
        if query_id in self.query_windows.keys():
            self.logger.error(
                (
                    f'Query ID already has a window controller attached to it.'
                    f'Will ignore this as a dupplicated event for query id: "{query_id}".'
                )
            )
            return

        window_controller_class = self.window_controllers[window_type]
        window_controller_args = window['args']
        self.query_windows[query_id] = window_controller_class(query_id, *window_controller_args)

    def process_event_type(self, event_type, event_data, json_msg):
        if not super(WindowManager, self).process_event_type(event_type, event_data, json_msg):
            return False
        if event_type == 'QueryCreated':
            parsed_query = event_data['parsed_query']
            query_id = event_data['query_id']
            window = parsed_query['window']
            self.add_query_window_action(query_id=query_id, window=window)

    def log_state(self):
        super(WindowManager, self).log_state()
        self._log_dict('Query Windows', self.query_windows)

    def run(self):
        super(WindowManager, self).run()
        self.cmd_thread = threading.Thread(target=self.run_forever, args=(self.process_cmd,))
        self.data_thread = threading.Thread(target=self.run_forever, args=(self.process_data,))
        self.cmd_thread.start()
        self.data_thread.start()
        self.cmd_thread.join()
        self.data_thread.join()
