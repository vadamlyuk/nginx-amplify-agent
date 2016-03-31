# -*- coding: utf-8 -*-
import time
import gc

from threading import current_thread
from collections import deque

from amplify.agent.context import context
from amplify.agent import Singleton


__author__ = "Mike Belov"
__copyright__ = "Copyright (C) Nginx, Inc. All rights reserved."
__credits__ = ["Mike Belov", "Andrei Belov", "Ivan Poluyanov", "Oleg Mamontov", "Andrew Alexeev", "Grant Hulegaard"]
__license__ = ""
__maintainer__ = "Mike Belov"
__email__ = "dedm@nginx.com"


class Bridge(Singleton):
    def __init__(self):
        self.payload = {}
        self.first_run = True

        # Instantiate payload with appropriate keys and buckets.
        self._reset_payload()

    @staticmethod
    def look_around():
        """
        Checks everything around and make appropriate tree structure
        :return: dict of structure
        """
        # TODO check docker or OS around
        tree = {'system': ['nginx']}
        return tree

    def run(self):
        current_thread().name = 'bridge'
        context.setup_thread_id()

        try:
            while True:
                self.wait()
                context.inc_action_id()
                self.flush_all()
                gc.collect()
        except:
            context.default_log.error('failed', exc_info=True)
            raise

    def flush_all(self):
        containers = {
            'meta': self.flush_meta,
            'metrics': self.flush_metrics,
            'events': self.flush_events,
            'configs': self.flush_configs
        }

        # Flush data and add to appropriate payload bucket.
        if self.first_run:
            # If this is the first run, flush meta only to ensure object creation.
            self.payload['meta'].append(self.flush_meta())
            self.first_run = False
        else:
            for container_type in self.payload.keys():
                flush_data = containers[container_type].__call__()
                if flush_data:
                    self.payload[container_type].append(flush_data)

        context.log.debug(
            'modified payload; current payload stats: '
            'meta - %s, metrics - %s, events - %s, configs - %s' % (
                len(self.payload['meta']),
                len(self.payload['metrics']),
                len(self.payload['events']),
                len(self.payload['configs'])
            )
        )

        # Send payload to backend.
        try:
            self._pre_process_payload()  # Convert deques to lists for encoding
            context.http_client.post('update/', data=self.payload)
            context.default_log.debug(self.payload)
            self._reset_payload()  # Clear payload after successful send
        except Exception as e:
            exception_name = e.__class__.__name__
            context.log.error('failed to push data due to %s' % exception_name)
            context.log.debug('additional info:', exc_info=True)
            self._post_process_payload()  # Convert lists to deques since send failed

        context.log.debug('finished flush_all; new payload: %s' % self.payload)

    def flush_meta(self):
        return self._flush(container=context.metad)

    def flush_metrics(self):
        return self._flush(container=context.statsd)

    def flush_events(self):
        return self._flush(container=context.eventd)

    def flush_configs(self):
        return self._flush(container=context.configd)

    def _flush(self, container=None):
        # get structure
        objects_structure = self.look_around()

        # get root object
        # there is only one root object!
        root_type = objects_structure.keys().pop()
        root = container.flush(type=root_type).values()

        if not len(root):
            root = {'object': context.top_object.definition}
            root_only_for_structure = True
        else:
            root = root[0]
            root_only_for_structure = False

        # go through children (one level)
        root['children'] = []
        child_data_found = False
        for child_type in objects_structure[root_type]:
            for child_data in container.flush(type=child_type).itervalues():
                if child_data:
                    child_data_found = True
                    root['children'].append(child_data)

        if child_data_found or not root_only_for_structure:
            return root

    def _reset_payload(self):
        """
        After payload has been succesfully sent, clear the queues (reset them to empty deques).
        """
        self.payload = {
            'meta': deque(maxlen=360),
            'metrics': deque(maxlen=360),
            'events': deque(maxlen=360),
            'configs': deque(maxlen=360)
        }

    def _pre_process_payload(self):
        """
        ujson.encode does not handle deque objects well.  So before attempting a send, convert all the deques to lists.
        """
        for key in self.payload.keys():
            self.payload[key] = list(self.payload[key])

    def _post_process_payload(self):
        """
        If a payload is NOT reset (cannot be sent), then we should reconvert the lists to deques with maxlen to enforce
        memory management.
        """
        for key in self.payload.keys():
            self.payload[key] = deque(self.payload[key], maxlen=360)

    @staticmethod
    def wait():
        time.sleep(context.app_config['cloud']['push_interval'])
