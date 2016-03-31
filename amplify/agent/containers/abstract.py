# -*- coding: utf-8 -*-
import time
import hashlib
import abc

from collections import defaultdict
from threading import current_thread
from gevent import queue

from amplify.agent.context import context
from amplify.agent.statsd import StatsdClient
from amplify.agent.eventd import EventdClient
from amplify.agent.metad import MetadClient
from amplify.agent.configd import ConfigdClient
from amplify.agent.util.threads import spawn

__author__ = "Mike Belov"
__copyright__ = "Copyright (C) Nginx, Inc. All rights reserved."
__credits__ = ["Mike Belov", "Andrei Belov", "Ivan Poluyanov", "Oleg Mamontov", "Andrew Alexeev"]
__license__ = ""
__maintainer__ = "Mike Belov"
__email__ = "dedm@nginx.com"


def definition_id(definition):
    """
    Returns object id based on its definition
    :param definition: dict with object definition
    :return: md5 based on it
    """
    definition_string = str(map(lambda x: u'%s:%s' % (x, definition[x]), sorted(definition.keys())))
    result = hashlib.md5(definition_string).hexdigest()
    return result


class AbstractContainer(object):
    type = None

    def __init__(self, object_configs=None):
        self.objects = {}
        self.object_configs = object_configs if object_configs else {}
        self.intervals = context.app_config['containers'][self.type]['poll_intervals']
        self.last_discover = 0

    def schedule_cloud_commands(self):
        """
        Reads global cloud command queue and applies commands to specific objects
        """
        pass

    def _discover_objects(self):
        """
        Wrapper for _discover_objects - runs discovering with period
        """
        if time.time() > self.last_discover + self.intervals['discover']:
            self.discover_objects()
        context.log.debug('%s objects: %s' % (self.type, self.objects.keys()))

    def discover_objects(self):
        """
        Abstract discovering method
        Should be overrided in subclasses and set self.objects = {obj_id: obj_instance}
        """
        pass

    def stop_objects(self):
        """
        Quietly stops all container objects
        """
        for obj in self.objects.itervalues():
            obj.stop(unregister=False)
        self.objects = {}

    def run_objects(self):
        """
        Starts all objects
        """
        for obj in self.objects.itervalues():
            obj.start()

    def sleep(self):
        time.sleep(self.intervals['discover'])


class AbstractObject(object):
    """
    Abstract object. Supervisor for collectors.
    """
    type = None

    def __init__(self, definition=None, data=None):
        self.definition = {} if definition is None else definition
        self.definition['type'] = self.type
        self.id = definition_id(self.definition)
        self.data = data
        self.intervals = context.app_config['containers'][self.type]['poll_intervals'] or {'default': 10}
        self.running = False
        self.need_restart = False

        self.threads = []
        self.collectors = []
        self.filters = []
        self.queue = queue.Queue()

        # data clients
        self.statsd = StatsdClient(object=self, interval=max(self.intervals.values()))
        self.eventd = EventdClient(object=self)
        self.metad = MetadClient(object=self)
        self.configd = ConfigdClient(object=self)

        # register data clients
        context.eventd.register(self.type, self.id, self.eventd)
        context.statsd.register(self.type, self.id, self.statsd)
        context.metad.register(self.type, self.id, self.metad)
        context.configd.register(self.type, self.id, self.configd)

    def start(self):
        """
        Starts all of the object's collector threads
        """
        if not self.running:
            context.log.debug('starting object %s' % self.id)
            for collector in self.collectors:
                self.threads.append(spawn(collector.run))
            self.running = True

    def stop(self, unregister=True):
        context.log.debug('halting object %s' % self.id)
        self.running = False
        if unregister:
            for container in (context.statsd, context.metad, context.eventd, context.configd):
                container.unregister(self.type, self.id)
        context.log.debug('object %s stopped' % self.id)


class AbstractCollector(object):
    """
    Abstract data collector
    Runs in a thread and collects specific data
    """
    short_name = None

    def __init__(self, object=None, interval=None):
        self.object = object
        self.interval = interval
        self.statsd = object.statsd
        self.metad = object.metad
        self.eventd = object.eventd
        self.configd = object.configd
        self.previous_values = defaultdict(dict)  # for deltas

    def run(self):
        """
        Common collector cycle

        1. Collect data
        2. Sleep
        3. Stop if object stopped
        """
        current_thread().name = self.short_name
        context.setup_thread_id()

        try:
            while True:
                context.inc_action_id()
                if self.object.running:
                    self._collect()
                    self._sleep()
                else:
                    break
        except:
            context.log.error('%s failed' % self.object.id, exc_info=True)
            raise

    def _collect(self):
        start_time = time.time()
        try:
            self.collect()
        except:
            raise
        finally:
            end_time = time.time()
            context.log.debug('%s collect in %.3f' % (self.object.id, end_time - start_time))

    def _sleep(self):
        time.sleep(self.interval)

    @abc.abstractmethod
    def collect(self):
        """
        Real collect method
        Override it
        """
        pass
