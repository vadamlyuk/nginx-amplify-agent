# -*- coding: utf-8 -*-
import time
import pprint
import gevent
import copy

from threading import current_thread

from amplify.agent.context import context
from amplify.agent.util import loader
from amplify.agent.bridge import Bridge
from amplify.agent.util.threads import spawn
from amplify.agent.errors import AmplifyCriticalException
from amplify.agent.cloud import CloudResponse

__author__ = "Mike Belov"
__copyright__ = "Copyright (C) Nginx, Inc. All rights reserved."
__credits__ = ["Mike Belov", "Andrei Belov", "Ivan Poluyanov", "Oleg Mamontov", "Andrew Alexeev", "Grant Hulegaard"]
__license__ = ""
__maintainer__ = "Mike Belov"
__email__ = "dedm@nginx.com"


class Supervisor(object):
    """
    Agent supervisor

    Starts dedicated threads for each data source
    """

    CONTAINER_CLASS = '%sContainer'
    CONTAINER_MODULE = 'amplify.agent.containers.%s.%s'

    def __init__(self, foreground=False):
        # daemon specific
        self.stdin_path = '/dev/null'

        if foreground:
            self.stdout_path = '/dev/stdout'
            self.stderr_path = '/dev/stderr'
        else:
            self.stdout_path = '/dev/null'
            self.stderr_path = '/dev/null'

        self.pidfile_path = context.app_config['daemon']['pid']
        self.pidfile_timeout = 1

        # init
        self.containers = {}
        self.bridge = None
        self.start_time = int(time.time())
        self.last_cloud_talk_time = 0
        self.is_running = True

    def init_containers(self):
        """
        Tries to load and create all objects containers specified in config
        """
        containers_from_local_config = context.app_config['containers']

        for container_name in containers_from_local_config.keys():
            try:
                container_classname = self.CONTAINER_CLASS % container_name.title()
                container_class = loader.import_class(self.CONTAINER_MODULE % (container_name, container_classname))

                # copy object configs
                if container_name in self.containers:
                    object_configs = copy.copy(self.containers[container_name].object_configs)
                else:
                    object_configs = None

                self.containers[container_name] = container_class(
                    object_configs=object_configs
                )
                context.log.debug('loaded container "%s" from %s' % (container_name, container_class))
            except:
                context.log.error('failed to load container %s' % container_name, exc_info=True)

    def run(self):
        # get correct pid
        context.set_pid()

        # set thread name
        current_thread().name = 'supervisor'

        # get initial config from cloud
        self.talk_to_cloud()

        # run containers
        self.init_containers()
        if not self.containers:
            context.log.error('no containers configured, stopping')
            return

        # run bridge thread
        self.bridge = spawn(Bridge().run)

        # main cycle
        while True:
            time.sleep(5.0)

            if not self.is_running:
                break

            try:
                context.inc_action_id()

                for container in self.containers.itervalues():
                    container._discover_objects()
                    container.run_objects()
                    container.schedule_cloud_commands()

                try:
                    self.talk_to_cloud(top_object=context.top_object.definition)
                except AmplifyCriticalException:
                    pass

                self.check_bridge()
            except OSError as e:
                if e.errno == 12:  # OSError errno 12 is a memory error (unable to allocate, out of memory, etc.)
                    context.log.error('OSError: [Errno %s] %s' % (e.errno, e.message), exc_info=True)
                    continue
                else:
                    raise e

    def stop(self):
        self.is_running = False

        bridge = Bridge()
        bridge.flush_all()

        for container in self.containers.itervalues():
            container.stop_objects()

    def talk_to_cloud(self, top_object=None):
        """
        Asks cloud for config, object configs, filters, etc
        Applies gathered data to objects and agent config

        :param top_object: {} definition dict of a top object
        """
        now = int(time.time())
        if now <= self.last_cloud_talk_time + context.app_config['cloud']['talk_interval']:
            return

        # talk to cloud
        try:
            cloud_response = CloudResponse(
                context.http_client.post('agent/', data=top_object)
            )
        except:
            context.log.error('could not connect to cloud', exc_info=True)
            raise AmplifyCriticalException()

        # check agent version status
        if context.version_major <= float(cloud_response.versions.obsolete):
            context.log.error(
                'agent is obsolete - cloud will refuse updates until it is updated (version: %s, current: %s)' %
                (context.version_major, cloud_response.versions.current)
            )
            self.stop()
        elif context.version_major <= float(cloud_response.versions.old):
            context.log.warn(
                'agent is old - update is recommended (version: %s, current: %s)' %
                (context.version_major, cloud_response.versions.current)
            )

        # update special object configs and filters
        changed_containers = set()
        for obj in cloud_response.objects:
            container = self.containers.get(obj.type)
            if not container:
                continue

            if container.object_configs.get(obj.id, {}) != obj.config:
                container.object_configs[obj.id] = obj.config
                changed_containers.add(obj.type)

        for obj_type in changed_containers:
            self.containers[obj_type].stop_objects()

        # global config changes
        config_changed = context.app_config.apply(cloud_response.config)
        if config_changed:
            context.http_client.update_cloud_url()
            context.cloud_restart = True
            if self.containers:
                context.log.info('config has changed. now running with: %s' % pprint.pformat(context.app_config.config))
                for container in self.containers.itervalues():
                    container.stop_objects()
                self.init_containers()

        context.cloud_restart = False
        self.last_cloud_talk_time = int(time.time())

    def check_bridge(self):
        """
        Check containers threads, restart if some failed
        """
        if self.bridge.ready and self.bridge.exception:
            context.log.debug('bridge exception: %s' % self.bridge.exception)
            self.bridge = gevent.spawn(Bridge().run)
