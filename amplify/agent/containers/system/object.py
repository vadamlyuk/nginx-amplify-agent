# -*- coding: utf-8 -*-
from amplify.agent.util import host
from amplify.agent.context import context
from amplify.agent.containers.abstract import AbstractObject
from amplify.agent.containers.system.collectors.meta.common import SystemCommonMetaCollector
from amplify.agent.containers.system.collectors.meta.centos import SystemCentosMetaCollector
from amplify.agent.containers.system.collectors.metrics import SystemMetricsCollector
from amplify.agent.eventd import INFO


__author__ = "Mike Belov"
__copyright__ = "Copyright (C) Nginx, Inc. All rights reserved."
__credits__ = ["Mike Belov", "Andrei Belov", "Ivan Poluyanov", "Oleg Mamontov", "Andrew Alexeev", "Grant Hulegaard"]
__license__ = ""
__maintainer__ = "Mike Belov"
__email__ = "dedm@nginx.com"


class SystemObject(AbstractObject):
    type = 'system'

    def __init__(self, **kwargs):
        super(SystemObject, self).__init__(**kwargs)

        self.uuid = self.data['uuid']
        self.hostname = self.data['hostname']

        meta_collector_class = SystemCommonMetaCollector
        if host.os_name() == 'linux' and host.linux_name() in ('centos',):
            meta_collector_class = SystemCentosMetaCollector

        self.collectors = [
            meta_collector_class(object=self, interval=self.intervals['meta']),
            SystemMetricsCollector(object=self, interval=self.intervals['metrics'])
        ]

    def start(self):
        if not self.running and not context.cloud_restart:
            # Fire agent started event.
            self.eventd.event(
                level=INFO,
                message='agent started, version: %s, pid: %s' % (context.version, context.pid),
                ctime=context.start_time-1  # Make sure that the start event is the first event reported.
            )

            # log agent started event
            context.log.info(
                'agent started, version=%s pid=%s uuid=%s hostname=%s' %
                (context.version, context.pid, self.uuid, self.hostname)
            )

            # log main host params
            context.log.info('')

        super(SystemObject, self).start()

    def stop(self, *args, **kwargs):
        if not context.cloud_restart:
            # Fire agent stopped event.
            self.eventd.event(
                level=INFO,
                message='agent stopped, version: %s, pid: %s' % (context.version, context.pid)
            )

            # log agent stopped event
            context.log.info(
                'agent stopped, version=%s pid=%s uuid=%s hostname=%s' %
                (context.version, context.pid, self.uuid, self.hostname)
            )

        super(SystemObject, self).stop(*args, **kwargs)
