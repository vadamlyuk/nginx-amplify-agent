# -*- coding: utf-8 -*-
import requests_mock
import copy

from copy import deepcopy
from hamcrest import *

from test.fixtures.defaults import DEFAULT_API_URL, DEFAULT_API_KEY
from amplify.agent.supervisor import Supervisor
from test.base import RealNginxTestCase
from amplify.agent.context import context

__author__ = "Mike Belov"
__copyright__ = "Copyright (C) Nginx, Inc. All rights reserved."
__credits__ = ["Mike Belov", "Andrei Belov", "Ivan Poluyanov", "Oleg Mamontov", "Andrew Alexeev"]
__license__ = ""
__maintainer__ = "Mike Belov"
__email__ = "dedm@nginx.com"


class SupervisorTestCase(RealNginxTestCase):

    def setup_method(self, method):
        super(SupervisorTestCase, self).setup_method(method)
        self.old_cloud_config = deepcopy(context.app_config.config)

    def teardown_method(self, method):
        context.app_config.config = self.old_cloud_config
        super(SupervisorTestCase, self).teardown_method(method)

    def test_talk_to_cloud(self):
        """
        Checks that we apply all changes from cloud to agent config and object configs
        :return:
        """
        supervisor = Supervisor()

        with requests_mock.mock() as m:
            m.post(
                '%s/%s/agent/' % (DEFAULT_API_URL, DEFAULT_API_KEY),
                text='{"config": {"cloud": {"push_interval": 60.0, "talk_interval": 60.0, "api_timeout": 5.0}, "containers": {"nginx": {"max_test_duration": 30.0, "run_test": false, "poll_intervals": {"metrics": 20.0, "configs": 20.0, "meta": 30.0, "discover": 10.0, "logs": 10.0}, "upload_ssl": true, "upload_config": true}, "system": {"poll_intervals": {"metrics": 20.0, "meta": 30.0, "discover": 10.0}}}}, "objects": [{"object":{"type":"nginx", "local_id": "151d8728e792f42e129337573a21bb30ab3065d59102f075efc2ded28e713ff8"}, "config":{"upload_ssl":true}}], "messages": [], "versions": {"current": 0.29, "old": 0.26, "obsolete": 0.21}}'
            )

            supervisor.init_containers()
            for container in supervisor.containers.itervalues():
                container._discover_objects()
            old_object_configs = copy.deepcopy(supervisor.containers['nginx'].object_configs)

            supervisor.talk_to_cloud()
            for container in supervisor.containers.itervalues():
                container._discover_objects()

            # check that agent config was changed
            assert_that(context.app_config.config, not_(equal_to(self.old_cloud_config)))

            # check that object configs were also changed
            nginx_container = supervisor.containers['nginx']
            assert_that(nginx_container.object_configs, not_(equal_to(old_object_configs)))
