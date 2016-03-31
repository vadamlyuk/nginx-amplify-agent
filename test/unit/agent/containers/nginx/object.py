# -*- coding: utf-8 -*-
import copy

from hamcrest import *

import amplify.agent.context

from test.base import RealNginxTestCase, nginx_plus_test
from amplify.agent.containers.nginx.container import NginxContainer

__author__ = "Mike Belov"
__copyright__ = "Copyright (C) Nginx, Inc. All rights reserved."
__credits__ = ["Mike Belov", "Andrei Belov", "Ivan Poluyanov", "Oleg Mamontov", "Andrew Alexeev"]
__license__ = ""
__maintainer__ = "Mike Belov"
__email__ = "dedm@nginx.com"


class ObjectTestCase(RealNginxTestCase):

    def setup_method(self, method):
        super(ObjectTestCase, self).setup_method(method)
        self.original_app_config = copy.deepcopy(amplify.agent.context.context.app_config.config)

    def teardown_method(self, method):
        amplify.agent.context.context.app_config.config = copy.deepcopy(self.original_app_config)
        super(ObjectTestCase, self).teardown_method(method)

    @nginx_plus_test
    def test_plus_status_discovery(self):
        """
        Checks that for plus nginx we collect two status urls:
        - one for web link (with server name)
        - one for agent purposes (local url)
        """
        container = NginxContainer()
        container.discover_objects()
        assert_that(container.objects, has_length(1))

        # get nginx object
        nginx_obj = container.objects.values().pop(0)

        # check all plus status urls
        assert_that(nginx_obj.plus_status_enabled, equal_to(True))
        assert_that(nginx_obj.plus_status_internal_url, equal_to('https://127.0.0.1:443/plus_status'))
        assert_that(nginx_obj.plus_status_external_url, equal_to('http://status.naas.nginx.com:443/plus_status_bad'))

    @nginx_plus_test
    def test_bad_plus_status_discovery(self):
        self.stop_first_nginx()
        self.start_second_nginx(conf='nginx_bad_status.conf')
        container = NginxContainer()
        container.discover_objects()

        assert_that(container.objects, has_length(1))

        # get nginx object
        nginx_obj = container.objects.values().pop(0)

        # check all plus status urls
        assert_that(nginx_obj.plus_status_enabled, equal_to(True))
        assert_that(nginx_obj.plus_status_internal_url, equal_to(None))
        assert_that(nginx_obj.plus_status_external_url, equal_to('http://bad.status.naas.nginx.com:82/plus_status'))

    @nginx_plus_test
    def test_bad_plus_status_discovery_with_config(self):
        amplify.agent.context.context.app_config['nginx']['plus_status'] = '/foo_plus'
        amplify.agent.context.context.app_config['nginx']['stub_status'] = '/foo_basic'

        self.stop_first_nginx()
        self.start_second_nginx(conf='nginx_bad_status.conf')
        container = NginxContainer()
        container.discover_objects()
        assert_that(container.objects, has_length(1))

        # self.http_request should look like this
        # [
        # first - internal plus statuses
        # 'http://127.0.0.1:82/plus_status', 'https://127.0.0.1:82/plus_status',
        # 'http://127.0.0.1/foo_plus', 'https://127.0.0.1/foo_plus',
        #
        # then external plus statuses
        # 'http://bad.status.naas.nginx.com:82/plus_status', 'https://bad.status.naas.nginx.com:82/plus_status',
        #
        # finally - stub statuses
        # 'http://127.0.0.1:82/basic_status', 'https://127.0.0.1:82/basic_status',
        # 'http://127.0.0.1/foo_basic', 'https://127.0.0.1/foo_basic'
        # ]

        assert_that(self.http_requests[2], equal_to('http://127.0.0.1/foo_plus'))
        assert_that(self.http_requests[-2], equal_to('http://127.0.0.1/foo_basic'))

    def test_bad_stub_status_discovery_with_config(self):
        amplify.agent.context.context.app_config['nginx']['stub_status'] = '/foo_basic'

        self.stop_first_nginx()
        self.start_second_nginx(conf='nginx_bad_status.conf')
        container = NginxContainer()
        container.discover_objects()
        assert_that(container.objects, has_length(1))

        assert_that(self.http_requests[-1], equal_to('https://127.0.0.1/foo_basic'))
        assert_that(self.http_requests[-2], equal_to('http://127.0.0.1/foo_basic'))
