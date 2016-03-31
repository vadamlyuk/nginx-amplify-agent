# -*- coding: utf-8 -*-
from hamcrest import *

from amplify.agent.nginx.log.error import NginxErrorLogParser
from test.base import BaseTestCase

__author__ = "Mike Belov"
__copyright__ = "Copyright (C) Nginx, Inc. All rights reserved."
__credits__ = ["Mike Belov", "Andrei Belov", "Ivan Poluyanov", "Oleg Mamontov", "Andrew Alexeev"]
__license__ = ""
__maintainer__ = "Mike Belov"
__email__ = "dedm@nginx.com"


class LogParserTestCase(BaseTestCase):
    def test_upstream_response_timed_out(self):
        line = '2015/07/14 08:42:57 [error] 28386#28386: *38698 upstream timed out ' + \
               '(110: Connection timed out) while reading response header from upstream, ' + \
               'client: 127.0.0.1, server: localhost, request: "GET /1.0/ HTTP/1.0", ' + \
               'upstream: "uwsgi://127.0.0.1:3131", host: "localhost:5000"'

        parser = NginxErrorLogParser()
        parsed = parser.parse(line)
        assert_that(parsed, equal_to('nginx.upstream.response.failed'))

    def test_upstream_response_buffered(self):
        line = '2015/07/15 05:56:33 [warn] 28386#28386: *94149 an upstream response is buffered ' + \
               'to a temporary file /var/cache/nginx/proxy_temp/4/08/0000000084 while reading upstream, ' + \
               'client: 85.141.232.177, server: *.compute.amazonaws.com, request: ' + \
               '"POST /api/metrics/query/timeseries/ HTTP/1.1", upstream: ' + \
               '"http://127.0.0.1:3000/api/metrics/query/timeseries/", host: ' + \
               '"ec2-54-78-3-178.eu-west-1.compute.amazonaws.com:4000", referrer: ' + \
               '"http://ec2-54-78-3-178.eu-west-1.compute.amazonaws.com:4000/"'

        parser = NginxErrorLogParser()
        parsed = parser.parse(line)
        assert_that(parsed, equal_to('nginx.upstream.response.buffered'))

    def test_none_found(self):
        line = '2015/07/15 05:56:30 [info] 28386#28386: *94160 client 10.196.158.41 closed keepalive connection'
        parser = NginxErrorLogParser()
        parsed = parser.parse(line)
        assert_that(parsed, equal_to(None))
