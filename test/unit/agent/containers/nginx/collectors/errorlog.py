# -*- coding: utf-8 -*-
from hamcrest import *

from amplify.agent.containers.nginx.collectors.errorlog import NginxErrorLogsCollector
from test.base import NginxCollectorTestCase


__author__ = "Mike Belov"
__copyright__ = "Copyright (C) Nginx, Inc. All rights reserved."
__credits__ = ["Mike Belov", "Andrei Belov", "Ivan Poluyanov", "Oleg Mamontov", "Andrew Alexeev", "Grant Hulegaard"]
__license__ = ""
__maintainer__ = "Mike Belov"
__email__ = "dedm@nginx.com"


class LogsOverallTestCase(NginxCollectorTestCase):

    def test_multiline(self):
        lines = [
            '2015/07/14 08:42:57 [error] 28386#28386: *38698 upstream timed out ' +
            '(110: Connection timed out) while reading response header from upstream, ' +
            'client: 127.0.0.1, server: localhost, request: "GET /1.0/ HTTP/1.0", ' +
            'upstream: "uwsgi://127.0.0.1:3131", host: "localhost:5000"',

            '2015/07/15 05:56:33 [warn] 28386#28386: *94149 an upstream response is buffered ' +
            'to a temporary file /var/cache/nginx/proxy_temp/4/08/0000000084 while reading upstream, ' +
            'client: 85.141.232.177, server: *.compute.amazonaws.com, request: ' +
            '"POST /api/metrics/query/timeseries/ HTTP/1.1", upstream: ' +
            '"http://127.0.0.1:3000/api/metrics/query/timeseries/", host: ' +
            '"ec2-54-78-3-178.eu-west-1.compute.amazonaws.com:4000", referrer: ' +
            '"http://ec2-54-78-3-178.eu-west-1.compute.amazonaws.com:4000/"',

            '2015/07/14 08:42:57 [error] 28386#28386: *38698 upstream timed out ' +
            '(110: Connection timed out) while reading response header from upstream, ' +
            'client: 127.0.0.1, server: localhost, request: "GET /1.0/ HTTP/1.0", upstream: ' +
            '"uwsgi://127.0.0.1:3131", host: "localhost:5000"'
        ]

        collector = NginxErrorLogsCollector(level='warn', object=self.fake_object, tail=lines)
        collector.collect()

        # check
        metrics = self.fake_object.statsd.flush()['metrics']
        assert_that(metrics, has_item('counter'))

        # counters
        counter = metrics['counter']
        for key in ('C|nginx.upstream.response.failed', 'C|nginx.upstream.response.buffered'):
            assert_that(counter, has_key(key))

        # values
        assert_that(counter['C|nginx.upstream.response.failed'][0][1], equal_to(2))
        assert_that(counter['C|nginx.upstream.response.buffered'][0][1], equal_to(1))

        # check zero values
        for error_counter in collector.counters:
            assert_that(counter, has_key('C|%s' % error_counter))
