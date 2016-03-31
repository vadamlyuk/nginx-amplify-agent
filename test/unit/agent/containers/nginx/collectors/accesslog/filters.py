# -*- coding: utf-8 -*-
import copy

from hamcrest import *

from amplify.agent.cloud import Filter
from test.base import NginxCollectorTestCase
from amplify.agent.containers.nginx.collectors.accesslog import NginxAccessLogsCollector

__author__ = "Mike Belov"
__copyright__ = "Copyright (C) Nginx, Inc. All rights reserved."
__credits__ = ["Mike Belov", "Andrei Belov", "Ivan Poluyanov", "Oleg Mamontov", "Andrew Alexeev"]
__license__ = ""
__maintainer__ = "Mike Belov"
__email__ = "dedm@nginx.com"


class LogsFiltersTestCase(NginxCollectorTestCase):

    lines = [
            '178.23.225.78 - - [18/Jun/2015:17:22:25 +0000] "GET /img/docker.png HTTP/1.1" 200 0 ' +
            '"http://ec2-54-78-3-178.eu-west-1.compute.amazonaws.com:4000/" ' +
            '"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_4) AppleWebKit/537.36 (KHTML, like Gecko) ' +
            'Chrome/43.0.2357.124 Safari/537.36"',

            '178.23.225.78 - - [18/Jun/2015:17:22:25 +0000] "GET /img/docker.png HTTP/1.2" 304 0 ' +
            '"http://ec2-54-78-3-178.eu-west-1.compute.amazonaws.com:4000/" ' +
            '"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_4) AppleWebKit/537.36 (KHTML, like Gecko) ' +
            'Chrome/43.0.2357.124 Safari/537.36"',

            '178.23.225.78 - - [18/Jun/2015:17:22:25 +0000] "POST /img/super/docker.png HTTP/1.2" 304 2 ' +
            '"http://ec2-54-78-3-178.eu-west-1.compute.amazonaws.com:5000/" ' +
            '"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_4) AppleWebKit/537.36 (KHTML, like Gecko) ' +
            'Chrome/43.0.2357.124 Safari/537.36"',

            '178.23.225.78 - - [18/Jun/2015:17:22:25 +0000] "GET /api/inventory/objects/ HTTP/1.1" 200 1093 ' +
            '"http://ec2-54-78-3-178.eu-west-1.compute.amazonaws.com:4000/" ' +
            '"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_4) AppleWebKit/537.36 (KHTML, like Gecko) ' +
            'Chrome/43.0.2357.124 Safari/537.36"',

            '127.0.0.1 - - [18/Jun/2015:17:22:33 +0000] "POST /1.0/589fjinijenfirjf/meta/ HTTP/1.1" ' +
            '202 2 "-" "python-requests/2.2.1 CPython/2.7.6 Linux/3.13.0-48-generic"',

            '52.6.158.18 - - [18/Jun/2015:17:22:40 +0000] "GET /#/objects HTTP/2.1" 416 84 ' +
            '"-" "Slackbot-LinkExpanding 1.0 (+https://api.slack.com/robots)"'
        ]

    def setup_method(self, method):
        super(LogsFiltersTestCase, self).setup_method(method)
        self.original_fake_object = copy.copy(self.fake_object)

    def teardown_method(self, method):
        self.fake_object = self.original_fake_object
        super(LogsFiltersTestCase, self).teardown_method(method)

    def test_simple_filter(self):
        self.fake_object.filters = [
            Filter(**dict(
                filter_rule_id=1,
                metric='nginx.http.status.2xx',
                data=[
                    {'$request_method': 'GET'},
                    {'$status': '200'}
                ]
            )),
            Filter(**dict(
                filter_rule_id=2,
                metric='nginx.http.request.body_bytes_sent',
                data=[
                    {'$request_uri': '/img*'},
                    {'$server_protocol': 'HTTP/1.2'}
                ]
            ))
        ]

        collector = NginxAccessLogsCollector(object=self.fake_object, tail=self.lines)
        collector.collect()

        # check
        metrics = self.fake_object.statsd.flush()['metrics']
        assert_that(metrics, has_item('counter'))

        # counters
        counter = metrics['counter']
        for key in ('C|nginx.http.method.get', 'C|nginx.http.request.body_bytes_sent', 'C|nginx.http.status.3xx',
                    'C|nginx.http.status.2xx', 'C|nginx.http.method.post', 'C|nginx.http.v1_1',
                    'C|nginx.http.status.4xx', 'C|nginx.http.status.2xx||1', 'C|nginx.http.method.post'):
            assert_that(counter, has_key(key))

        # values
        assert_that(counter['C|nginx.http.method.get'][0][1], equal_to(4))
        assert_that(counter['C|nginx.http.method.post'][0][1], equal_to(2))
        assert_that(counter['C|nginx.http.status.2xx'][0][1], equal_to(3))

        # filter values
        assert_that(counter['C|nginx.http.status.2xx||1'][0][1], equal_to(2))

    def test_regex_filter(self):
        self.fake_object.filters = [
            Filter(**dict(
                filter_rule_id=2,
                metric='nginx.http.request.body_bytes_sent',
                data=[
                    {'$request_uri': '/img*'},
                    {'$server_protocol': 'HTTP/1.2'}
                ]
            ))
        ]

        collector = NginxAccessLogsCollector(object=self.fake_object, tail=self.lines)
        collector.collect()

        # check
        metrics = self.fake_object.statsd.flush()['metrics']
        assert_that(metrics, has_item('counter'))

        # counters
        counter = metrics['counter']
        for key in ('C|nginx.http.method.get', 'C|nginx.http.request.body_bytes_sent', 'C|nginx.http.status.3xx',
                    'C|nginx.http.status.2xx', 'C|nginx.http.method.post', 'C|nginx.http.v1_1',
                    'C|nginx.http.status.4xx', 'C|nginx.http.request.body_bytes_sent||2', 'C|nginx.http.method.post'):
            assert_that(counter, has_key(key))

        # values
        assert_that(counter['C|nginx.http.method.get'][0][1], equal_to(4))
        assert_that(counter['C|nginx.http.method.post'][0][1], equal_to(2))
        assert_that(counter['C|nginx.http.status.2xx'][0][1], equal_to(3))

        # filter values
        assert_that(counter['C|nginx.http.request.body_bytes_sent||2'][0][1], equal_to(2))
