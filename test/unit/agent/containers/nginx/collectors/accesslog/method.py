# -*- coding: utf-8 -*-
from collections import defaultdict

from hamcrest import *

from amplify.agent.containers.nginx.collectors.accesslog import NginxAccessLogsCollector
from amplify.agent.nginx.log.access import NginxAccessLogParser
from test.base import NginxCollectorTestCase

__author__ = "Mike Belov"
__copyright__ = "Copyright (C) Nginx, Inc. All rights reserved."
__credits__ = ["Mike Belov", "Andrei Belov", "Ivan Poluyanov", "Oleg Mamontov", "Andrew Alexeev"]
__license__ = ""
__maintainer__ = "Mike Belov"
__email__ = "dedm@nginx.com"


class LogsPerMethodTestCase(NginxCollectorTestCase):

    def test_http_method(self):
        line = '127.0.0.1 - - [02/Jul/2015:14:49:48 +0000] "GET /basic_status HTTP/1.1" 200 110 "-" ' + \
               '"python-requests/2.2.1 CPython/2.7.6 Linux/3.13.0-48-generic"'

        # run single method
        collector = NginxAccessLogsCollector(object=self.fake_object, tail=[])
        collector.http_method(NginxAccessLogParser().parse(line))

        # check
        metrics = self.fake_object.statsd.current
        assert_that(metrics, has_item('counter'))
        counters = metrics['counter']
        assert_that(counters, has_item('nginx.http.method.get'))
        assert_that(counters['nginx.http.method.get'][0][1], equal_to(1))

    def test_non_standard_http_method(self):
        line = '127.0.0.1 - - [02/Jul/2015:14:49:48 +0000] "PROPFIND /basic_status HTTP/1.1" 200 110 "-" ' + \
               '"python-requests/2.2.1 CPython/2.7.6 Linux/3.13.0-48-generic"'

        # run single method
        collector = NginxAccessLogsCollector(object=self.fake_object, tail=[])
        collector.http_method(NginxAccessLogParser().parse(line))

        # check
        metrics = self.fake_object.statsd.current
        assert_that(metrics, has_item('counter'))
        counters = metrics['counter']
        assert_that(counters, has_item('nginx.http.method.other'))
        assert_that(counters['nginx.http.method.other'][0][1], equal_to(1))


    def test_http_status(self):
        line = '127.0.0.1 - - [02/Jul/2015:14:49:48 +0000] "GET /basic_status HTTP/1.1" 200 110 "-" ' + \
               '"python-requests/2.2.1 CPython/2.7.6 Linux/3.13.0-48-generic"'

        # run single method
        collector = NginxAccessLogsCollector(object=self.fake_object, tail=[])
        collector.http_status(NginxAccessLogParser().parse(line))

        # check
        metrics = self.fake_object.statsd.current
        assert_that(metrics, has_item('counter'))
        counters = metrics['counter']
        assert_that(counters, has_item('nginx.http.status.2xx'))
        assert_that(counters['nginx.http.status.2xx'][0][1], equal_to(1))

    def test_upstreams(self):
        log_format = '$remote_addr - $remote_user [$time_local] ' + \
                     '"$request" $status $body_bytes_sent "$http_referer" "$http_user_agent" ' + \
                     'rt=$request_time ut="$upstream_response_time" cs=$upstream_cache_status'

        line = \
            '1.2.3.4 - - [22/Jan/2010:19:34:21 +0300] "GET /foo/ HTTP/1.1" 200 11078 ' + \
            '"http://www.rambler.ru/" "Mozilla/5.0 (Windows; U; Windows NT 5.1" rt=0.010 ut="2.001, 0.345" cs=MISS'

        # run single method
        collector = NginxAccessLogsCollector(object=self.fake_object, tail=[])
        collector.upstreams(NginxAccessLogParser(log_format).parse(line))

        # check
        metrics = self.fake_object.statsd.current
        assert_that(metrics, has_item('counter'))
        assert_that(metrics, has_item('timer'))

        # counters
        counters = metrics['counter']
        assert_that(counters, has_item('nginx.upstream.request.count'))
        assert_that(counters, has_item('nginx.upstream.next.count'))
        assert_that(counters, has_item('nginx.cache.miss'))
        assert_that(counters['nginx.upstream.request.count'][0][1], equal_to(1))
        assert_that(counters['nginx.upstream.next.count'][0][1], equal_to(1))
        assert_that(counters['nginx.cache.miss'][0][1], equal_to(1))

        # histogram
        histogram = metrics['timer']
        assert_that(histogram, has_item('nginx.upstream.response.time'))
        assert_that(histogram['nginx.upstream.response.time'], equal_to([2.001 + 0.345]))

    def test_empty_upstreams(self):
        log_format = '$remote_addr - $remote_user [$time_local] ' + \
                     '"$request" $status $body_bytes_sent "$http_referer" "$http_user_agent" ' + \
                     'rt=$request_time cs=$upstream_cache_status ut="$upstream_response_time"'

        line = \
            '1.2.3.4 - - [22/Jan/2010:19:34:21 +0300] "GET /foo/ HTTP/1.1" 200 11078 ' + \
            '"http://www.rambler.ru/" "Mozilla/5.0 (Windows; U; Windows NT 5.1" rt=0.010 cs=- ut="-"'

        # run single method
        collector = NginxAccessLogsCollector(object=self.fake_object, tail=[])
        collector.upstreams(NginxAccessLogParser(log_format).parse(line))

        # check
        metrics = self.fake_object.statsd.current
        assert_that(metrics, equal_to(defaultdict()))

        # counters
        counters = metrics['counter']
        assert_that(counters, equal_to({}))

        # histogram
        histogram = metrics['timer']
        assert_that(histogram, equal_to({}))

    def test_part_empty_upstreams(self):
        log_format = '$remote_addr - $remote_user [$time_local] ' + \
                     '"$request" $status $body_bytes_sent "$http_referer" "$http_user_agent" ' + \
                     'rt=$request_time ut="$upstream_response_time" cs=$upstream_cache_status'

        line = \
            '1.2.3.4 - - [22/Jan/2010:19:34:21 +0300] "GET /foo/ HTTP/1.1" 200 11078 ' + \
            '"http://www.rambler.ru/" "Mozilla/5.0 (Windows; U; Windows NT 5.1" rt=0.010 ut="-" cs=MISS'

        # run single method
        collector = NginxAccessLogsCollector(object=self.fake_object, tail=[])
        collector.upstreams(NginxAccessLogParser(log_format).parse(line))

        # check
        metrics = self.fake_object.statsd.current
        assert_that(metrics, has_item('counter'))

        # counters
        counters = metrics['counter']
        assert_that(counters, has_item('nginx.upstream.request.count'))
        assert_that(counters, has_item('nginx.upstream.next.count'))
        assert_that(counters, has_item('nginx.cache.miss'))
        assert_that(counters['nginx.upstream.request.count'][0][1], equal_to(1))
        assert_that(counters['nginx.upstream.next.count'][0][1], equal_to(0))
        assert_that(counters['nginx.cache.miss'][0][1], equal_to(1))

        # histogram
        histogram = metrics['timer']
        assert_that(histogram, equal_to({}))

    def test_part_empty_upstreams2(self):
        log_format = '$remote_addr - $remote_user [$time_local] ' + \
                     '"$request" $status $body_bytes_sent "$http_referer" "$http_user_agent" ' + \
                     'rt=$request_time ut="$upstream_response_time" cs=$upstream_cache_status'

        line = \
            '1.2.3.4 - - [22/Jan/2010:19:34:21 +0300] "GET /foo/ HTTP/1.1" 200 11078 ' + \
            '"http://www.rambler.ru/" "Mozilla/5.0 (Windows; U; Windows NT 5.1" rt=0.010 ut="2.001, 0.345" cs=-'

        # run single method
        collector = NginxAccessLogsCollector(object=self.fake_object, tail=[])
        collector.upstreams(NginxAccessLogParser(log_format).parse(line))

        # check
        metrics = self.fake_object.statsd.current
        assert_that(metrics, has_item('counter'))
        assert_that(metrics, has_item('timer'))

        # counters
        counters = metrics['counter']
        assert_that(counters, has_item('nginx.upstream.request.count'))
        assert_that(counters, has_item('nginx.upstream.next.count'))
        assert_that(counters, not has_item('nginx.cache.miss'))
        assert_that(counters['nginx.upstream.request.count'][0][1], equal_to(1))
        assert_that(counters['nginx.upstream.next.count'][0][1], equal_to(1))

        # histogram
        histogram = metrics['timer']
        assert_that(histogram, has_item('nginx.upstream.response.time'))
        assert_that(histogram['nginx.upstream.response.time'], equal_to([2.001 + 0.345]))

    def test_upstream_status_and_length(self):
        log_format = '$remote_addr - $remote_user [$time_local] ' + \
                     '"$request" $status $body_bytes_sent "$http_referer" "$http_user_agent" ' + \
                     'rt=$request_time ut="$upstream_response_time" cs=$upstream_cache_status ' + \
                     'us=$upstream_status $upstream_response_length'

        line = \
            '1.2.3.4 - - [22/Jan/2010:19:34:21 +0300] "GET /foo/ HTTP/1.1" 200 11078 ' + \
            '"http://www.rambler.ru/" "Mozilla/5.0 (Windows; U; Windows NT 5.1" rt=0.010 ut="2.001, 0.345" cs=MISS ' + \
            'us=200 20'

        # run single method
        collector = NginxAccessLogsCollector(object=self.fake_object, tail=[])
        collector.upstreams(NginxAccessLogParser(log_format).parse(line))

        # check
        metrics = self.fake_object.statsd.current
        assert_that(metrics, has_item('counter'))
        assert_that(metrics, has_item('timer'))

        # counters
        counters = metrics['counter']
        assert_that(counters, has_item('nginx.upstream.request.count'))
        assert_that(counters, has_item('nginx.upstream.next.count'))
        assert_that(counters, has_item('nginx.cache.miss'))
        assert_that(counters, has_item('nginx.upstream.http.status.2xx'))
        assert_that(counters, has_item('nginx.upstream.http.response.length'))
        assert_that(counters['nginx.upstream.request.count'][0][1], equal_to(1))
        assert_that(counters['nginx.upstream.next.count'][0][1], equal_to(1))
        assert_that(counters['nginx.upstream.http.status.2xx'][0][1], equal_to(1))
        assert_that(counters['nginx.upstream.http.response.length'][0][1], equal_to(20))

        # histogram
        histogram = metrics['timer']
        assert_that(histogram, has_item('nginx.upstream.response.time'))
        assert_that(histogram['nginx.upstream.response.time'], equal_to([2.001 + 0.345]))

    def test_upstream_status_and_length2(self):
        """
        Test 3XX status for response length as well.
        """
        log_format = '$remote_addr - $remote_user [$time_local] ' + \
                     '"$request" $status $body_bytes_sent "$http_referer" "$http_user_agent" ' + \
                     'rt=$request_time ut="$upstream_response_time" cs=$upstream_cache_status ' + \
                     'us=$upstream_status $upstream_response_length'

        line = \
            '1.2.3.4 - - [22/Jan/2010:19:34:21 +0300] "GET /foo/ HTTP/1.1" 200 11078 ' + \
            '"http://www.rambler.ru/" "Mozilla/5.0 (Windows; U; Windows NT 5.1" rt=0.010 ut="2.001, 0.345" cs=MISS ' + \
            'us=300 40'

        # run single method
        collector = NginxAccessLogsCollector(object=self.fake_object, tail=[])
        collector.upstreams(NginxAccessLogParser(log_format).parse(line))

        # check
        metrics = self.fake_object.statsd.current
        assert_that(metrics, has_item('counter'))
        assert_that(metrics, has_item('timer'))

        # counters
        counters = metrics['counter']
        assert_that(counters, has_item('nginx.upstream.request.count'))
        assert_that(counters, has_item('nginx.upstream.next.count'))
        assert_that(counters, has_item('nginx.cache.miss'))
        assert_that(counters, has_item('nginx.upstream.http.status.3xx'))
        assert_that(counters, has_item('nginx.upstream.http.response.length'))
        assert_that(counters['nginx.upstream.request.count'][0][1], equal_to(1))
        assert_that(counters['nginx.upstream.next.count'][0][1], equal_to(1))
        assert_that(counters['nginx.upstream.http.status.3xx'][0][1], equal_to(1))
        assert_that(counters['nginx.upstream.http.response.length'][0][1], equal_to(40))

        # histogram
        histogram = metrics['timer']
        assert_that(histogram, has_item('nginx.upstream.response.time'))
        assert_that(histogram['nginx.upstream.response.time'], equal_to([2.001 + 0.345]))

