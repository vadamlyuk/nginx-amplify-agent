# -*- coding: utf-8 -*-
from hamcrest import *

from test.base import NginxCollectorTestCase
from amplify.agent.containers.nginx.collectors.accesslog import NginxAccessLogsCollector

__author__ = "Mike Belov"
__copyright__ = "Copyright (C) Nginx, Inc. All rights reserved."
__credits__ = ["Mike Belov", "Andrei Belov", "Ivan Poluyanov", "Oleg Mamontov", "Andrew Alexeev"]
__license__ = ""
__maintainer__ = "Mike Belov"
__email__ = "dedm@nginx.com"


class LogsOverallTestCase(NginxCollectorTestCase):

    def test_combined(self):
        lines = [
            '178.23.225.78 - - [18/Jun/2015:17:22:25 +0000] "GET /img/docker.png HTTP/1.1" 304 0 ' +
            '"http://ec2-54-78-3-178.eu-west-1.compute.amazonaws.com:4000/" ' +
            '"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_4) AppleWebKit/537.36 (KHTML, like Gecko) ' +
            'Chrome/43.0.2357.124 Safari/537.36"',

            '178.23.225.78 - - [18/Jun/2015:17:22:25 +0000] "GET /api/inventory/objects/ HTTP/1.1" 200 1093 ' +
            '"http://ec2-54-78-3-178.eu-west-1.compute.amazonaws.com:4000/" ' +
            '"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_4) AppleWebKit/537.36 (KHTML, like Gecko) ' +
            'Chrome/43.0.2357.124 Safari/537.36"',

            '127.0.0.1 - - [18/Jun/2015:17:22:33 +0000] "POST /1.0/589fjinijenfirjf/meta/ HTTP/1.1" ' +
            '202 2 "-" "python-requests/2.2.1 CPython/2.7.6 Linux/3.13.0-48-generic"',

            '52.6.158.18 - - [18/Jun/2015:17:22:40 +0000] "GET /#/objects HTTP/1.1" 416 84 ' +
            '"-" "Slackbot-LinkExpanding 1.0 (+https://api.slack.com/robots)"'
        ]

        collector = NginxAccessLogsCollector(object=self.fake_object, tail=lines)
        collector.collect()

        # check
        metrics = self.fake_object.statsd.flush()['metrics']
        assert_that(metrics, has_item('counter'))

        # counters
        counter = metrics['counter']
        for key in ('C|nginx.http.method.get', 'C|nginx.http.request.body_bytes_sent', 'C|nginx.http.status.3xx',
                    'C|nginx.http.status.2xx','C|nginx.http.method.post', 'C|nginx.http.v1_1',
                    'C|nginx.http.status.4xx'):
            assert_that(counter, has_key(key))

        # values
        assert_that(counter['C|nginx.http.method.get'][0][1], equal_to(3))
        assert_that(counter['C|nginx.http.status.2xx'][0][1], equal_to(2))
        assert_that(counter['C|nginx.http.v1_1'][0][1], equal_to(4))
        assert_that(counter['C|nginx.http.request.body_bytes_sent'][0][1], equal_to(84 + 2 + 1093 + 0))

        # check zero values
        for counter_name, counter_key in collector.counters.iteritems():
            if counter_key in collector.parser.keys:
                assert_that(counter, has_key('C|%s' % counter_name))
                if counter_name not in (
                    'nginx.http.status.2xx',
                    'nginx.http.status.3xx',
                    'nginx.http.status.4xx',
                    'nginx.http.v1_1',
                    'nginx.http.request.body_bytes_sent'
                ):
                    assert_that(counter['C|%s' % counter_name][0][1], equal_to(0))
            else:
                if counter_key not in collector.parser.request_variables:
                    assert_that(counter, not_(has_key('C|%s' % counter_name)))

    def test_extended(self):
        log_format = '$remote_addr - $remote_user [$time_local] ' + \
                     '"$request" $status $body_bytes_sent "$http_referer" "$http_user_agent" ' + \
                     'rt=$request_time ut="$upstream_response_time" cs=$upstream_cache_status'

        lines = [
            '1.2.3.4 - - [22/Jan/2010:19:34:21 +0300] "GET /foo/ HTTP/1.1" 200 11078 ' +
            '"http://www.rambler.ru/" "Mozilla/5.0 (Windows; U; Windows NT 5.1" rt=0.010 ut="2.001, 0.345" cs=MISS',

            '1.2.3.4 - - [22/Jan/2010:20:34:21 +0300] "GET /foo/ HTTP/1.1" 300 1078 ' +
            '"http://www.rambler.ru/" "Mozilla/5.0 (Windows; U; Windows NT 5.1" rt=0.010 ut="2.002" cs=HIT',

        ]

        collector = NginxAccessLogsCollector(object=self.fake_object, log_format=log_format, tail=lines)
        collector.collect()

        # check
        metrics = self.fake_object.statsd.flush()['metrics']
        assert_that(metrics, has_item('counter'))
        assert_that(metrics, has_item('timer'))

        # counter keys
        counter = metrics['counter']
        for key in ['C|nginx.http.method.get', 'C|nginx.http.v1_1', 'C|nginx.upstream.next.count',
                    'C|nginx.upstream.request.count', 'C|nginx.http.status.3xx', 'C|nginx.cache.miss',
                    'C|nginx.http.status.2xx', 'C|nginx.http.request.body_bytes_sent', 'C|nginx.cache.hit']:
            assert_that(counter, has_key(key))

        # timer keys
        timer = metrics['timer']
        for key in ['G|nginx.upstream.response.time.pctl95', 'C|nginx.upstream.response.time.count',
                    'C|nginx.http.request.time.count', 'G|nginx.http.request.time',
                    'G|nginx.http.request.time.pctl95', 'G|nginx.http.request.time.median',
                    'G|nginx.http.request.time.max', 'G|nginx.upstream.response.time',
                    'G|nginx.upstream.response.time.median', 'G|nginx.upstream.response.time.max']:
            assert_that(timer, has_key(key))

        # values
        assert_that(counter['C|nginx.http.method.get'][0][1], equal_to(2))
        assert_that(counter['C|nginx.upstream.request.count'][0][1], equal_to(2))
        assert_that(counter['C|nginx.upstream.next.count'][0][1], equal_to(1))
        assert_that(timer['G|nginx.upstream.response.time.max'][0][1], equal_to(2.001+0.345))

        # check zero values
        for counter_name, counter_key in collector.counters.iteritems():
            if counter_key in collector.parser.keys:
                assert_that(counter, has_key('C|%s' % counter_name))
                if counter_name not in (
                    'nginx.http.status.2xx',
                    'nginx.http.status.3xx',
                    'nginx.http.method.get',
                    'nginx.upstream.request.count',
                    'nginx.upstream.next.count',
                    'nginx.upstream.response.time.max',
                    'nginx.http.v1_1',
                    'nginx.cache.miss',
                    'nginx.cache.hit',
                    'nginx.http.request.body_bytes_sent'
                ):
                    assert_that(counter['C|%s' % counter_name][0][1], equal_to(0))
            else:
                if counter_key not in collector.parser.request_variables:
                    assert_that(counter, not_(has_key('C|%s' % counter_name)))

    def test_extend_duplicates(self):
        """
        Test a log format that defines duplicate variables.
        """
        log_format = '$remote_addr - $remote_addr - $remote_addr - $remote_user [$time_local] ' + \
                     '"$request" $status $body_bytes_sent "$http_referer" "$http_user_agent" ' + \
                     'rt=$request_time ut="$upstream_response_time" cs=$upstream_cache_status'

        lines = [
            '1.2.3.4 - 1.2.3.4 - 1.2.3.4 - - [22/Jan/2010:19:34:21 +0300] "GET /foo/ HTTP/1.1" 200 11078 ' +
            '"http://www.rambler.ru/" "Mozilla/5.0 (Windows; U; Windows NT 5.1" rt=0.010 ut="2.001, 0.345" cs=MISS',

            '1.2.3.4 - 1.2.3.4 - 1.2.3.4 - - [22/Jan/2010:20:34:21 +0300] "GET /foo/ HTTP/1.1" 300 1078 ' +
            '"http://www.rambler.ru/" "Mozilla/5.0 (Windows; U; Windows NT 5.1" rt=0.010 ut="2.002" cs=HIT',

        ]

        collector = NginxAccessLogsCollector(object=self.fake_object, log_format=log_format, tail=lines)
        collector.collect()

        # check
        metrics = self.fake_object.statsd.flush()['metrics']
        assert_that(metrics, has_item('counter'))
        assert_that(metrics, has_item('timer'))

        # counter keys
        counter = metrics['counter']
        for key in ['C|nginx.http.method.get', 'C|nginx.http.v1_1', 'C|nginx.upstream.next.count',
                    'C|nginx.upstream.request.count', 'C|nginx.http.status.3xx', 'C|nginx.cache.miss',
                    'C|nginx.http.status.2xx', 'C|nginx.http.request.body_bytes_sent', 'C|nginx.cache.hit']:
            assert_that(counter, has_key(key))

        # timer keys
        timer = metrics['timer']
        for key in ['G|nginx.upstream.response.time.pctl95', 'C|nginx.upstream.response.time.count',
                    'C|nginx.http.request.time.count', 'G|nginx.http.request.time',
                    'G|nginx.http.request.time.pctl95', 'G|nginx.http.request.time.median',
                    'G|nginx.http.request.time.max', 'G|nginx.upstream.response.time',
                    'G|nginx.upstream.response.time.median', 'G|nginx.upstream.response.time.max']:
            assert_that(timer, has_key(key))

        # values
        assert_that(counter['C|nginx.http.method.get'][0][1], equal_to(2))
        assert_that(counter['C|nginx.upstream.request.count'][0][1], equal_to(2))
        assert_that(counter['C|nginx.upstream.next.count'][0][1], equal_to(1))
        assert_that(timer['G|nginx.upstream.response.time.max'][0][1], equal_to(2.001+0.345))

        # check zero values
        for counter_name, counter_key in collector.counters.iteritems():
            if counter_key in collector.parser.keys:
                assert_that(counter, has_key('C|%s' % counter_name))
                if counter_name not in (
                    'nginx.http.status.2xx',
                    'nginx.http.status.3xx',
                    'nginx.http.method.get',
                    'nginx.upstream.request.count',
                    'nginx.upstream.next.count',
                    'nginx.upstream.response.time.max',
                    'nginx.http.v1_1',
                    'nginx.cache.miss',
                    'nginx.cache.hit',
                    'nginx.http.request.body_bytes_sent'
                ):
                    assert_that(counter['C|%s' % counter_name][0][1], equal_to(0))
            else:
                if counter_key not in collector.parser.request_variables:
                    assert_that(counter, not_(has_key('C|%s' % counter_name)))

    def test_extend_duplicates_reported(self):
        """
        Test the specific reported bug format.
        """
        log_format = '$remote_addr - $remote_user [$time_local]  ' + \
                     '"$request" $status $body_bytes_sent ' + \
                     '"$http_referer" "$http_user_agent" ' + \
                     '$request_length $body_bytes_sent'

        lines = [
            '188.165.1.1 - - [17/Nov/2015:22:07:42 +0100]  ' +
            '"GET /2014/09/quicktipp-phpmyadmin-update-script/?pk_campaign=feed&pk_kwd=quicktipp-' +
            'phpmyadmin-update-script HTTP/1.1" ' +
            '200 41110 "http://www.google.co.uk/url?sa=t&source=web&cd=1" "Mozilla/5.0 (Windows NT 6.1; WOW64) ' +
            'AppleWebKit/535.2 (KHTML, like Gecko) Chrome/15.0.874.92 Safari/535.2" 327 41110',

            '192.168.100.200 - - [17/Nov/2015:22:09:26 +0100]  "POST /wp-cron.php?doing_wp_cron=1447794566.' +
            '5160338878631591796875 HTTP/1.0" 200 0 "-" "WordPress/4.3.1; http://my.domain.at.private.com" 281 0'
        ]

        collector = NginxAccessLogsCollector(object=self.fake_object, log_format=log_format, tail=lines)
        collector.collect()

        # check
        metrics = self.fake_object.statsd.flush()['metrics']
        assert_that(metrics, has_item('counter'))

        # counter keys
        counter = metrics['counter']
        for key in ['C|nginx.http.method.post', 'C|nginx.http.method.get', 'C|nginx.http.status.2xx',
                    'C|nginx.http.v1_1', 'C|nginx.http.request.body_bytes_sent', 'C|nginx.http.request.length',
                    'C|nginx.http.v1_0']:
            assert_that(counter, has_key(key))

        # values
        assert_that(counter['C|nginx.http.method.post'][0][1], equal_to(1))
        assert_that(counter['C|nginx.http.method.get'][0][1], equal_to(1))
        assert_that(counter['C|nginx.http.status.2xx'][0][1], equal_to(2))
        assert_that(counter['C|nginx.http.request.length'][0][1], equal_to(608))
        assert_that(counter['C|nginx.http.request.body_bytes_sent'][0][1], equal_to(41110))

        # check zero values
        for counter_name, counter_key in collector.counters.iteritems():
            if counter_key in collector.parser.keys:
                assert_that(counter, has_key('C|%s' % counter_name))
                if counter_name not in (
                    'nginx.http.status.2xx',
                    'nginx.http.method.post',
                    'nginx.http.method.get',
                    'nginx.http.request.length',
                    'nginx.http.request.body_bytes_sent',
                    'nginx.http.v1_1',
                    'nginx.http.v1_0'
                ):
                    assert_that(counter['C|%s' % counter_name][0][1], equal_to(0))
            else:
                if counter_key not in collector.parser.request_variables:
                    assert_that(counter, not_(has_key('C|%s' % counter_name)))
