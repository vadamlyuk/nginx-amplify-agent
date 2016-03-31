# -*- coding: utf-8 -*-
from amplify.agent.containers.abstract import AbstractCollector
from amplify.agent.context import context
from amplify.agent.nginx.log.access import NginxAccessLogParser
from amplify.agent.util.tail import FileTail

__author__ = "Mike Belov"
__copyright__ = "Copyright (C) Nginx, Inc. All rights reserved."
__credits__ = ["Mike Belov", "Andrei Belov", "Ivan Poluyanov", "Oleg Mamontov", "Andrew Alexeev", "Grant Hulegaard"]
__license__ = ""
__maintainer__ = "Mike Belov"
__email__ = "dedm@nginx.com"


class NginxAccessLogsCollector(AbstractCollector):

    short_name = 'nginx_alog'

    counters = {
        'nginx.http.method.head': 'request_method',
        'nginx.http.method.get': 'request_method',
        'nginx.http.method.post': 'request_method',
        'nginx.http.method.put': 'request_method',
        'nginx.http.method.delete': 'request_method',
        'nginx.http.method.options': 'request_method',
        'nginx.http.method.other': 'request_method',
        'nginx.http.status.1xx': 'status',
        'nginx.http.status.2xx': 'status',
        'nginx.http.status.3xx': 'status',
        'nginx.http.status.4xx': 'status',
        'nginx.http.status.5xx': 'status',
        'nginx.http.status.discarded': 'status',
        'nginx.http.v0_9': 'server_protocol',
        'nginx.http.v1_0': 'server_protocol',
        'nginx.http.v1_1': 'server_protocol',
        'nginx.http.v2': 'server_protocol',
        'nginx.http.request.body_bytes_sent': 'body_bytes_sent',
        'nginx.http.request.bytes_sent': 'bytes_sent',
        'nginx.http.request.length': 'request_length',
        'nginx.upstream.http.status.1xx': 'upstream_status',
        'nginx.upstream.http.status.2xx': 'upstream_status',
        'nginx.upstream.http.status.3xx': 'upstream_status',
        'nginx.upstream.http.status.4xx': 'upstream_status',
        'nginx.upstream.http.status.5xx': 'upstream_status',
        'nginx.upstream.http.response.length': 'upstream_response_length',
        'nginx.cache.bypass': 'upstream_cache_status',
        'nginx.cache.expired': 'upstream_cache_status',
        'nginx.cache.hit': 'upstream_cache_status',
        'nginx.cache.miss': 'upstream_cache_status',
        'nginx.cache.revalidated': 'upstream_cache_status',
        'nginx.cache.stale': 'upstream_cache_status',
        'nginx.cache.updating': 'upstream_cache_status',
        # 'upstream.next.count': None,  # Not sure how best to handle this since this...ignoring for now.
        # 'upstream.request.count': None  # Not sure how to handle for same reason above.
    }

    valid_http_methods = (
        'head',
        'get',
        'post',
        'put',
        'delete',
        'options'
    )

    def __init__(self, filename=None, log_format=None, tail=None, **kwargs):
        super(NginxAccessLogsCollector, self).__init__(**kwargs)
        self.filename = filename
        self.parser = NginxAccessLogParser(log_format)
        self.tail = tail if tail is not None else FileTail(filename)
        self.filters = []
        
        # skip empty filters and filters for other log file
        for log_filter in self.object.filters:
            if log_filter.empty:
                continue
            if log_filter.filename and log_filter.filename != self.filename:
                continue
            self.filters.append(log_filter)

    def init_counters(self):
        for counter, key in self.counters.iteritems():
            # If keys are in the parser format (access log) or not defined (error log)
            if key in self.parser.keys:
                self.statsd.incr(counter, value=0)

    def collect(self):
        self.init_counters()  # set all counters to 0

        count = 0
        for line in self.tail:
            count += 1
            try:
                parsed = self.parser.parse(line)
            except:
                context.log.debug('could parse line %s' % line, exc_info=True)
                parsed = None

            if not parsed:
                continue

            if parsed['malformed']:
                self.request_malformed()
            else:
                # try to match custom filters 
                matched_filters = []
                for log_filter in self.filters:
                    if log_filter.match(parsed):
                        matched_filters.append(log_filter)
                
                for method in (
                    self.http_method,
                    self.http_status,
                    self.http_version,
                    self.request_length,
                    self.body_bytes_sent,
                    self.bytes_sent,
                    self.gzip_ration,
                    self.request_time,
                    self.upstreams,
                ):
                    try:
                        method(parsed, matched_filters)
                    except Exception as e:
                        exception_name = e.__class__.__name__
                        context.log.error(
                            'failed to collect log metrics %s due to %s' % (method.__name__, exception_name))
                        context.log.debug('additional info:', exc_info=True)

        context.log.debug('%s processed %s lines from %s' % (self.object.id, count, self.filename))

    def request_malformed(self):
        """
        nginx.http.request.malformed
        """
        self.statsd.incr('nginx.http.request.malformed')

    def http_method(self, data, matched_filters=None):
        """
        nginx.http.method.head
        nginx.http.method.get
        nginx.http.method.post
        nginx.http.method.put
        nginx.http.method.delete
        nginx.http.method.options
        nginx.http.method.other
        
        :param data: {} of parsed line
        :param matched_filters: [] of matched filters
        """
        if 'request_method' in data:
            method = data['request_method'].lower()
            method = method if method in self.valid_http_methods else 'other'
            metric_name = 'nginx.http.method.%s' % method
            self.statsd.incr(metric_name)

            # call custom filters
            if matched_filters:
                self.count_custom_filter(matched_filters, metric_name, 1, self.statsd.incr)

    def http_status(self, data, matched_filters=None):
        """
        nginx.http.status.1xx
        nginx.http.status.2xx
        nginx.http.status.3xx
        nginx.http.status.4xx
        nginx.http.status.5xx
        nginx.http.status.discarded
        
        :param data: {} of parsed line
        :param matched_filters: [] of matched filters
        """
        if 'status' in data:
            status = data['status']
            suffix = 'discarded' if status in ('499', '444', '408') else '%sxx' % status[0]
            metric_name = 'nginx.http.status.%s' % suffix
            self.statsd.incr(metric_name)

            # call custom filters
            if matched_filters:
                self.count_custom_filter(matched_filters, metric_name, 1, self.statsd.incr)

    def http_version(self, data, matched_filters=None):
        """
        nginx.http.v0_9
        nginx.http.v1_0
        nginx.http.v1_1
        nginx.http.v2
        
        :param data: {} of parsed line
        :param matched_filters: [] of matched filters
        """
        if 'server_protocol' in data:
            proto = data['server_protocol']
            if not proto.startswith('HTTP'):
                return

            version = proto.split('/')[-1]

            if version.startswith('0.9'):
                suffix = '0_9'
            elif version.startswith('1.0'):
                suffix = '1_0'
            elif version.startswith('1.1'):
                suffix = '1_1'
            elif version.startswith('2.0'):
                suffix = '2'
            else:
                suffix = version.replace('.', '_')

            metric_name = 'nginx.http.v%s' % suffix
            self.statsd.incr(metric_name)

            # call custom filters
            if matched_filters:
                self.count_custom_filter(matched_filters, metric_name, 1, self.statsd.incr)

    def request_length(self, data, matched_filters=None):
        """
        nginx.http.request.length
        
        :param data: {} of parsed line
        :param matched_filters: [] of matched filters
        """
        if 'request_length' in data:
            metric_name, value = 'nginx.http.request.length', data['request_length']
            self.statsd.incr(metric_name, value)

            # call custom filters
            if matched_filters:
                self.count_custom_filter(matched_filters, metric_name, value, self.statsd.incr)

    def body_bytes_sent(self, data, matched_filters=None):
        """
        nginx.http.request.body_bytes_sent

        :param data: {} of parsed line
        :param matched_filters: [] of matched filters
        """
        if 'body_bytes_sent' in data:
            metric_name, value = 'nginx.http.request.body_bytes_sent', data['body_bytes_sent']
            self.statsd.incr(metric_name, value)

            # call custom filters
            if matched_filters:
                self.count_custom_filter(matched_filters, metric_name, value, self.statsd.incr)

    def bytes_sent(self, data, matched_filters=None):
        """
        nginx.http.request.bytes_sent

        :param data: {} of parsed line
        :param matched_filters: [] of matched filters
        """
        if 'bytes_sent' in data:
            metric_name, value = 'nginx.http.request.bytes_sent', data['bytes_sent']
            self.statsd.incr(metric_name, value)

            # call custom filters
            if matched_filters:
                self.count_custom_filter(matched_filters, metric_name, value, self.statsd.incr)

    def gzip_ration(self, data, matched_filters=None):
        """
        nginx.http.gzip.ratio
        
        :param data: {} of parsed line
        :param matched_filters: [] of matched filters
        """
        if 'gzip_ratio' in data:
            metric_name, value = 'nginx.http.gzip.ratio', data['gzip_ratio']
            self.statsd.average(metric_name, value)

            # call custom filters
            if matched_filters:
                self.count_custom_filter(matched_filters, metric_name, value, self.statsd.average)

    def request_time(self, data, matched_filters=None):
        """
        nginx.http.request.time
        nginx.http.request.time.median
        nginx.http.request.time.max
        nginx.http.request.time.pctl95
        nginx.http.request.time.count
        
        :param data: {} of parsed line
        :param matched_filters: [] of matched filters
        """
        if 'request_time' in data:
            metric_name, value = 'nginx.http.request.time', sum(data['request_time'])
            self.statsd.timer(metric_name, value)

            # call custom filters
            if matched_filters:
                self.count_custom_filter(matched_filters, metric_name, value, self.statsd.timer)

    def upstreams(self, data, matched_filters=None):
        """
        nginx.cache.bypass
        nginx.cache.expired
        nginx.cache.hit
        nginx.cache.miss
        nginx.cache.revalidated
        nginx.cache.stale
        nginx.cache.updating
        nginx.upstream.request.count
        nginx.upstream.next.count
        nginx.upstream.connect.time
        nginx.upstream.connect.time.median
        nginx.upstream.connect.time.max
        nginx.upstream.connect.time.pctl95
        nginx.upstream.connect.time.count
        nginx.upstream.header.time
        nginx.upstream.header.time.median
        nginx.upstream.header.time.max
        nginx.upstream.header.time.pctl95
        nginx.upstream.header.time.count
        nginx.upstream.response.time
        nginx.upstream.response.time.median
        nginx.upstream.response.time.max
        nginx.upstream.response.time.pctl95
        nginx.upstream.response.time.count
        nginx.upstream.http.status.1xx
        nginx.upstream.http.status.2xx
        nginx.upstream.http.status.3xx
        nginx.upstream.http.status.4xx
        nginx.upstream.http.status.5xx
        nginx.upstream.http.response.length
        
        :param data: {} of parsed line
        :param matched_filters: [] of matched filters
        """

        # find out if we have info about upstreams
        empty_values = ('-', '')
        upstream_data_found = False
        for key in data.iterkeys():
            if key.startswith('upstream') and data[key] not in empty_values:
                upstream_data_found = True
                break

        if not upstream_data_found:
            return

        # counters
        upstream_response = False
        if 'upstream_status' in data:
            status = data['upstream_status']
            suffix = '%sxx' % status[0]
            metric_name = 'nginx.upstream.http.status.%s' % suffix
            upstream_response = True if suffix in ('2xx', '3xx') else False  # Set flag for upstream length processing
            self.statsd.incr(metric_name)

            # call custom filters
            if matched_filters:
                self.count_custom_filter(matched_filters, metric_name, 1, self.statsd.incr)

        if upstream_response and 'upstream_response_length' in data:
            metric_name, value = 'nginx.upstream.http.response.length', data['upstream_response_length']
            self.statsd.incr(metric_name, value)

            # call custom filters
            if matched_filters:
                self.count_custom_filter(matched_filters, metric_name, value, self.statsd.incr)

        # gauges
        upstream_switches = None
        for metric_name, key_name in {
            'nginx.upstream.connect.time': 'upstream_connect_time',
            'nginx.upstream.response.time': 'upstream_response_time',
            'nginx.upstream.header.time': 'upstream_header_time'
        }.iteritems():
            if key_name in data:
                values = data[key_name]

                # set upstream switches one time
                if len(values) > 1 and upstream_switches is None:
                    upstream_switches = len(values) - 1

                # store all values
                value = sum(values)
                self.statsd.timer(metric_name, value)

                # call custom filters
                if matched_filters:
                    self.count_custom_filter(matched_filters, metric_name, value, self.statsd.timer)

        # log upstream switches
        metric_name, value = 'nginx.upstream.next.count', 0 if upstream_switches is None else upstream_switches
        self.statsd.incr(metric_name, value)

        # call custom filters
        if matched_filters:
            self.count_custom_filter(matched_filters, metric_name, value, self.statsd.incr)

        # cache
        if 'upstream_cache_status' in data:
            cache_status = data['upstream_cache_status']
            if not cache_status.startswith('-'):
                metric_name = 'nginx.cache.%s' % cache_status.lower()
                self.statsd.incr(metric_name)

                # call custom filters
                if matched_filters:
                    self.count_custom_filter(matched_filters, metric_name, 1, self.statsd.incr)

        # log total upstream requests
        metric_name = 'nginx.upstream.request.count'
        self.statsd.incr(metric_name)

        # call custom filters
        if matched_filters:
            self.count_custom_filter(matched_filters, metric_name, 1, self.statsd.incr)

    @staticmethod
    def count_custom_filter(matched_filters, metric_name, value, method):
        """
        Collect custom metric

        :param matched_filters: [] of matched filters
        :param metric_name: str metric name
        :param value: int/float value
        :param method: function to call
        :return:
        """
        for log_filter in matched_filters:
            if log_filter.metric == metric_name:
                full_metric_name = '%s||%s' % (log_filter.metric, log_filter.filter_rule_id)
                method(full_metric_name, value)
