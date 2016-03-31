# -*- coding: utf-8 -*-
import os

from hamcrest import *

from amplify.agent.context import context
from amplify.agent.nginx.config.config import NginxConfig
from test.base import BaseTestCase

__author__ = "Mike Belov"
__copyright__ = "Copyright (C) Nginx, Inc. All rights reserved."
__credits__ = ["Mike Belov", "Andrei Belov", "Ivan Poluyanov", "Oleg Mamontov", "Andrew Alexeev", "Grant Hulegaard"]
__license__ = ""
__maintainer__ = "Mike Belov"
__email__ = "dedm@nginx.com"


simple_config = os.getcwd() + '/test/fixtures/nginx/simple/nginx.conf'
complex_config = os.getcwd() + '/test/fixtures/nginx/complex/nginx.conf'
huge_config = os.getcwd() + '/test/fixtures/nginx/huge/nginx.conf'
broken_config = os.getcwd() + '/test/fixtures/nginx/broken/nginx.conf'
proxy_buffers_simple_config = os.getcwd() + '/test/fixtures/nginx/proxy_buffers_simple/nginx.conf'
proxy_buffers_complex_config = os.getcwd() + '/test/fixtures/nginx/proxy_buffers_complex/nginx.conf'
tabs_config = os.getcwd() + '/test/fixtures/nginx/custom/tabs.conf'
fastcgi_config = os.getcwd() + '/test/fixtures/nginx/fastcgi/nginx.conf'
json_config = os.getcwd() + '/test/fixtures/nginx/custom/json.conf'
ssl_simple_config = os.getcwd() + '/test/fixtures/nginx/ssl/simple/nginx.conf'
regex_status_config = os.getcwd() + '/test/fixtures/nginx/regex_status/nginx.conf'


class ConfigTestCase(BaseTestCase):

    def test_parse_simple(self):
        config = NginxConfig(simple_config)
        config.full_parse()

        # error logs
        assert_that(config.error_logs, has_length(1))
        assert_that(config.error_logs, has_key('/var/log/nginx/error.log'))

        # access logs
        assert_that(config.access_logs, has_length(2))
        assert_that(config.access_logs, has_item('/var/log/nginx/access.log'))
        assert_that(config.access_logs, has_item('/var/log/nginx/superaccess.log'))
        assert_that(config.access_logs['/var/log/nginx/access.log'], equal_to('super_log_format'))

        # log formats
        assert_that(config.log_formats, has_length(1))
        assert_that(config.log_formats, has_item('super_log_format'))
        assert_that(
            config.log_formats['super_log_format'],
            equal_to(
                '$remote_addr - $remote_user [$time_local] "$request" $status ' +
                '$body_bytes_sent "$http_referer" "$http_user_agent" "$http_x_forwarded_for" ' +
                'rt="$request_time" ua="$upstream_addr" us="$upstream_status" ' +
                'ut="$upstream_response_time" "$gzip_ratio"'
            )
        )

        # stub status urls
        assert_that(config.stub_status_urls, has_length(1))
        assert_that(config.stub_status_urls[0], equal_to('127.0.0.1:81/basic_status'))

        # status urls
        assert_that(config.plus_status_external_urls, has_length(1))
        assert_that(config.plus_status_external_urls[0], equal_to('127.0.0.1:81/plus_status'))

        assert_that(config.plus_status_internal_urls, has_length(1))
        assert_that(config.plus_status_internal_urls[0], equal_to('127.0.0.1:81/plus_status'))

    def test_parse_huge(self):
        config = NginxConfig(huge_config)
        config.full_parse()

        # error logs
        assert_that(config.error_logs, has_length(1))
        assert_that(config.error_logs, has_key('/var/log/nginx-error.log'))

        # access logs
        assert_that(config.access_logs, has_length(2))
        assert_that(config.access_logs, has_item('/var/log/default.log'))
        assert_that(config.access_logs, has_item('/var/log/pp.log'))
        assert_that(config.access_logs['/var/log/pp.log'], equal_to('main'))

        # log formats
        assert_that(config.log_formats, has_length(1))
        assert_that(config.log_formats, has_item('main'))
        assert_that(
            config.log_formats['main'],
            equal_to(
                '$remote_addr - $remote_user [$time_local] "$request" ' +
                '$status $body_bytes_sent "$http_referer" "$http_user_agent" "$http_x_forwarded_for"'
            )
        )

        # stub status url
        assert_that(config.stub_status_urls, has_length(2))
        assert_that(config.stub_status_urls[0], equal_to('127.0.0.1:80/nginx_status'))

    def test_parse_complex(self):
        config = NginxConfig(complex_config)
        config.full_parse()

        context.log.info(config.index)
        context.log.info(config.tree)
        context.log.info(config.files)
        context.log.info(config.checksum())

        # error logs
        assert_that(config.error_logs, has_length(0))

        # access logs
        assert_that(config.access_logs, has_length(0))

        # log formats
        assert_that(config.log_formats, has_length(0))

        # stub status url
        assert_that(config.stub_status_urls, has_length(0))

    def test_broken(self):
        config = NginxConfig(broken_config)
        config.full_parse()

        assert_that(config.tree, equal_to({}))
        assert_that(config.parser_errors, has_length(1))

    def test_broken_includes(self):
        config = NginxConfig(huge_config)
        config.full_parse()

        assert_that(config.tree, not_(equal_to({})))
        assert_that(config.parser_errors, has_length(5))  # 5 missing includes

    def test_proxy_buffers_simple(self):
        config = NginxConfig(proxy_buffers_simple_config)
        config.full_parse()

        assert_that(config.tree, has_key('http'))

        http_bucket = config.tree['http'][0]
        assert_that(http_bucket, has_key('proxy_buffering'))
        assert_that(http_bucket, has_key('proxy_buffers'))

        assert_that(config.parser_errors, has_length(0))
        assert_that(config.test_errors, has_length(0))

    def test_proxy_buffers_complex(self):
        config = NginxConfig(proxy_buffers_complex_config)
        config.full_parse()

        assert_that(config.tree, has_key('http'))

        http_bucket = config.tree['http'][0]
        assert_that(http_bucket, has_key('proxy_buffering'))
        assert_that(http_bucket, has_key('proxy_buffers'))

        location_bucket = config.tree['http'][0]['server'][0][0]['location']['/'][0]
        assert_that(location_bucket, has_key('proxy_buffering'))
        assert_that(location_bucket, has_key('proxy_buffers'))

        assert_that(config.parser_errors, has_length(0))
        assert_that(config.test_errors, has_length(0))

    def test_parse_tabbed_config(self):
        config = NginxConfig(tabs_config)
        config.full_parse()

        assert_that(config.log_formats, has_key('main'))
        assert_that(
            config.log_formats['main'],
            equal_to('"$time_local"\t"$remote_addr"\t"$http_host"\t"$request"\t'
                     '"$status"\t"$body_bytes_sent\t"$http_referer"\t'
                     '"$http_user_agent"\t"$http_x_forwarded_for"')
        )

    def test_fastcgi(self):
        config = NginxConfig(fastcgi_config)
        config.full_parse()

        assert_that(config.tree, has_key('http'))

        http_bucket = config.tree['http'][0]
        server_bucket = http_bucket['server'][0][0]  # fastcgi server tree
        location = server_bucket['location']['~ \\.php$'][0]  # fastcgi pass location tree

        assert_that(location, has_key('fastcgi_pass'))
        assert_that(location, has_key('fastcgi_param'))
        assert_that(location['fastcgi_param'], has_length(17))

    def test_json(self):
        config = NginxConfig(json_config)
        config.full_parse()

        assert_that(config.log_formats, has_key('json'))
        assert_that(
            config.log_formats['json'],
            equal_to('{ "time_iso8601": "$time_iso8601", "browser": [{"modern_browser": "$modern_browser", '
                     '"ancient_browser": "$ancient_browser", "msie": "$msie"}], "core": [{"args": "$args", "arg": '
                     '{ "arg_example": "$arg_example"}, "body_bytes_sent": "$body_bytes_sent", "bytes_sent": '
                     '"$bytes_sent", "cookie": { "cookie_example": "$cookie_example" }, "connection": "$connection", '
                     '"connection_requests": "$connection_requests", "content_length": "$content_length", '
                     '"content_type": "$content_type", "document_root": "$document_root", "document_uri": '
                     '"$document_uri","host": "$host", "hostname": "$hostname", "http": { "http_example": '
                     '"$http_example" }, "https": "$https", "is_args": "$is_args", "limit_rate": "$limit_rate", '
                     '"msec": "$msec", "nginx_version": "$nginx_version", "pid": "$pid", "pipe": "$pipe", '
                     '"proxy_protocol_addr": "$proxy_protocol_addr", "query_string": "$query_string", "realpath_root": '
                     '"$realpath_root", "remote_addr": "$remote_addr", "remote_port": "$remote_port", "remote_user": '
                     '"$remote_user", "request": "$request", "request_body": "$request_body", "request_body_file": '
                     '"$request_body_file", "request_completion": "$request_completion", "request_filename": '
                     '"$request_filename", "request_length": "$request_length", "request_method": "$request_method", '
                     '"request_time": "$request_time", "request_uri": "$request_uri", "scheme": "$scheme", '
                     '"sent_http_": { "sent_http_example": "$sent_http_example" }, "server_addr": "$server_addr", '
                     '"server_name": "$server_name", "server_port": "$server_port", "server_protocol": '
                     '"$server_protocol", "status": "$status", "tcpinfo_rtt": "$tcpinfo_rtt", "tcpinfo_rttvar": '
                     '"$tcpinfo_rttvar", "tcpinfo_snd_cwnd": "$tcpinfo_snd_cwnd", "tcpinfo_rcv_space": '
                     '"$tcpinfo_rcv_space", "uri": "$uri" }]}')
        )

    def test_ssl(self):
        config = NginxConfig(ssl_simple_config)
        config.full_parse()
        config.run_ssl_analysis()

        ssl_certificates = config.ssl_certificates
        assert_that(ssl_certificates, has_length(1))

        # check contents
        assert_that(ssl_certificates.keys()[0], ends_with('certs.d/example.com.crt'))
        assert_that(ssl_certificates.values()[0], has_item('names'))

    def test_regex_status_url(self):
        """
        Check that we could handle regex urls like

        location ~ /(nginx_status|status)
        location ~ ^/nginx_status$
        """
        config = NginxConfig(regex_status_config)
        config.full_parse()

        # check total amount of status urls
        assert_that(config.stub_status_urls, has_length(4))  # we have 4 valid locations in the regex_status/status.conf

        # check each location
        valid_urls_dict = {
            '1.1.1.1:80': [
                '1.1.1.1:80/nginx_status',
                '1.1.1.1:80/status',
            ],
            '1.1.1.1:81': ['1.1.1.1:81/nginx_status'],
            '1.1.1.1:82': [
                '1.1.1.1:82/status_weird_thing', '1.1.1.1:82/nginx_status_weird_thing',
                '1.1.1.1:82/status_weird_some', '1.1.1.1:82/nginx_status_weird_some'
            ],
            '1.1.1.1:84': ['1.1.1.1:84/valid_location'],
        }

        for url in config.stub_status_urls:
            address = url.split('/')[0]
            valid_urls = valid_urls_dict[address]
            assert_that(valid_urls, has_item(url))
