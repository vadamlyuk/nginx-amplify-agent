# -*- coding: utf-8 -*-
import os

from hamcrest import *

from amplify.agent.nginx.config.parser import NginxConfigParser, IGNORED_DIRECTIVES
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
rewrites_config = os.getcwd() + '/test/fixtures/nginx/rewrites/nginx.conf'
map_lua_perl = os.getcwd() + '/test/fixtures/nginx/map_lua_perl/nginx.conf'
ssl_broken_config = os.getcwd() + '/test/fixtures/nginx/ssl/broken/nginx.conf'
bad_log_directives_config = os.getcwd() + '/test/fixtures/nginx/broken/bad_logs.conf'
includes_config = os.getcwd() + '/test/fixtures/nginx/includes/nginx.conf'
windows_config = os.getcwd() + '/test/fixtures/nginx/windows/nginx.conf'
tab_config = os.getcwd() + '/test/fixtures/nginx/custom/tabs.conf'
json_config = os.getcwd() + '/test/fixtures/nginx/custom/json.conf'
ssl_simple_config = os.getcwd() + '/test/fixtures/nginx/ssl/simple/nginx.conf'
sub_filter_config = os.getcwd() + '/test/fixtures/nginx/custom/sub_filter.conf'


class ParserTestCase(BaseTestCase):

    def test_parse_simple(self):
        cfg = NginxConfigParser(simple_config)

        cfg.parse()
        tree = cfg.simplify()
        indexed_tree = cfg.tree

        # common structure
        assert_that(tree, has_key('http'))
        assert_that(tree, has_key('events'))

        # http
        http = tree['http']
        assert_that(http, has_key('server'))
        assert_that(http, has_key('types'))
        assert_that(http, has_key('include'))
        assert_that(http['server'], is_(instance_of(list)))
        assert_that(http['server'], has_length(2))

        # server
        server = http['server'][1]
        assert_that(server, has_key('listen'))
        assert_that(server, has_key('location'))
        assert_that(server, has_key('server_name'))
        assert_that(
            server['server_name'], equal_to('127.0.0.1 "~^([a-z]{2})?\.?test\.nginx\.org" "~^([a-z]{2})?\.?beta\.nginx\.org"')
        )
        assert_that(server['location'], is_(instance_of(dict)))

        # location
        location = server['location']
        assert_that(location, has_key('/basic_status'))

        # nested location
        assert_that(http['server'][0]['location']['/'], has_key('location'))

        # included mimes
        mimes = http['types']
        assert_that(mimes, has_key('application/java-archive'))

        # check index tree
        worker_connections_index = indexed_tree['events'][0]['worker_connections'][1]
        basic_status_index = indexed_tree['http'][0]['server'][1][0]['location']['/basic_status'][1]
        stub_status_in_basic_index = indexed_tree['http'][0]['server'][1][0]['location']['/basic_status'][0]['stub_status'][1]
        proxy_pass_index = indexed_tree['http'][0]['server'][0][0]['location']['/'][0]['proxy_pass'][1]

        assert_that(cfg.index[worker_connections_index], equal_to((0, 6)))  # root file, line number 6
        assert_that(cfg.index[basic_status_index], equal_to((0, 67)))  # root file, line number 65
        assert_that(cfg.index[stub_status_in_basic_index], equal_to((0, 69)))  # root file, line number 66
        assert_that(cfg.index[proxy_pass_index], equal_to((2, 13)))  # third loaded file, line number 13

    def test_parse_huge(self):
        cfg = NginxConfigParser(huge_config)

        cfg.parse()
        tree = cfg.simplify()
        indexed_tree = cfg.tree

        # common structure
        assert_that(tree, has_key('http'))
        assert_that(tree, has_key('events'))

        # http
        http = tree['http']
        assert_that(http, has_key('server'))
        assert_that(http, has_key('include'))
        assert_that(http['server'], is_(instance_of(list)))
        assert_that(http['server'], has_length(8))

        # map
        http_map = http['map']
        assert_that(http_map, equal_to({'$dirname $diruri': {'default': '"dirindex.html"', 'include': ['"dir.map"']}}))

        # check index tree
        books_location_index = indexed_tree['http'][0]['server'][2][0]['location']['/books/'][1]
        assert_that(cfg.index[books_location_index], equal_to((0, 134)))  # root file, line number 134

    def test_parse_complex(self):
        cfg = NginxConfigParser(complex_config)

        cfg.parse()
        tree = cfg.simplify()
        indexed_tree = cfg.tree

        # common structure
        assert_that(tree, has_key('http'))
        assert_that(tree, has_key('events'))

        # http
        http = tree['http']
        assert_that(http, has_key('server'))
        assert_that(http, has_key('upstream'))
        assert_that(http, has_key('include'))
        assert_that(http['server'], is_(instance_of(list)))
        assert_that(http['server'], has_length(11))

        # upstream
        upstream = http['upstream']
        assert_that(upstream, has_length(2))

        # ifs
        for server in http['server']:
            if server.get('listen', '') == '127.0.0.3:10122':
                assert_that(server, has_item('if'))

        # check index tree
        x1_location_index = indexed_tree['http'][0]['server'][0][0]['location']['/'][1]
        x2_return_index = indexed_tree['http'][0]['server'][1][0]['location']['/'][0]['return'][1]
        assert_that(cfg.index[x1_location_index], equal_to((0, 8)))  # root file, line number 8
        assert_that(cfg.index[x2_return_index], equal_to((0, 9)))  # root file, line number 9

    def test_parse_rewrites(self):
        cfg = NginxConfigParser(rewrites_config)

        cfg.parse()
        tree = cfg.simplify()

        # common structure
        assert_that(tree, has_key('http'))

        # http
        http = tree['http']
        assert_that(http, has_key('server'))

        # rewrites
        for server in http['server']:
            if server.get('server_name', '') == 'mb.some.org localhost melchior melchior.some.org':
                assert_that(server, has_item('rewrite'))

    def test_parse_map_lua_perl(self):
        cfg = NginxConfigParser(map_lua_perl)

        cfg.parse()
        tree = cfg.simplify()

        # common structure
        assert_that(tree, has_key('http'))

        # http
        http = tree['http']
        assert_that(http, has_key('server'))
        assert_that(http, has_key('map'))
        assert_that(http, has_key('perl_set'))

        # lua
        for server in http['server']:
            if server.get('server_name', '') == '127.0.0.1':
                assert_that(server, has_item('lua_shared_dict'))

                for location, data in server['location'].iteritems():
                    if location == '= /some/':
                        assert_that(data, has_item('rewrite_by_lua'))

        # maps
        assert_that(http['map']['$http_user_agent $device'], has_key('~*Nexus\\ One|Nexus\\ S'))
        assert_that(http['map']['$http_referer $bad_referer'], has_key('"~* move-"'))

    def test_parse_ssl(self):
        """
        This test case specifically checks to see that none of the excluded directives (SSL focused) are parsed.
        """
        cfg = NginxConfigParser(ssl_broken_config)

        cfg.parse()
        tree = cfg.simplify()

        assert_that(tree, has_key('server'))

        # ssl
        for directive in IGNORED_DIRECTIVES:
            assert_that(tree['server'][1], is_not(has_item(directive)))
        assert_that(tree['server'][1], has_item('ssl_certificate'))
        assert_that(tree['server'][1]['ssl_certificate'], equal_to('certs.d/example.cert'))

    def test_parse_bad_access_and_error_log(self):
        """
        Test case for ignoring access_log and error_log edge cases.
        """
        cfg = NginxConfigParser(bad_log_directives_config)

        cfg.parse()
        tree = cfg.simplify()

        assert_that(tree, not has_key('access_log'))
        assert_that(tree, not has_key('error_log'))

    def test_lightweight_parse_includes(self):
        # simple
        cfg = NginxConfigParser(simple_config)
        files = cfg.collect_all_files()
        assert_that(files.keys(), equal_to([
            '/amplify/test/fixtures/nginx/simple/conf.d/something.conf',
            '/amplify/test/fixtures/nginx/simple/mime.types',
            '/amplify/test/fixtures/nginx/simple/nginx.conf'
        ]))

        # includes
        cfg = NginxConfigParser(includes_config)
        files = cfg.collect_all_files()
        assert_that(files.keys(), equal_to([
            '/amplify/test/fixtures/nginx/includes/conf.d/something.conf',
            '/amplify/test/fixtures/nginx/includes/mime.types',
            '/amplify/test/fixtures/nginx/includes/conf.d/additional.conf',
            '/amplify/test/fixtures/nginx/includes/conf.d/include.conf',
            '/amplify/test/fixtures/nginx/includes/nginx.conf'
        ]))

    def test_parse_windows(self):
        """
        Test that windows style line endings are replaces with Unix style ones for parser.
        """
        cfg = NginxConfigParser(windows_config)

        cfg.parse()
        tree = cfg.simplify()

        assert_that(
            tree['http']['gzip_types'], equal_to(
                'application/atom+xml\n    application/javascript\n    application/json\n    application/ld+json\n' \
                '    application/manifest+json\n    application/rss+xml\n    application/vnd.geo+json\n    ' \
                'application/vnd.ms-fontobject\n    application/x-font-ttf\n    application/x-web-app-manifest+json\n'\
                '    application/xhtml+xml\n    application/xml\n    font/opentype\n    image/bmp\n    ' \
                'image/svg+xml\n    image/x-icon\n    text/cache-manifest\n    text/css\n    text/plain\n    ' \
                'text/vcard\n    text/vnd.rim.location.xloc\n    text/vtt\n    text/x-component\n   ' \
                ' text/x-cross-domain-policy'
            )
        )

    def test_parse_tabs(self):
        """
        Test tab config format.  This is the first test investigating Parser auto-escape problems.
        """
        cfg = NginxConfigParser(tab_config)

        cfg.parse()
        tree = cfg.simplify()

        for log_format in tree['http']['log_format'].itervalues():
            assert_that(log_format.find('\\'), equal_to(-1))

    def test_parse_json(self):
        """
        Test json config format.  This is the first test investigating Parser auto-escape problems.
        """
        cfg = NginxConfigParser(json_config)

        cfg.parse()
        tree = cfg.simplify()

        for log_format in tree['http']['log_format'].itervalues():
            assert_that(log_format.find('\\'), equal_to(-1))

    def test_parse_ssl_simple_config(self):
        cfg = NginxConfigParser(ssl_simple_config)
        cfg.parse()
        tree = cfg.simplify()

        assert_that(tree, has_key('http'))
        http = tree['http']

        assert_that(http, has_key('server'))
        server = http['server']

        # ssl
        for directive in IGNORED_DIRECTIVES:
            assert_that(server[2], is_not(has_item(directive)))
        assert_that(server[2], has_item('ssl_certificate'))
        assert_that(server[2]['ssl_certificate'], equal_to('certs.d/example.com.crt'))

        ssl_certificates = cfg.ssl_certificates
        assert_that(len(ssl_certificates), equal_to(1))

    def test_lightweight_parse_includes_permissions(self):
        """
        Checks that we get file permissions during lightweight parsing
        """
        cfg = NginxConfigParser(simple_config)
        files = cfg.collect_all_files()

        test_file = '/amplify/test/fixtures/nginx/simple/conf.d/something.conf'
        size = os.path.getsize(test_file)
        mtime = int(os.path.getmtime(test_file))
        permissions = oct(os.stat(test_file).st_mode & 0777)

        assert_that(
            files[test_file],
            equal_to('%s_%s_%s' % (size, mtime, permissions))
        )

    def test_sub_filter(self):
        cfg = NginxConfigParser(sub_filter_config)
        cfg.parse()
        tree = cfg.simplify()

        assert_that(
            tree['http']['sub_filter'],
            equal_to(
                '\'</body>\'\'<p style="position: fixed;top:\n            60px;width:100%;;background-color: #f00;background-color:\n            rgba(255,0,0,0.5);color: #000;text-align: center;font-weight:\n            bold;padding: 0.5em;z-index: 1;">Test</p></body>\''
            )
        )
