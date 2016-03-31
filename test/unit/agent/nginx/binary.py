# -*- coding: utf-8 -*-
from hamcrest import *

from amplify.agent.nginx.binary import get_prefix_and_conf_path
from test.base import BaseTestCase

__author__ = "Mike Belov"
__copyright__ = "Copyright (C) Nginx, Inc. All rights reserved."
__credits__ = ["Mike Belov", "Andrei Belov", "Ivan Poluyanov", "Oleg Mamontov", "Andrew Alexeev"]
__license__ = ""
__maintainer__ = "Mike Belov"
__email__ = "dedm@nginx.com"


class PrefixConfigPathTestCase(BaseTestCase):
    """
    Cases are named with binary code

    Coding scheme:
    -c  -p  --prefix --conf-path
    0    0         0           1

    """

    def test_0000(self):
        # none specified
        bin_path, prefix, conf_path, version = get_prefix_and_conf_path(
            'nginx: master process nginx', {}
        )
        assert_that(bin_path, equal_to('nginx'))
        assert_that(prefix, equal_to('/usr/local/nginx'))
        assert_that(conf_path, equal_to('/usr/local/nginx/conf/nginx.conf'))

    def test_0001_absolute(self):
        bin_path, prefix, conf_path, version = get_prefix_and_conf_path(
            'nginx: master process nginx', {'conf-path': '/etc/nginx/nginx.conf'}
        )
        assert_that(bin_path, equal_to('nginx'))
        assert_that(prefix, equal_to('/usr/local/nginx'))
        assert_that(conf_path, equal_to('/etc/nginx/nginx.conf'))

    def test_0001_relative(self):
        bin_path, prefix, conf_path, version = get_prefix_and_conf_path(
            'nginx: master process nginx', {'conf-path': 'dir/nginx.conf'}
        )
        assert_that(bin_path, equal_to('nginx'))
        assert_that(prefix, equal_to('/usr/local/nginx'))
        assert_that(conf_path, equal_to('/usr/local/nginx/dir/nginx.conf'))

    def test_0010(self):
        bin_path, prefix, conf_path, version = get_prefix_and_conf_path(
            'nginx: master process nginx', {'prefix': '/var'}
        )
        assert_that(bin_path, equal_to('nginx'))
        assert_that(prefix, equal_to('/var'))
        assert_that(conf_path, equal_to('/var/conf/nginx.conf'))

    def test_0011_absolute(self):
        bin_path, prefix, conf_path, version = get_prefix_and_conf_path(
            'nginx: master process nginx', {'prefix': '/var', 'conf-path': '/etc/nginx/nginx.conf'}
        )
        assert_that(bin_path, equal_to('nginx'))
        assert_that(prefix, equal_to('/var'))
        assert_that(conf_path, equal_to('/etc/nginx/nginx.conf'))

    def test_0011_relative(self):
        bin_path, prefix, conf_path, version = get_prefix_and_conf_path(
            'nginx: master process nginx', {'prefix': '/var', 'conf-path': 'dir/nginx.conf'}
        )
        assert_that(bin_path, equal_to('nginx'))
        assert_that(prefix, equal_to('/var'))
        assert_that(conf_path, equal_to('/var/dir/nginx.conf'))

    def test_0100(self):
        bin_path, prefix, conf_path, version = get_prefix_and_conf_path(
            'nginx: master process nginx -p /var', {}
        )
        assert_that(bin_path, equal_to('nginx'))
        assert_that(prefix, equal_to('/var'))
        assert_that(conf_path, equal_to('/var/conf/nginx.conf'))

    def test_0101_absolute(self):
        bin_path, prefix, conf_path, version = get_prefix_and_conf_path(
            'nginx: master process nginx -p /var', {'conf-path': '/etc/nginx.conf'}
        )
        assert_that(bin_path, equal_to('nginx'))
        assert_that(prefix, equal_to('/var'))
        assert_that(conf_path, equal_to('/etc/nginx.conf'))

    def test_0101_relative(self):
        bin_path, prefix, conf_path, version = get_prefix_and_conf_path(
            'nginx: master process nginx -p /var', {'conf-path': 'dir/nginx.conf'}
        )
        assert_that(bin_path, equal_to('nginx'))
        assert_that(prefix, equal_to('/var'))
        assert_that(conf_path, equal_to('/var/dir/nginx.conf'))

    def test_0110(self):
        bin_path, prefix, conf_path, version = get_prefix_and_conf_path(
            'nginx: master process nginx -p /var', {'prefix': '/foo'}
        )
        assert_that(bin_path, equal_to('nginx'))
        assert_that(prefix, equal_to('/var'))
        assert_that(conf_path, equal_to('/var/conf/nginx.conf'))

    def test_0111_absolute(self):
        bin_path, prefix, conf_path, version = get_prefix_and_conf_path(
            'nginx: master process nginx -p /var', {'prefix': '/foo', 'conf-path': '/etc/nginx.conf'}
        )
        assert_that(bin_path, equal_to('nginx'))
        assert_that(prefix, equal_to('/var'))
        assert_that(conf_path, equal_to('/etc/nginx.conf'))

    def test_0111_relative(self):
        bin_path, prefix, conf_path, version = get_prefix_and_conf_path(
            'nginx: master process nginx -p /var', {'prefix': '/foo', 'conf-path': 'dir/nginx.conf'}
        )
        assert_that(bin_path, equal_to('nginx'))
        assert_that(prefix, equal_to('/var'))
        assert_that(conf_path, equal_to('/var/dir/nginx.conf'))

    def test_1000_absolute(self):
        bin_path, prefix, conf_path, version = get_prefix_and_conf_path(
            'nginx: master process nginx -c /etc/nginx.conf', {}
        )
        assert_that(bin_path, equal_to('nginx'))
        assert_that(prefix, equal_to('/usr/local/nginx'))
        assert_that(conf_path, equal_to('/etc/nginx.conf'))

    def test_1000_relative(self):
        bin_path, prefix, conf_path, version = get_prefix_and_conf_path(
            'nginx: master process nginx -c dir/nginx.conf', {}
        )
        assert_that(bin_path, equal_to('nginx'))
        assert_that(prefix, equal_to('/usr/local/nginx'))
        assert_that(conf_path, equal_to('/usr/local/nginx/dir/nginx.conf'))

    def test_1001_absolute(self):
        bin_path, prefix, conf_path, version = get_prefix_and_conf_path(
            'nginx: master process nginx -c /etc/nginx.conf', {'conf-path': '/foo/nginx.conf'}
        )
        assert_that(bin_path, equal_to('nginx'))
        assert_that(prefix, equal_to('/usr/local/nginx'))
        assert_that(conf_path, equal_to('/etc/nginx.conf'))

    def test_1001_relative(self):
        bin_path, prefix, conf_path, version = get_prefix_and_conf_path(
            'nginx: master process nginx -c dir/nginx.conf', {'conf-path': 'foo/nginx.conf'}
        )
        assert_that(bin_path, equal_to('nginx'))
        assert_that(prefix, equal_to('/usr/local/nginx'))
        assert_that(conf_path, equal_to('/usr/local/nginx/dir/nginx.conf'))

    def test_1010_absolute(self):
        bin_path, prefix, conf_path, version = get_prefix_and_conf_path(
            'nginx: master process nginx -c /etc/nginx.conf', {'prefix': '/var'}
        )
        assert_that(bin_path, equal_to('nginx'))
        assert_that(prefix, equal_to('/var'))
        assert_that(conf_path, equal_to('/etc/nginx.conf'))

    def test_1010_relative(self):
        bin_path, prefix, conf_path, version = get_prefix_and_conf_path(
            'nginx: master process nginx -c dir/nginx.conf', {'prefix': '/var'}
        )
        assert_that(bin_path, equal_to('nginx'))
        assert_that(prefix, equal_to('/var'))
        assert_that(conf_path, equal_to('/var/dir/nginx.conf'))

    def test_1011_absolute(self):
        bin_path, prefix, conf_path, version = get_prefix_and_conf_path(
            'nginx: master process nginx -c /etc/nginx.conf', {'prefix': '/var', 'conf-path': '/foo/nginx.conf'}
        )
        assert_that(bin_path, equal_to('nginx'))
        assert_that(prefix, equal_to('/var'))
        assert_that(conf_path, equal_to('/etc/nginx.conf'))

    def test_1011_relative(self):
        bin_path, prefix, conf_path, version = get_prefix_and_conf_path(
            'nginx: master process nginx -c dir/nginx.conf', {'prefix': '/var', 'conf-path': '/foo/nginx.conf'}
        )
        assert_that(bin_path, equal_to('nginx'))
        assert_that(prefix, equal_to('/var'))
        assert_that(conf_path, equal_to('/var/dir/nginx.conf'))

    def test_1100_absolute(self):
        bin_path, prefix, conf_path, version = get_prefix_and_conf_path(
            'nginx: master process nginx -p /var -c /etc/nginx.conf', {}
        )
        assert_that(bin_path, equal_to('nginx'))
        assert_that(prefix, equal_to('/var'))
        assert_that(conf_path, equal_to('/etc/nginx.conf'))

    def test_1100_relative(self):
        bin_path, prefix, conf_path, version = get_prefix_and_conf_path(
            'nginx: master process nginx -p /var -c dir/nginx.conf', {}
        )
        assert_that(bin_path, equal_to('nginx'))
        assert_that(prefix, equal_to('/var'))
        assert_that(conf_path, equal_to('/var/dir/nginx.conf'))

    def test_1101_absolute(self):
        bin_path, prefix, conf_path, version = get_prefix_and_conf_path(
            'nginx: master process nginx -p /var -c /etc/nginx.conf', {'conf-path': '/foo/nginx.conf'}
        )
        assert_that(bin_path, equal_to('nginx'))
        assert_that(prefix, equal_to('/var'))
        assert_that(conf_path, equal_to('/etc/nginx.conf'))

    def test_1101_relative(self):
        bin_path, prefix, conf_path, version = get_prefix_and_conf_path(
            'nginx: master process nginx -p /var -c dir/nginx.conf', {'conf-path': '/foo/nginx.conf'}
        )
        assert_that(bin_path, equal_to('nginx'))
        assert_that(prefix, equal_to('/var'))
        assert_that(conf_path, equal_to('/var/dir/nginx.conf'))

    def test_1111_absolute(self):
        bin_path, prefix, conf_path, version = get_prefix_and_conf_path(
            'nginx: master process nginx -p /var -c /etc/nginx.conf', {'prefix': '/var', 'conf-path': '/foo/nginx.conf'}
        )
        assert_that(bin_path, equal_to('nginx'))
        assert_that(prefix, equal_to('/var'))
        assert_that(conf_path, equal_to('/etc/nginx.conf'))

    def test_1111_relative(self):
        bin_path, prefix, conf_path, version = get_prefix_and_conf_path(
            'nginx: master process nginx -p /var -c dir/nginx.conf', {'prefix': '/var', 'conf-path': '/foo/nginx.conf'}
        )
        assert_that(bin_path, equal_to('nginx'))
        assert_that(prefix, equal_to('/var'))
        assert_that(conf_path, equal_to('/var/dir/nginx.conf'))

