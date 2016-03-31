# -*- coding: utf-8 -*-
import re

import psutil

from amplify.agent.containers.abstract import AbstractCollector
from amplify.agent.context import context
from amplify.agent.nginx.binary import nginx_v
from amplify.agent.util import subp

__author__ = "Mike Belov"
__copyright__ = "Copyright (C) Nginx, Inc. All rights reserved."
__credits__ = ["Mike Belov", "Andrei Belov", "Ivan Poluyanov", "Oleg Mamontov", "Andrew Alexeev", "Grant Hulegaard"]
__license__ = ""
__maintainer__ = "Mike Belov"
__email__ = "dedm@nginx.com"


class NginxCommonMetaCollector(AbstractCollector):

    short_name = 'nginx_meta'

    def collect(self):
        meta = {
            'pid': self.object.pid,
            'local_id': self.object.local_id,
            'parent_hostname': context.hostname,
            'start_time': None,
            'running': True,
            'stub_status_enabled': self.object.stub_status_enabled,
            'status_module_enabled': self.object.plus_status_enabled,
            'stub_status_url': self.object.stub_status_url,
            'plus_status_url': self.object.plus_status_external_url or self.object.plus_status_internal_url,
            'version': None,
            'plus': {
                'enabled': False,
                'release': None
            },
            'configure': {},
            'packages': {},
            'path': {
                'bin': self.object.bin_path,
                'conf': self.object.conf_path,
            },
            'warnings': [],
            'ssl': {},
        }

        for method in (
            self.nginx_minus_v,
            self.find_packages,
            self.nginx_uptime,
            self.open_ssl
        ):
            try:
                method(meta)
            except Exception as e:
                exception_name = e.__class__.__name__
                context.log.error('failed to collect meta %s due to %s' % (method.__name__, exception_name))
                context.log.debug('additional info:', exc_info=True)

        self.metad.meta(meta)

    def nginx_minus_v(self, meta):
        """ call -V and parse """
        parsed_v = nginx_v(self.object.bin_path)
        meta['ssl'] = parsed_v['ssl']
        meta['version'] = parsed_v['version']
        meta['plus'] = parsed_v['plus']
        meta['configure'] = parsed_v['configure']

    def find_packages(self, meta):
        pass

    def nginx_uptime(self, meta):
        # collect info about start time
        master_process = psutil.Process(self.object.pid)
        meta['start_time'] = int(master_process.create_time()) * 1000

    def open_ssl(self, meta):
        """Old nginx uses standart openssl library - find its version"""
        if not meta['ssl']:
            openssl_out, _ = subp.call("dpkg -l | grep openssl")
            for line in openssl_out:
                gwe = re.match('([\d\w]+)\s+([\d\w\.\-]+)\s+([\d\w\.\-\+_~]+)\s', line)
                if gwe:
                    if gwe.group(2).startswith('openssl'):
                        meta['ssl'] = {
                            'built': [gwe.group(2), gwe.group(3)],
                            'run': [gwe.group(2), gwe.group(3)],
                        }
