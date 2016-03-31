# -*- coding: utf-8 -*-
import copy
import hashlib
import os
import re
import time
import rstr

from amplify.agent.context import context
from amplify.agent.nginx.config.parser import NginxConfigParser
from amplify.agent.util import subp
from amplify.agent.util.ssl import ssl_analysis

__author__ = "Mike Belov"
__copyright__ = "Copyright (C) Nginx, Inc. All rights reserved."
__credits__ = ["Mike Belov", "Andrei Belov", "Ivan Poluyanov", "Oleg Mamontov", "Andrew Alexeev", "Grant Hulegaard"]
__license__ = ""
__maintainer__ = "Mike Belov"
__email__ = "dedm@nginx.com"


ERROR_LOG_LEVELS = (
    'debug',
    'info',
    'notice',
    'warn',
    'error',
    'crit',
    'alert',
    'emerg'
)


class NginxConfig(object):
    """
    Nginx config representation
    Parses configs with all includes, etc

    Main tasks:
    - find all log formats
    - find all access logs
    - find all error logs
    - find stub_status url
    """

    def __init__(self, filename, binary=None, prefix=None):
        self.filename = filename
        self.binary = binary
        self.prefix = prefix
        self.log_formats = {}
        self.access_logs = {}
        self.error_logs = {}
        self.test_errors = []
        self.tree = {}
        self.files = {}
        self.index = []
        self.ssl_certificates = {}
        self.parser_errors = []
        self.stub_status_urls = []
        self.plus_status_external_urls = []
        self.plus_status_internal_urls = []
        self.parser = NginxConfigParser(filename)

    def full_parse(self):
        context.log.debug('parsing full tree of %s' % self.filename)

        # parse raw data
        self.parser.parse()

        self.tree = self.parser.tree
        self.files = self.parser.files
        self.index = self.parser.index
        self.parser_errors = self.parser.errors

        # go through and collect all logical data
        self.__recursive_search(subtree=self.parser.simplify())

        # try to locate and use default logs (PREFIX/logs/*)
        self.add_default_logs()

    def get_all_files(self, include_ssl_certs=False):
        """
        Goes through all files (light-parsed includes) and collects their mtime
        :param include_ssl_certs: bool - include ssl certs  or not
        :return: {} - dict of files
        """
        files = self.parser.collect_all_files(include_ssl_certs=include_ssl_certs)
        context.log.debug('found %s files for %s' % (len(files.keys()), self.filename))
        return files

    def total_size(self):
        """
        Returns the total size of a config tree
        :return: int size in bytes
        """
        result = 0
        for file_name, file_info in self.files.iteritems():
            result += file_info['size']
        return result

    def __recursive_search(self, subtree=None, ctx=None):
        """
        Searches needed data in config's tree

        :param subtree: dict with tree to parse
        :param ctx: dict with context
        """
        ctx = ctx if ctx is not None else {}
        subtree = subtree if subtree is not None else {}

        for key, value in subtree.iteritems():
            if key == 'error_log':
                error_logs = value if isinstance(value, list) else [value]
                for er_log_definition in error_logs:
                    if er_log_definition == 'off':
                        continue

                    split_er_log_definition = er_log_definition.split(' ')
                    log_name = split_er_log_definition[0]
                    log_level = split_er_log_definition[-1] \
                        if split_er_log_definition[-1] in ERROR_LOG_LEVELS else 'error'  # nginx default log level
                    log_name = re.sub('[\'"]', '', log_name)  # remove all ' and "
                    if log_name.startswith('syslog'):
                        continue
                    elif not log_name.startswith('/'):
                        log_name = '%s/%s' % (self.prefix, log_name)

                    if log_name not in self.error_logs:
                        self.error_logs[log_name] = log_level
            elif key == 'access_log':
                access_logs = value if isinstance(value, list) else [value]
                for ac_log_definition in access_logs:
                    if ac_log_definition == 'off':
                        continue

                    parts = filter(lambda x: x, ac_log_definition.split(' '))
                    log_format = None if len(parts) == 1 else parts[1]
                    log_name = parts[0]
                    log_name = re.sub('[\'"]', '', log_name)  # remove all ' and "

                    if log_name.startswith('syslog'):
                        continue
                    elif not log_name.startswith('/'):
                        log_name = '%s/%s' % (self.prefix, log_name)

                    self.access_logs[log_name] = log_format
            elif key == 'log_format':
                for k, v in value.iteritems():
                    self.log_formats[k] = v
            elif key == 'server' and isinstance(value, list) and 'upstream' not in ctx:
                for server in value:

                    current_ctx = copy.copy(ctx)
                    if server.get('listen') is None:
                        # if no listens specified, then use default *:80 and *:8000
                        listen = ['80', '8000']
                    else:
                        listen = server.get('listen')
                    listen = listen if isinstance(listen, list) else [listen]

                    ctx['ip_port'] = []
                    for item in listen:
                        listen_first_part = item.split(' ')[0]
                        addr, port = self.__parse_listen(listen_first_part)
                        if addr in ('*', '0.0.0.0'):
                            addr = '127.0.0.1'
                        elif addr == '[::]':
                            addr = '[::1]'
                        ctx['ip_port'].append((addr, port))

                    if 'server_name' in server:
                        ctx['server_name'] = server.get('server_name')

                    self.__recursive_search(subtree=server, ctx=ctx)
                    ctx = current_ctx
            elif key == 'upstream':
                for upstream, upstream_info in value.iteritems():
                    current_ctx = copy.copy(ctx)
                    ctx['upstream'] = upstream
                    self.__recursive_search(subtree=upstream_info, ctx=ctx)
                    ctx = current_ctx
            elif key == 'location':
                for location, location_info in value.iteritems():
                    current_ctx = copy.copy(ctx)
                    ctx['location'] = location
                    self.__recursive_search(subtree=location_info, ctx=ctx)
                    ctx = current_ctx
            elif key == 'stub_status' and ctx and 'ip_port' in ctx:
                for url in self.__status_url(ctx):
                    if url not in self.stub_status_urls:
                        self.stub_status_urls.append(url)
            elif key == 'status' and ctx and 'ip_port' in ctx:
                # use different url builders for external and internal urls
                for url in self.__status_url(ctx, server_preferred=True):
                    if url not in self.plus_status_external_urls:
                        self.plus_status_external_urls.append(url)

                # for internal (agent) usage local ip address is a better choice,
                # because the external url might not be accessible from a host
                for url in self.__status_url(ctx, server_preferred=False):
                    if url not in self.plus_status_internal_urls:
                        self.plus_status_internal_urls.append(url)
            elif isinstance(value, dict):
                self.__recursive_search(subtree=value, ctx=ctx)
            elif isinstance(value, list):
                for next_subtree in value:
                    if isinstance(next_subtree, dict):
                        self.__recursive_search(subtree=next_subtree, ctx=ctx)

    @staticmethod
    def __status_url(ctx, server_preferred=False):
        """
        Creates stub/plus status url based on context

        :param ctx: {} of current parsing context
        :param server_preferred: bool - use server_name instead of listen
        :return: [] of urls
        """
        results = []
        location = ctx.get('location', '/')

        # remove all modifiers
        location_parts = location.split(' ')
        final_location_part = location_parts[-1]

        # generate a random sting that will fit regex location
        if location.startswith('~'):
            try:
                exact_location = rstr.xeger(final_location_part)

                # check that regex location has / and add it
                if not exact_location.startswith('/'):
                    exact_location = '/%s' % exact_location
            except:
                context.log.debug('bad regex location: %s' % final_location_part)
                exact_location = None
        else:
            exact_location = final_location_part

            # if an exact location doesn't have / that's not a working location, we should not use it
            if not exact_location.startswith('/'):
                context.log.debug('bad exact location: %s' % final_location_part)
                exact_location = None

        if exact_location:
            for ip_port in ctx.get('ip_port'):
                address, port = ip_port
                if server_preferred and 'server_name' in ctx:
                    if isinstance(ctx['server_name'], list):
                        address = ctx['server_name'][0].split(' ')[0]
                    else:
                        address = ctx['server_name'].split(' ')[0]

                results.append('%s:%s%s' % (address, port, exact_location))

        return results

    def run_test(self):
        """
        Tests the configuration using nginx -t
        Saves event info if syntax check was not successful
        """
        start_time = time.time()
        context.log.info('running %s -t -c %s' % (self.binary, self.filename))
        if self.binary:
            try:
                _, nginx_t_err = subp.call("%s -t -c %s" % (self.binary, self.filename), check=False)
                for line in nginx_t_err:
                    if 'syntax is' in line and 'syntax is ok' not in line:
                        self.test_errors.append(line)
            except Exception as e:
                exception_name = e.__class__.__name__
                context.log.error('failed to %s -t -c %s due to %s' % (self.binary, self.filename, exception_name))
                context.log.debug('additional info:', exc_info=True)
        end_time = time.time()
        return end_time - start_time

    def checksum(self):
        """
        Calculates total checksum of all config files, certificates and permissions

        :return: str checksum
        """
        checksums = []
        for filename in self.files.iterkeys():
            checksums.append(hashlib.sha256(open(filename).read()).hexdigest())
        for cert in self.ssl_certificates.iterkeys():
            checksums.append(hashlib.sha256(open(cert).read()).hexdigest())
        return hashlib.sha256('.'.join(checksums)).hexdigest()

    def __parse_listen(self, listen):
        """
        Parses listen directive value and return ip:port string, like *:80 and so on

        :param listen: str raw listen
        :return: str ip:port
        """
        if '[' in listen:
            # ipv6
            addr_port_parts = filter(lambda x: x, listen.rsplit(']', 1))
            address = '%s]' % addr_port_parts[0]

            if len(addr_port_parts) == 1:  # only address specified, add default 80
                return address, '80'
            else:  # get port
                bracket, port = addr_port_parts[1].split(':')
                return address, port
        else:
            # ipv4
            addr_port_parts = filter(lambda x: x, listen.rsplit(':', 1))

            if len(addr_port_parts) == 1:
                # can be address or port only
                is_port = addr_port_parts[0].isdigit()
                if is_port:  # port!
                    port = addr_port_parts[0]
                    return '*', port
                else:  # it was address only, add default 80
                    address = addr_port_parts[0]
                    return address, '80'
            else:
                address, port = addr_port_parts
                return address, port

    def add_default_logs(self):
        """
        By default nginx uses logs placed in --prefix/logs/ directory
        This method tries to find and add them
        """
        access_log_path = '%s/logs/access.log' % self.prefix
        if os.path.isfile(access_log_path) and access_log_path not in self.access_logs:
            self.access_logs[access_log_path] = None

        error_log_path = '%s/logs/error.log' % self.prefix
        if os.path.isfile(error_log_path) and error_log_path not in self.error_logs:
            self.error_logs[error_log_path] = 'error'

    def run_ssl_analysis(self):
        """
        Iterate over a list of ssl_certificate definitions and run ssl_analysis to construct a dictionary with
        ssl_certificate value paired with results fo ssl_analysis.

        :return: float run time
        """
        if not self.parser.ssl_certificates:
            return

        start_time = time.time()

        for cert_filename in set(self.parser.ssl_certificates):
            if cert_filename not in self.ssl_certificates:
                ssl_analysis_result = ssl_analysis(cert_filename)
                if ssl_analysis_result:
                    self.ssl_certificates[cert_filename] = ssl_analysis_result

        end_time = time.time()
        return end_time - start_time
