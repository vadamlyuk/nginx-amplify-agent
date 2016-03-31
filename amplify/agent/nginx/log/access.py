# -*- coding: utf-8 -*-
import re

from amplify.agent.util.escape import prep_raw


__author__ = "Mike Belov"
__copyright__ = "Copyright (C) Nginx, Inc. All rights reserved."
__credits__ = ["Mike Belov", "Andrei Belov", "Ivan Poluyanov", "Oleg Mamontov", "Andrew Alexeev", "Grant Hulegaard"]
__license__ = ""
__maintainer__ = "Mike Belov"
__email__ = "dedm@nginx.com"


REQUEST_RE = re.compile(r'(?P<request_method>[A-Z]+) (?P<request_uri>/.*) (?P<server_protocol>.+)')


class NginxAccessLogParser(object):
    """
    Nginx access log parser
    """
    short_name = 'nginx_alog'

    combined_format = '$remote_addr - $remote_user [$time_local] "$request" ' + \
                      '$status $body_bytes_sent "$http_referer" "$http_user_agent"'

    default_variable = ['.+', str]

    common_variables = {
        'request': ['.+', str],
        'body_bytes_sent': ['\d+', int],
        'bytes_sent': ['\d+', int],
        'connection': ['[\d\s]+', str],
        'connection_requests': ['\d+', int],
        'msec': ['.+', float],
        'pipe': ['[p|\.]', str],
        'request_length': ['\d+', int],
        'request_time': ['.+', str],
        'status': ['\d+', str],
        'time_iso8601': ['.+', str],
        'time_local': ['.+', str],
        'upstream_response_time': ['.+', str],
        'upstream_response_length': ['\d+', int],
        'upstream_connect_time': ['.+', str],
        'upstream_header_time': ['.+', str],
        'upstream_status': ['.+', str],
        'upstream_cache_status': ['.+', str],
        'gzip_ratio': ['.+', float],
    }

    request_variables = {
        'request_method': ['[A-Z]+', str],
        'request_uri': ['/.*', str],
        'server_protocol': ['[\d\.]+', str],
    }

    def __init__(self, raw_format=None):
        """
        Takes raw format and generates regex
        :param raw_format: raw log format
        """
        self.raw_format = self.combined_format if raw_format is None else raw_format

        self.keys = []
        self.regex_string = r''
        self.regex = None
        current_key = None

        self.raw_format = prep_raw(self.raw_format)

        def finalize_key():
            key_without_dollar = current_key[1:]
            self.keys.append(key_without_dollar)
            rxp = self.common_variables.get(key_without_dollar, self.default_variable)[0]
            # Handle formats with multiple instances of the same variable.
            var_count = self.keys.count(key_without_dollar)
            if var_count > 1:  # Duplicate variables will be named starting at 2 (var, var2, var3, etc...)
                regex_var_name = '%s%s' % (key_without_dollar, var_count)
            else:
                regex_var_name = key_without_dollar
            self.regex_string += '(?P<%s>%s)' % (regex_var_name, rxp)

        for char in self.raw_format:
            if current_key:
                # if there's a current key
                if char.isalpha() or char == '_':
                    # continue building key
                    current_key += char
                else:
                    # finalize current_key
                    finalize_key()

                    if char == '$':
                        # if there's a new key - create it
                        current_key = char
                    else:
                        # otherwise - add char to regex
                        current_key = None
                        if char.isalpha() or char.isdigit():
                            self.regex_string += char
                        else:
                            self.regex_string += '\%s' % char
            else:
                # if there's no current key
                if char == '$':
                    current_key = char
                else:
                    if char.isalpha() or char.isdigit():
                        self.regex_string += char
                    else:
                        self.regex_string += '\%s' % char

        # key can be the last one element in a string
        if current_key:
            finalize_key()

        self.regex = re.compile(self.regex_string)

    def parse(self, line):
        """
        Parses the line and if there are some special fields - parse them too
        For example we can get HTTP method and HTTP version from request

        :param line: log line
        :return: dict with parsed info
        """
        result = {'malformed': False}

        # parse the line
        common = self.regex.match(line)
        if common:
            for key in self.keys:  # TODO: Remove extra processing by using a set of keys.
                func = self.common_variables.get(key, self.default_variable)[1]
                try:
                    value = func(common.group(key))
                # for example gzip ratio can be '-' and float
                except ValueError:
                    value = 0

                # time variables should be parsed to array of float
                if key.endswith('_time'):
                    # skip empty vars
                    if value != '-':
                        array_value = []
                        for x in value.replace(' ', '').split(','):
                            x = float(x)
                            # workaround for an old nginx bug with time. ask lonerr@ for details
                            if x > 10000000:
                                continue
                            else:
                                array_value.append(x)
                        if array_value:
                            result[key] = array_value
                else:
                    result[key] = value

        # parse sub fields
        if 'request' in result:
            req = REQUEST_RE.match(result['request'])
            if req:
                for req_key in self.request_variables.iterkeys():
                    result[req_key] = req.group(req_key)
            else:
                result['malformed'] = True

        return result
