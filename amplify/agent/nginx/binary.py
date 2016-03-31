# -*- coding: utf-8 -*-
import re

from amplify.agent.util import subp

__author__ = "Mike Belov"
__copyright__ = "Copyright (C) Nginx, Inc. All rights reserved."
__credits__ = ["Mike Belov", "Andrei Belov", "Ivan Poluyanov", "Oleg Mamontov", "Andrew Alexeev"]
__license__ = ""
__maintainer__ = "Mike Belov"
__email__ = "dedm@nginx.com"


DEFAULT_PREFIX = '/usr/local/nginx'
DEFAULT_CONFPATH = 'conf/nginx.conf'


def nginx_v(path_to_binary):
    """
    call -V and parse results

    :param path_to_binary str - path to binary
    :return {} - see result
    """

    result = {
        'version': None,
        'plus': {'enabled': False, 'release': None},
        'ssl': {'built': None, 'run': None},
        'configure': {}
    }

    _, nginx_v_err = subp.call("%s -V" % path_to_binary)
    for line in nginx_v_err:
        # SSL stuff
        if line.lower().startswith('built with') and 'ssl' in line.lower():
            parts = line.split(' ')
            lib_name, lib_version = parts[2:4]
            result['ssl'] = {
                'built': [lib_name, lib_version],
                'run': [lib_name, lib_version],
            }

        if line.lower().startswith('run with') and 'ssl' in line.lower():
            parts = line.split(' ')
            lib_name, lib_version = parts[2:4]
            result['ssl']['run'] = [lib_name, lib_version]

        parts = line.split(':')
        if len(parts) < 2:
            continue

        # parse version
        key, value = parts
        if key == 'nginx version':
            # parse major version
            major_parsed = re.match('.*/([\d\w\.]+)', value)
            result['version'] = major_parsed.group(1) if major_parsed else value.lstrip()

            # parse plus version
            if 'plus' in value:
                plus_parsed = re.match('.*\(([\w\-]+)\).*', value)
                if plus_parsed:
                    result['plus']['enabled'] = True
                    result['plus']['release'] = plus_parsed.group(1)

        # parse configure
        if key == 'configure arguments':
            arguments = _parse_arguments(value)
            result['configure'] = arguments

    return result


def get_prefix_and_conf_path(cmd, configure=None):
    """
    Finds prefix and path to config based on running cmd and optional configure args

    :param running_binary_cmd: full cmd from ps
    :param configure: parsed configure args from nginx -V
    :return: prefix, conf_path
    """

    # find bin path
    cmd = cmd.replace('nginx: master process ', '')
    params = filter(lambda x: x != '', cmd.split(' '))
    bin_path = params.pop(0)

    # parse nginx -V
    if configure is None:
        configure = nginx_v(bin_path)['configure']

    # parse running cmd - try to find config and prefix
    conf_path = None
    prefix = None
    for i in xrange(len(params)):
        value = params[i]
        if value == '-c':
            conf_path = params[i + 1]
        elif value == '-p':
            prefix = params[i + 1]

    # if prefix was not found in cmd - try to read it from configure args
    # if there is no key "prefix" in args, then use default
    if not prefix:
        if 'prefix' in configure:
            prefix = configure['prefix']
        else:
            prefix = DEFAULT_PREFIX

    if not conf_path:
        if 'conf-path' in configure:
            conf_path = configure['conf-path']
        else:
            conf_path = DEFAULT_CONFPATH

    # start processing conf_path
    # if it has not an absolutely path, then we should add prefix to it

    if not conf_path.startswith('/'):
        conf_path = '%s/%s' % (prefix, conf_path)

    return bin_path, prefix, conf_path, nginx_v(bin_path)['version']


def _parse_arguments(argstring):
    """
    Parses argstring from nginx -V

    :param argstring: configure string
    :return: {} of parsed string
    """
    arguments = {}

    current_key = None
    current_value = None

    for part in argstring.split(' --'):
        if '=' in part:
            # next part of compound
            if current_key and current_value:
                current_value += part
                if part.endswith("'"):
                    arguments[current_key] = current_value
                    current_key = None
                    current_value = None
            else:
                k, v = part.split('=', 1)
                # compound argument
                if v.startswith("'") and v.endswith("'"):
                    arguments[k] = v
                elif v.startswith("'"):
                    current_key = k
                    current_value = v
                # simple argument
                else:
                    arguments[k] = v
        else:
            # boolean
            if part:
                arguments[part] = True

    return arguments
