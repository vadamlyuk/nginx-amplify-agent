#!/usr/bin/python
# -*- coding: utf-8 -*-
import json
import os
import sys
from optparse import OptionParser, Option

sys.path.append(os.getcwd())  # to make amplify libs available

from amplify.agent.context import context
context.setup(
    app='agent',
    config_file='etc/agent.conf.development',
)

from amplify.agent.nginx.config.config import NginxConfig


__author__ = "Mike Belov"
__copyright__ = "Copyright (C) Nginx, Inc. All rights reserved."
__credits__ = ["Mike Belov", "Andrei Belov", "Ivan Poluyanov", "Oleg Mamontov", "Andrew Alexeev", "Grant Hulegaard"]
__license__ = ""
__maintainer__ = "Mike Belov"
__email__ = "dedm@nginx.com"

usage = "usage: sudo -u nginx %prog -h"

option_list = (
    Option(
        '-c', '--config',
        action='store',
        dest='config',
        type='string',
        help='path to config'
    ),
    Option(
        '--light',
        action='store_true',
        dest='light',
        help='light parse (find all files)',
        default=False,
    ),
    Option(
        '--pretty',
        action='store_true',
        dest='pretty',
        help='pretty print',
        default=False,
    ),

)

parser = OptionParser(usage, option_list=option_list)
(options, args) = parser.parse_args()


if __name__ == '__main__':
    if not options.config:
        parser.print_help()
        sys.exit(1)

    if options.config.startswith('~'):
        filename = options.config.replace('~', os.path.expanduser('~'))
    elif not options.config.startswith('/'):
        filename = os.getcwd() + '/' + options.config
    else:
        filename = options.config

    cfg = NginxConfig(filename=filename)
    print_args = dict(indent=4, sort_keys=True) if options.pretty else dict()

    if not options.light:
        cfg.full_parse()

        print('\033[32mConfig tree for %s\033[0m' % filename)
        print(json.dumps(cfg.tree, **print_args))

        print('\n\033[32mConfig index for %s\033[0m' % filename)
        print(json.dumps(cfg.index, **print_args))

        print('\n\033[32mConfig files for %s\033[0m' % filename)
        print(json.dumps(cfg.files, **print_args))

        print('\n\033[32mStub/plus status %s\033[0m' % filename)
        print(json.dumps(cfg.stub_status_urls, **print_args))
        print(json.dumps(cfg.plus_status_external_urls, **print_args))
        print(json.dumps(cfg.plus_status_internal_urls, **print_args))

        print('\n\033[32mAccess logs %s\033[0m' % filename)
        print(json.dumps(cfg.access_logs, **print_args))

        print('\n\033[32mError logs %s\033[0m' % filename)
        print(json.dumps(cfg.error_logs, **print_args))

        print('\n\033[32mLog formats %s\033[0m' % filename)
        print(json.dumps(cfg.log_formats, **print_args))

        print('\n\033[32mConfig errors for %s\033[0m' % filename)
        print(json.dumps(cfg.parser_errors, **print_args))
    else:
        print('\n\033[32mLight parse results for %s\033[0m' % filename)
        print(json.dumps(cfg.get_all_files(), **print_args))
