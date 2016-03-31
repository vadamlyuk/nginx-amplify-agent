#!/usr/bin/python
# -*- coding: utf-8 -*-
import os

from optparse import OptionParser, Option

from builders.util import shell_call, color_print


__author__ = "Mike Belov"
__copyright__ = "Copyright (C) Nginx, Inc. All rights reserved."
__credits__ = ["Mike Belov", "Andrei Belov", "Ivan Poluyanov", "Oleg Mamontov", "Andrew Alexeev"]
__license__ = ""
__maintainer__ = "Mike Belov"
__email__ = "dedm@nginx.com"


def rm_and_build(folder, name):
    shell_call('docker rm %s' % name, terminal=True)
    shell_call('docker build -t %s docker/%s' % (name, folder), terminal=True)

supported_os = ['ubuntu1404', 'ubuntu1404-plus', 'ubuntu1004', 'debian8', 'centos6', 'centos7']

usage = "usage: %prog -h"

option_list = (
    Option(
        '--rebuild',
        action='store_true',
        dest='rebuild',
        help='Rebuild before run (False by default)',
        default=False,
    ),
    Option(
        '--drop',
        action='store_true',
        dest='drop',
        help='Drop everything before run (False by default)',
        default=False,
    ),
    Option(
        '--background',
        action='store_true',
        dest='background',
        help='Run in background (False by default)',
        default=False,
    ),
    Option(
        '--shell',
        action='store_true',
        dest='shell',
        help='Get shell access',
        default=False,
    ),
    Option(
        '--os',
        action='store',
        dest='os',
        type='string',
        help='OS from %s. Default is %s' % (supported_os, supported_os[0]),
        default=supported_os[0],
    ),
    Option(
        '--all',
        action='store_true',
        dest='all',
        help='Runs all agent images! Watch for CPU!',
        default=False
    ),
)

parser = OptionParser(usage, option_list=option_list)
(options, args) = parser.parse_args()

if __name__ == '__main__':
    shell_call('find . -name "*.pyc" -type f -delete')

    if options.drop:
        shell_call('docker stop $(docker ps -a -q)')
        shell_call('docker rm $(docker ps -a -q)')

    if options.all:
        shell_call('docker-compose -f docker/agents.yml stop', terminal=True)

        if options.rebuild:
            for osname in supported_os:
                rm_and_build('%s' % osname, 'amplify-agent-%s' % osname)

        runcmd = 'docker-compose -f docker/agents.yml up'
    else:
        shell_call('docker-compose -f docker/%s.yml stop' % options.os, terminal=True)

        if options.rebuild:
            rm_and_build('%s' % options.os, 'amplify-agent-%s' % options.os)

        if options.shell:
            rows, columns = os.popen('stty size', 'r').read().split()
            color_print("\n= USEFUL COMMANDS =" + "="*(int(columns)-20))
            for helper in (
                "nginx",
                "python /amplify/nginx-amplify-agent.py start --config=/amplify/etc/agent.conf.development",
                "python /amplify/nginx-amplify-agent.py stop --config=/amplify/etc/agent.conf.development"
            ):
                color_print(helper, color='yellow')
            color_print("="*int(columns)+"\n")

            runcmd = 'docker-compose -f docker/%s.yml run agent bash' % options.os
        else:
            runcmd = 'docker-compose -f docker/%s.yml up' % options.os

    if options.background and not options.shell:
        runcmd += ' -d'

    shell_call(runcmd, terminal=True)
