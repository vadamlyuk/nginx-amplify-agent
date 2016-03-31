# -*- coding: utf-8 -*-
import subprocess
import sys
import os

__author__ = "Mike Belov"
__copyright__ = "Copyright (C) Nginx, Inc. All rights reserved."
__credits__ = ["Mike Belov", "Andrei Belov", "Ivan Poluyanov", "Oleg Mamontov", "Andrew Alexeev", "Grant Hulegaard"]
__license__ = ""
__maintainer__ = "Mike Belov"
__email__ = "dedm@nginx.com"


def color_print(message, color='green'):
    if color == 'red':
        template = '\033[31m%s\033[0m'
    elif color == 'green':
        template = '\033[32m%s\033[0m'
    elif color == 'yellow':
        template = '\033[33m%s\033[0m'
    print (template % message)


def shell_call(cmd, terminal=False, important=True):
    """
    Runs shell command

    :param cmd: ready-to-run command
    :param terminal: uses os.system to run instead of process
    :param important: stops the script if shell command returns non-zero exit code
    :return:
    """
    print('\033[32m%s\033[0m' % cmd)

    if terminal:
        os.system(cmd)
    else:
        process = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        results, errors = process.communicate()

        # print normal results
        for line in results.split('\n'):
            if line:
                print(line)

        # print warnings and errors
        sys.stdout.write('\033[33m')
        for line in errors.split('\n'):
            if line:
                print(line)
        sys.stdout.write('\033[0m')
        print('')

        # check
        process.wait()
        if important and process.returncode != 0:
            print('\033[31mFAILED!\033[0m')
            sys.exit(1)
        else:
            return results


def get_version_and_build():
    with open('packages/version', 'r') as f:
        version, build = f.readline().split('-')
        return '%.2f' % float(version), int(build)


def change_first_line(filename, first_line):
    with open(filename, 'r+') as f:
        lines = f.readlines()
        lines[0] = first_line
        lines.insert(1, "\n")
        f.seek(0)
        f.writelines(lines)


def install_pip(python='python'):
    shell_call('wget --no-check-certificate https://bootstrap.pypa.io/get-pip.py')
    shell_call('%s get-pip.py --user' % python)

    # next lines are for "error: invalid command 'bdist_wheel'" on centos6
    shell_call('~/.local/bin/pip install setuptools --user')
    shell_call('~/.local/bin/pip install setuptools --upgrade --user')


def install_pip_deps():
    shell_call('~/.local/bin/pip install --upgrade --target=amplify --no-compile -r packages/requirements')
