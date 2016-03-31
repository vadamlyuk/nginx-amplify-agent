# -*- coding: utf-8 -*-
import traceback
import os
import requests

from amplify.agent.context import context
from amplify.agent.util.loader import import_class

__author__ = "Mike Belov"
__copyright__ = "Copyright (C) Nginx, Inc. All rights reserved."
__credits__ = ["Mike Belov", "Andrei Belov", "Ivan Poluyanov", "Oleg Mamontov", "Andrew Alexeev", "Grant Hulegaard"]
__license__ = ""
__maintainer__ = "Mike Belov"
__email__ = "dedm@nginx.com"

CONFIG_CACHE = {}


def read(config_name, config_file=None):
    """
    Reads specified config and caches it in CONFIG_CACHE dict

    Each config is a python file which can
    Typical name of config for example: /agent/config/app.py

    :param config_name: str config name
    :param config_file: str config file name
    :return: python object
    """
    if config_name not in CONFIG_CACHE:
        full_module_name = 'amplify.agent.config.%s' % config_name
        class_name = '%sConfig' % context.environment.title()
        config_object = import_class('%s.%s' % (full_module_name, class_name))(config_file)
        CONFIG_CACHE[config_name] = config_object

    return CONFIG_CACHE[config_name]


def test(config_file, pid_file):
    print('')

    try:
        # check that config file exists
        if not os.path.isfile(config_file) or not os.access(config_file, os.R_OK):
            print("\033[31mConfig file %s could not be found or opened.\033[0m\n" % config_file)
            print("If you installed the agent from the package you should do the following actions:")
            print("  1. sudo cp /etc/amplify-agent/agent.conf.default /etc/amplify-agent/agent.conf")
            print("  2. sudo chown nginx /etc/amplify-agent/agent.conf")
            print("  3. write your API key in [credentials][api_key]")
            return 1

        # check it can be loaded
        from amplify.agent.context import context
        context.setup(
            app='agent',
            config_file=config_file,
            pid_file=pid_file
        )

        # check that it contain needed stuff
        if not context.app_config['cloud']['api_url']:
            print("\033[31mAPI url is not specified in %s\033[0m\n" % config_file)
            print("Write API url https://receiver.amplify.nginx.com:443/1.0 in [cloud][api_url]")
            return 1

        if not context.app_config['credentials']['api_key']:
            print("\033[31mAPI key is not specified in %s\033[0m\n" % config_file)
            print("Write your API key in [credentials][api_key]")
            return 1

        # test logger
        try:
            context.log.info('configtest check')
        except:
            print("\033[31mCould not write to log\033[0m\n")
            print("Maybe the log folder doestn't exist or rights are broken")
            print("You should do the following actions:")
            print("  1. sudo mkdir /var/log/amplify-agent")
            print("  2. sudo touch /var/log/amplify-agent/agent.log")
            print("  3. sudo chown nginx /var/log/amplify-agent/agent.log")
            return 1

        # try to connect to the cloud
        try:
            context.http_client.post('agent/', {})
        except requests.HTTPError as e:
            api_url = context.app_config['cloud']['api_url']
            print("\033[31mCould not connect to cloud via url %s\033[0m" % api_url)

            if e.response.status_code == 404:
                api_key = context.app_config['credentials']['api_key']
                print("\033[31mIt seems like your API key '%s' is wrong. \033[0m\n" % api_key)
            else:
                print("\033[31mIt seems like we have little problems at our side. \nApologies and bear with us\033[0m\n")

            return 1
    except:
        print("\033[31mSomething failed:\033[0m\n")
        print(traceback.format_exc())
        return 1

    print("\033[32mConfig file %s is OK\033[0m" % config_file)
    return 0
