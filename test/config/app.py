# -*- coding: utf-8 -*-
from test.fixtures.defaults import *
from amplify.agent.config.app import Config

__author__ = "Mike Belov"
__copyright__ = "Copyright (C) Nginx, Inc. All rights reserved."
__credits__ = ["Mike Belov", "Andrei Belov", "Ivan Poluyanov", "Oleg Mamontov", "Andrew Alexeev"]
__license__ = ""
__maintainer__ = "Mike Belov"
__email__ = "dedm@nginx.com"


class TestingConfig(Config):
    filename = 'etc/agent.conf.testing'
    write_new = True

    config_changes = dict(
        cloud=dict(
            api_url=DEFAULT_API_URL,
            verify_ssl_cert=False,
        ),
        credentials=dict(
            uuid=DEFAULT_UUID,
            api_key=DEFAULT_API_KEY,
            hostname=DEFAULT_HOST
        ),
        containers=dict(
            system=dict(
                poll_intervals=dict(
                    discover=10.0,
                    meta=30.0,
                    metrics=20.0,
                    logs=10.0
                )
            ),
            nginx=dict(
                upload_config=True,
                run_test=True,
                max_test_duration=10.0,
                upload_ssl=True,
                poll_intervals=dict(
                    discover=10.0,
                    meta=30.0,
                    metrics=20.0,
                    logs=10.0,
                    configs=10.0,
                ),
            )
        )
    )
