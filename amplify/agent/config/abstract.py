# -*- coding: utf-8 -*-
import ConfigParser

__author__ = "Mike Belov"
__copyright__ = "Copyright (C) Nginx, Inc. All rights reserved."
__credits__ = ["Mike Belov"]
__license__ = ""
__maintainer__ = "Mike Belov"
__email__ = "dedm@nginx.com"


class AbstractConfig(object):
    filename = None
    write_new = False
    config = None

    def __init__(self, config_file=None):
        self.from_file = None
        self.unchangeable = set()
        if config_file:
            self.filename = config_file
        if self.filename:
            self.load()

    def mark_unchangeable(self, key):
        self.unchangeable.add(key)

    def load(self):
        """
        Loads config from file and updates it
        """
        self.from_file = ConfigParser.RawConfigParser()
        self.from_file.read(self.filename)

        patch = {}
        for section in self.from_file.sections():
            patch[section] = {}
            for (key, value) in self.from_file.items(section):
                patch[section][key] = value

        self.apply(patch)

    def save(self, section, key, value):
        self.config[section][key] = value
        if self.write_new:
            self.from_file.set(section, key, value)
            with open(self.filename, 'wb') as configfile:
                self.from_file.write(configfile)

    def get(self, section, default=None):
        if default is None:
            default = {}
        return self.config.get(section, default)

    def __getitem__(self, item):
        return self.config[item]

    def apply(self, patch, current=None):
        """
        Recursevly applies changes to config and return amount of changes

        :param patch: patches to config
        :param current: current tree
        :return: amount of changes
        """
        changes = 0

        if current is None:
            current = self.config

        for k, v in patch.iteritems():
            if k in current:
                if isinstance(v, dict) and isinstance(current[k], dict):
                    changes += self.apply(v, current[k])
                elif v and v != current[k] and k not in self.unchangeable:
                    changes += 1
                    current[k] = v
            else:
                changes += 1
                current[k] = v

        return changes
