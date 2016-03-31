# -*- coding: utf-8 -*-
from collections import defaultdict

__author__ = "Mike Belov"
__copyright__ = "Copyright (C) Nginx, Inc. All rights reserved."
__credits__ = ["Mike Belov", "Andrei Belov", "Ivan Poluyanov", "Oleg Mamontov", "Andrew Alexeev"]
__license__ = ""
__maintainer__ = "Mike Belov"
__email__ = "dedm@nginx.com"


class Singleton(object):
    _instance = None

    def __new__(cls):
        if not cls._instance:
            cls._instance = super(Singleton, cls).__new__(cls)
        return cls._instance


class CommonDataTank(Singleton):
    def __init__(self):
        self.clients = defaultdict(dict)

    def register(self, type, object_id, client):
        """
        Registers some client
        :param type: object type (prefix)
        :param object_id: object id
        :param client: some client
        """
        self.clients[type][object_id] = client

    def unregister(self, type, object_id):
        """
        Unregisters client
        :param type: object type (prefix)
        :param object_id: object id
        """
        del self.clients[type][object_id]

    def flush(self, type):
        result = {}
        for object_id, client in self.clients.get(type, {}).iteritems():
            data = client.flush()
            if data:
                result[object_id] = data
        return result


class CommonDataClient(object):
    def __init__(self, object=None):
        self.object = object
        self.current = {}
        self.delivery = {}
