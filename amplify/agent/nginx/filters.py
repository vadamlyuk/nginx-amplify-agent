# -*- coding: utf-8 -*-
import re

__author__ = "Mike Belov"
__copyright__ = "Copyright (C) Nginx, Inc. All rights reserved."
__credits__ = ["Mike Belov", "Andrei Belov", "Ivan Poluyanov", "Oleg Mamontov", "Andrew Alexeev", "Grant Hulegaard"]
__license__ = ""
__maintainer__ = "Mike Belov"
__email__ = "dedm@nginx.com"


RE_TYPE = type(re.compile('amplify'))


class Filter(object):
    def __init__(self, data=None, metric=None, filter_rule_id=None):
        self.metric = metric
        self.filter_rule_id = filter_rule_id
        self.filename = None
        self.data = {}

        # pre-process some vars
        data = data if data else []

        # normalize them
        for raw_filter in data:
            for k, v in raw_filter.iteritems():
                if k == 'filename':
                    self.filename = v
                else:
                    if k == '$request_method':
                        normalized_value = v.upper()
                    else:
                        normalized_value = v

                    if '*' in normalized_value:
                        template = re.escape(normalized_value).replace("\\*", ".*")
                        normalized_value = re.compile(template)

                    normalized_key = k.replace('$', '')
                    self.data[normalized_key] = normalized_value

        self.empty = False if (self.data or self.filename) else True

    def match(self, parsed):
        """
        Checks if a parsed string matches a filter
        :param parsed: {} of parsed string
        :return: True of False
        """
        result = True
        for filter_key, filter_value in self.data.iteritems():
            key_matched = False
            if filter_key in parsed:
                value = parsed[filter_key]
                if isinstance(filter_value, str) and filter_value == value:
                    key_matched = True
                elif isinstance(filter_value, RE_TYPE) and re.match(filter_value, value):
                    key_matched = True
            if not key_matched:
                result = False
                break
        return result
