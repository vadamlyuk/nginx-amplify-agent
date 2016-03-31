# -*- coding: utf-8 -*-
import hashlib
import re

import psutil

from amplify.agent.containers.abstract import AbstractContainer, definition_id
from amplify.agent.containers.nginx.object import NginxObject
from amplify.agent.context import context
from amplify.agent.eventd import INFO
from amplify.agent.nginx.binary import get_prefix_and_conf_path
from amplify.agent.util import subp

__author__ = "Mike Belov"
__copyright__ = "Copyright (C) Nginx, Inc. All rights reserved."
__credits__ = ["Mike Belov", "Andrei Belov", "Ivan Poluyanov", "Oleg Mamontov", "Andrew Alexeev", "Grant Hulegaard"]
__license__ = ""
__maintainer__ = "Mike Belov"
__email__ = "dedm@nginx.com"


class NginxContainer(AbstractContainer):
    type = 'nginx'

    def discover_objects(self):
        # save current ids
        existing_ids = self.objects.keys()

        # discover nginxes
        nginxes = self.find_all()

        # process all founded nginxes
        dicovered_ids = []
        while len(nginxes):
            try:
                definition, data = nginxes.pop()
                object_id = definition_id(definition)
                dicovered_ids.append(object_id)

                if object_id not in self.objects:
                    # new object - push it
                    data.update(self.object_configs.get(object_id, {}))  # push cloud config
                    new_obj = NginxObject(definition=definition, data=data)

                    # Send discover event.
                    new_obj.eventd.event(
                        level=INFO,
                        message='nginx-%s master process found, pid %s' % (new_obj.version, new_obj.pid)
                    )

                    self.objects[object_id] = new_obj
                elif object_id in self.objects:
                    current_obj = self.objects[object_id]

                    if current_obj.need_restart:
                        # restart object if needed
                        context.log.debug('config was changed (pid %s)' % current_obj.pid)
                        data.update(self.object_configs.get(object_id, {}))  # push cloud config
                        new_obj = NginxObject(definition=definition, data=data)

                        # Send nginx config changed event.
                        new_obj.eventd.event(
                            level=INFO,
                            message='nginx-%s config changed, read from %s' % (new_obj.version, new_obj.conf_path)
                        )

                        self.objects[object_id] = new_obj
                        current_obj.stop(unregister=False)  # stop old object
                    elif current_obj.pid != data['pid']:
                        # check that object pids didn't change
                        context.log.debug(
                            'nginx was restarted (pid was %s now %s)' % (
                                current_obj.pid, data['pid']
                            )
                        )
                        data.update(self.object_configs.get(object_id, {}))  # push cloud config
                        new_obj = NginxObject(definition=definition, data=data)

                        # Send nginx master process restart/reload event.
                        new_obj.eventd.event(
                            level=INFO,
                            message='nginx-%s master process restarted/reloaded, new pid %s, old pid %s' % (
                                new_obj.version,
                                new_obj.pid,
                                current_obj.pid
                            )
                        )

                        self.objects[object_id] = new_obj
                        current_obj.stop(unregister=False)  # stop old object
                    elif current_obj.workers != data['workers']:
                        # check workers on reload
                        context.log.debug(
                            'nginx was reloaded (workers were %s now %s)' % (
                                current_obj.workers, data['workers']
                            )
                        )
                        current_obj.workers = data['workers']
            except psutil.NoSuchProcess:
                context.log.debug('nginx is restarting/reloading, pids are changing, we will wait')

        # check if we left something in objects (nginx could be stopped or something)
        dropped_ids = filter(lambda x: x not in dicovered_ids, existing_ids)
        if len(dropped_ids):
            for dropped_id in dropped_ids:
                dropped_object = self.objects[dropped_id]
                context.log.debug('nginx was stopped (pid was %s)' % dropped_object.pid)
                dropped_object.stop()  # this is necessary too!
                del self.objects[dropped_id]  # this is necessary

    @staticmethod
    def find_all():
        """
        Tries to find all master processes

        :return: list of dict: nginx object definitions
        """
        # get ps info
        ps_cmd = 'ps -xa -o pid,ppid,command | egrep "PID|nginx" | grep -v egrep'
        try:
            ps, _ = subp.call(ps_cmd)
            context.log.debug('ps nginx output: %s' % ps)
        except:
            context.log.error('failed to find running nginx via %s' % ps_cmd)
            context.log.debug('additional info:', exc_info=True)
            return []

        # calculate total amount of nginx master processes
        # if no masters - return
        masters_amount = len(filter(lambda x: 'nginx: master process' in x, ps))
        if masters_amount == 0:
            context.log.debug('nginx masters amount is zero')
            return []

        # collect all info about processes
        masters = {}
        try:
            for line in ps:
                # parse ps response line:
                # 21355     1 nginx: master process /usr/sbin/nginx
                gwe = re.match(r'\s*(?P<pid>\d+)\s+(?P<ppid>\d+)\s+(?P<cmd>.+)\s*', line)

                # if not parsed - go to the next line
                if not gwe:
                    continue

                pid, ppid, cmd = int(gwe.group('pid')), int(gwe.group('ppid')), gwe.group('cmd')

                # match daemonized master and skip the other stuff
                if 'nginx: master process' in cmd and ppid == 1:
                    # get path to binary, prefix and conf_path
                    try:
                        bin_path, prefix, conf_path, version = get_prefix_and_conf_path(cmd)
                    except:
                        context.log.error('failed to find bin_path, prefix and conf_path for %s' % cmd)
                        context.log.debug('', exc_info=True)
                    else:
                        # calculate local id
                        local_id = hashlib.sha256('%s_%s_%s' % (bin_path, conf_path, prefix)).hexdigest()

                        if pid in masters:
                            masters[pid].update(
                                dict(
                                    version=version,
                                    bin_path=bin_path,
                                    conf_path=conf_path,
                                    prefix=prefix,
                                    pid=pid,
                                    local_id=local_id
                                )
                            )
                        else:
                            masters[pid] = dict(
                                version=version,
                                bin_path=bin_path,
                                conf_path=conf_path,
                                prefix=prefix,
                                pid=pid,
                                local_id=local_id,
                                workers=[]
                            )
                # match worker
                elif 'nginx: worker process' or 'nginx: cache manager process' in cmd:
                    if ppid in masters:
                        masters[ppid]['workers'].append(pid)
                    else:
                        masters[ppid] = dict(workers=[pid])
        except Exception as e:
            exception_name = e.__class__.__name__
            context.log.error('failed to parse ps results due to %s' % exception_name)
            context.log.debug('additional info:', exc_info=True)

        # collect results
        results = []
        for pid, description in masters.iteritems():
            if 'bin_path' in description:  # filter workers with non-executable nginx -V (relative paths, etc)
                definition = {'local_id': description['local_id'], 'type': NginxContainer.type}
                results.append((definition, description))
        return results
