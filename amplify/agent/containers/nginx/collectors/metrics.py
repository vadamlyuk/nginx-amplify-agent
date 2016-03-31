# -*- coding: utf-8 -*-
import re
import time
import psutil

from amplify.agent.util.ps import Process
from amplify.agent.errors import AmplifyParseException
from amplify.agent.context import context
from amplify.agent.containers.abstract import AbstractCollector
from amplify.agent.eventd import WARNING

__author__ = "Mike Belov"
__copyright__ = "Copyright (C) Nginx, Inc. All rights reserved."
__credits__ = ["Mike Belov", "Andrei Belov", "Ivan Poluyanov", "Oleg Mamontov", "Andrew Alexeev", "Grant Hulegaard"]
__license__ = ""
__maintainer__ = "Mike Belov"
__email__ = "dedm@nginx.com"

STUB_RE = re.compile(r'^Active connections: (?P<connections>\d+)\s+[\w ]+\n'
                     r'\s+(?P<accepts>\d+)'
                     r'\s+(?P<handled>\d+)'
                     r'\s+(?P<requests>\d+)'
                     r'\s+Reading:\s+(?P<reading>\d+)'
                     r'\s+Writing:\s+(?P<writing>\d+)'
                     r'\s+Waiting:\s+(?P<waiting>\d+)')


class NginxMetricsCollector(AbstractCollector):

    short_name = 'nginx_metrics'

    def __init__(self, **kwargs):
        super(NginxMetricsCollector, self).__init__(**kwargs)

        self.processes = [Process(pid) for pid in self.object.workers]
        self.zombies = set()

    def collect(self):
        for method in (
            self.workers_count,
            self.memory_info,
            self.workers_fds_count,
            self.workers_rlimit_nofile,
            self.workers_io,
            self.workers_cpu,
            self.status
        ):
            try:
                method()
            except psutil.NoSuchProcess as e:
                exception_name = e.__class__.__name__
                pid = e.pid

                # Log exception
                context.log.warning(
                    'failed to collect metrics %s due to %s, object restart needed (PID: %s)' %
                    (method.__name__, exception_name, pid)
                )
                self.object.need_restart = True
            except Exception as e:
                exception_name = e.__class__.__name__

                # Fire event warning.
                self.eventd.event(
                    level=WARNING,
                    message="can't obtain worker process metrics (maybe permissions?)",
                    onetime=True
                )

                # Log exception
                context.log.error('failed to collect metrics %s due to %s' % (method.__name__, exception_name))
                context.log.debug('additional info:', exc_info=True)

    def workers_count(self):
        """nginx.workers.count"""
        self.statsd.gauge('nginx.workers.count', len(self.object.workers))

    def handle_zombie(self, pid):
        """
        removes pid from workers list
        :param pid: zombie pid
        """
        context.log.warning('zombie process %s found' % pid)
        self.zombies.add(pid)

    def memory_info(self):
        """
        memory info

        nginx.workers.mem.rss
        nginx.workers.mem.vms
        nginx.workers.mem.rss_pct
        """
        rss, vms, pct = 0, 0, 0.0
        for p in self.processes:
            if p.pid in self.zombies:
                continue
            try:
                mem_info = p.memory_info()
                rss += mem_info.rss
                vms += mem_info.vms
                pct += p.memory_percent()
            except psutil.ZombieProcess:
                self.handle_zombie(p.pid)

        self.statsd.gauge('nginx.workers.mem.rss', rss)
        self.statsd.gauge('nginx.workers.mem.vms', vms)
        self.statsd.gauge('nginx.workers.mem.rss_pct', pct)

    def workers_fds_count(self):
        """nginx.workers.fds_count"""
        fds = 0
        for p in self.processes:
            if p.pid in self.zombies:
                continue
            try:
                fds += p.num_fds()
            except psutil.ZombieProcess:
                self.handle_zombie(p.pid)
        self.statsd.incr('nginx.workers.fds_count', fds)

    def workers_rlimit_nofile(self):
        """
        nginx.workers.rlimit_nofile

        sum for all hard limits (second value of rlimit)
        """
        rlimit = 0
        for p in self.processes:
            if p.pid in self.zombies:
                continue
            try:
                rlimit += p.rlimit_nofile()
            except psutil.ZombieProcess:
                self.handle_zombie(p.pid)
        self.statsd.gauge('nginx.workers.rlimit_nofile', rlimit)

    def workers_io(self):
        """
        io

        nginx.workers.io.kbs_r
        nginx.workers.io.kbs_w
        """
        # collect raw data
        read, write = 0, 0
        for p in self.processes:
            if p.pid in self.zombies:
                continue
            try:
                io = p.io_counters()
                read += io.read_bytes
                write += io.write_bytes
            except psutil.ZombieProcess:
                self.handle_zombie(p.pid)
        current_stamp = int(time.time())

        # kilobytes!
        read /= 1024
        write /= 1024

        # get deltas and store metrics
        for metric_name, value in {'nginx.workers.io.kbs_r': read, 'nginx.workers.io.kbs_w': write}.iteritems():
            prev_stamp, prev_value = self.previous_values.get(metric_name, [None, None])
            if prev_stamp:
                value_delta = value - prev_value
                self.statsd.incr(metric_name, value_delta)
            self.previous_values[metric_name] = [current_stamp, value]

    def workers_cpu(self):
        """
        cpu

        nginx.workers.cpu.system
        nginx.workers.cpu.user
        """
        worker_user, worker_sys = 0.0, 0.0
        for p in self.processes:
            if p.pid in self.zombies:
                continue
            try:
                u, s = p.cpu_percent()
                worker_user += u
                worker_sys += s
            except psutil.ZombieProcess:
                self.handle_zombie(p.pid)
        self.statsd.gauge('nginx.workers.cpu.total', worker_user + worker_sys)
        self.statsd.gauge('nginx.workers.cpu.user', worker_user)
        self.statsd.gauge('nginx.workers.cpu.system', worker_sys)

    def status(self):
        """
        check if found extended status, collect "global" metrics from it
        don't look for stub_status
        if there's no extended status easily accessible, proceed with stub_status
        """
        if self.object.plus_status_enabled and self.object.plus_status_internal_url:
            self.plus_status()
        elif self.object.stub_status_enabled and self.object.stub_status_url:
            self.stub_status()
        else:
            return

    def stub_status(self):
        """
        stub status metrics

        nginx.http.conn.current = ss.active
        nginx.http.conn.active = ss.active - ss.waiting
        nginx.http.conn.idle = ss.waiting
        nginx.http.request.count = ss.requests ## counter
        nginx.http.request.reading = ss.reading
        nginx.http.request.writing = ss.writing
        nginx.http.conn.dropped = ss.accepts - ss.handled ## counter
        nginx.http.conn.accepted = ss.accepts ## counter
        """
        stub_body = ''
        stub = {}
        stub_time = int(time.time())

        # get stub status body
        try:
            stub_body = context.http_client.get(self.object.stub_status_url, timeout=1, json=False, log=False)
        except:
            context.log.error('failed to check stub_status url %s' % self.object.stub_status_url)
            context.log.debug('additional info', exc_info=True)
            stub_body = None

        if not stub_body:
            return

        # parse body
        try:
            gre = STUB_RE.match(stub_body)
            if not gre:
                raise AmplifyParseException(message='stub status %s' % stub_body)
            for field in ('connections', 'accepts', 'handled', 'requests', 'reading', 'writing', 'waiting'):
                stub[field] = int(gre.group(field))
        except:
            context.log.error('failed to parse stub_status body')
            raise

        # store some variables for further use
        stub['dropped'] = stub['accepts'] - stub['handled']

        # gauges
        self.statsd.gauge('nginx.http.conn.current', stub['connections'])
        self.statsd.gauge('nginx.http.conn.active', stub['connections'] - stub['waiting'])
        self.statsd.gauge('nginx.http.conn.idle', stub['waiting'])
        self.statsd.gauge('nginx.http.request.writing', stub['writing'])
        self.statsd.gauge('nginx.http.request.reading', stub['reading'])
        self.statsd.gauge('nginx.http.request.current', stub['reading'] + stub['writing'])

        # counters
        counted_vars = {
            'nginx.http.request.count': 'requests',
            'nginx.http.conn.accepted': 'accepts',
            'nginx.http.conn.dropped': 'dropped'
        }
        for metric_name, stub_name in counted_vars.iteritems():
            value, stamp = stub[stub_name], stub_time
            prev_stamp, prev_value = self.previous_values.get(metric_name, [None, None])

            if prev_stamp:
                value_delta = value - prev_value
                self.statsd.incr(metric_name, value_delta)

            self.previous_values[metric_name] = [stamp, value]

    def plus_status(self):
        """
        plus status metrics

        nginx.http.conn.accepted = connections.accepted  ## counter
        nginx.http.conn.dropped = connections.dropped  ## counter
        nginx.http.conn.active = connections.active
        nginx.http.conn.current = connections.active + connections.idle
        nginx.http.conn.idle = connections.idle
        nginx.http.request.count = requests.total  ## counter
        nginx.http.request.current = requests.current
        """
        stamp = int(time.time())

        # get plus status body
        try:
            status = context.http_client.get(self.object.plus_status_internal_url, timeout=1, log=False)
        except:
            context.log.error('failed to check plus_status url %s' % self.object.plus_status_internal_url)
            context.log.debug('additional info', exc_info=True)
            status = None

        if not status:
            return

        connections = status.get('connections', {})
        requests = status.get('requests', {})

        # gauges

        self.statsd.gauge('nginx.http.conn.active', connections.get('active'))
        self.statsd.gauge('nginx.http.conn.idle', connections.get('idle'))
        self.statsd.gauge('nginx.http.conn.current', connections.get('active') + connections.get('idle'))
        self.statsd.gauge('nginx.http.request.current', requests.get('current'))

        # counters
        counted_vars = {
            'nginx.http.request.count': requests.get('total'),
            'nginx.http.conn.accepted': connections.get('accepted'),
            'nginx.http.conn.dropped': connections.get('dropped'),
        }
        for metric_name, value in counted_vars.iteritems():
            prev_stamp, prev_value = self.previous_values.get(metric_name, [None, None])

            if prev_stamp:
                value_delta = value - prev_value
                self.statsd.incr(metric_name, value_delta)

            self.previous_values[metric_name] = [stamp, value]
