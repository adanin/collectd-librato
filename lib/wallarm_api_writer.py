# Copyright 2014 Andrey Danin
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import collectd
import logging
import msgpack
import os
import Queue
import requests
import time
import threading
import traceback
import urlparse
import yaml

from copy import copy

# NOTE: This version is grepped from the Makefile, so don't change the
# format of this line.
version = "0.0.2"

plugin_name = 'wallarm_api_writer'

class WallarmApiWriter(object):
    def __init__(self, plugin_name):
        self.plugin_name = plugin_name

        self.config = {
            'url_path': '/',
            'types_db': ['/usr/share/collectd/types.db'],
            'default_ports': {
                'http': 80,
                'https': 444,
            },
            'measr_avg_size': 50,
            'send_timeout_secs': 10,
            'flush_interval_secs': 2,
            'sleep_tick_interval_secs': 0.1,
            'max_msg_size_bytes': 7000,
            'max_store_timeout': 60,
            'min_send_threshold': 10,
            'queue_size_decreaser': 5,
            'logging': {
                'enabled': False,
                'filename': '/tmp/wallarm_api_writer.log',
                'level': 'debug',
            }
        }
        self.default_api_config = {
                'host': 'localhost',
                # 'port' depends on 'use_ssl' value
                'ca_path': '/dev/null',
                'ca_verify': False,
                'use_ssl': False,
            }

        self.types = {}
        self.api_config = {}
        self.api_url = ''
        self.api_file_mtime = 0

        self.measr_avg_size = None
        self.last_try_time = 0
        self.last_flush_time = 0

        self.logger = None

    def get_time(self):
        """
        Return the current time as epoch seconds.
        """

        return int(time.mktime(time.localtime()))

    def setup_logging(self):
        if ('logging' not in self.config or
                not self.config['logging'].get('enabled')):
            return
        logconfig = self.config['logging']
        logging.basicConfig(
            filename=logconfig['filename'],
            level=logging.getLevelName(logconfig['level'].upper()))
        self.logger = logging.getLogger()

    def log(self, level, msg):
        if self.logger:
            getattr(self.logger, level)(msg)
        getattr(collectd, level)(msg)

    def wallarm_api_writer_config(self, cfg_obj):

        for child in cfg_obj.children:
            val = child.values[0]

            if child.key == 'APIConnFile':
                self.config['api_conn_file'] = val
            elif child.key == 'TypesDB':
                self.config['types_db'] = child.values
            elif child.key == 'MaxRequestSize':
                self.config['max_msg_size_bytes'] = int(val)
            elif child.key == 'DropOutdatedTimeout':
                self.config['max_store_timeout'] = int(val)
            elif child.key == 'FlushInterval':
                self.config['flush_interval_secs'] = int(val)
            elif child.key == 'FlushTimeout':
                self.config['send_timeout_secs'] = int(val)
            elif child.key == 'URLPath':
                self.config['url_path'] = val
            else:
                self.log(
                    'warning',
                    '{0}: Unknown config key: {1}.'.format(
                        self.plugin_name,
                        child.key
                    )
                )

        self.setup_logging()

        if 'api_conn_file' not in self.config:
            msg = '{0}: No file with an API configuration provided'.format(
                self.plugin_name
            )
            self.log('error', msg)
            raise ValueError(msg)

        self.log("info", "Got config: {}".format(self.config))

    def wallarm_parse_types_file(self, path):
        """
        Parse the types.db(5) file to determine metric types.
        """

        with open(path, 'r') as f:
            for line in f:
                fields = line.split()
                if len(fields) < 2:
                    continue

                type_name = fields[0]

                if type_name[0] == '#':
                    continue

                v = []
                for ds in fields[1:]:
                    ds = ds.rstrip(',')
                    ds_fields = ds.split(':')

                    if len(ds_fields) != 4:
                        self.log(
                            'warning',
                            '{0}: cannot parse data source {1}'
                            ' on type {2}'.format(
                                self.plugin_name,
                                ds,
                                type_name
                            )
                        )
                        continue

                    v.append(ds_fields)

                self.types[type_name] = zip(
                    *map(
                        lambda n: n[:2], v
                    )
                )

    def drop_creds(self):
        self.api_config = {}
        self.api_url = ''

    def get_api_credentials(self):
        """
        Read a settings YAML file with API credentials
        """

        self.drop_creds()
        try:
            with open(self.config['api_conn_file']) as fo:
                api_creds = yaml.load(fo)
                # TODO (adanin): catch yaml.load exception too.
        except IOError as e:
            self.log(
                'error',
                "{0}: Cannot get API configuration from file {1}: {2}".format(
                    self.plugin_name,
                    self.config['api_conn_file'],
                    str(e)
                )
            )
            raise e

        if 'uuid' not in api_creds or 'secret' not in api_creds:
            msg = (
                "{0}: There is no 'secret' or 'uuid' fields"
                " in API configuration file".format(self.plugin_name)
            )
            self.log('error',msg)
            raise ValueError(msg)

        self.api_config = copy(self.default_api_config)
        for key in 'uuid', 'secret':
            self.api_config[key] = api_creds[key]

        if 'api' not in api_creds:
            return

        for key in 'host', 'port', 'use_ssl', 'ca_path', 'ca_verify':
            if key in api_creds['api']:
                self.api_config[key] = api_creds['api'][key]

    def create_api_url(self):
        scheme = 'https' if self.api_config['use_ssl'] else 'http'
        port = self.api_config.get(
            'port',
            self.config['default_ports'][scheme]
        )
        netloc = '{}:{}'.format(self.api_config['host'], port)

        self.api_url = urlparse.urlunparse((
            scheme,
            netloc,
            self.config['url_path'],
            None,
            None,
            None
        ))

    def build_http_auth(self):
        return {
            'X-Wallarm-Node': self.api_config['uuid'],
            'X-Wallarm-Secret': self.api_config['secret'],
        }

    def prepare_http_headers(self):
        self.http_headers = {
            'Content-Type': 'application/msgpack',
        }
        self.http_headers.update(self.build_http_auth())

    def is_new_credentials(self):
        try:
            file_mtime = os.stat(self.config['api_conn_file']).st_mtime
            if file_mtime != self.api_file_mtime:
                self.api_file_mtime = file_mtime
                return True
        except OSError:
            self.drop_creds()
        return False

    def update_credentials(self):
        if self.is_new_credentials():
            try:
                self.get_api_credentials()
                self.prepare_http_headers()
                self.create_api_url()
            except ValueError:
                pass

    def wallarm_write(self, value):
        if value.type not in self.types:
            self.log(
                'warning',
                '{0}: do not know how to handle type {1}. Do you have'
                ' all your types.db files configured?'.format(
                    self.plugin_name,
                    value.type,
                )
            )
            return

        v_type = self.types[value.type]

        if len(v_type[0]) != len(value.values):
            self.log(
                'warning',
                '{0}: differing number of values for type {1}'.format(
                    self.plugin_name,
                    value.type,
                )
            )
            return

        measurement = {
            "values": value.values,
            "dstypes": v_type[0],
            "dsnames": v_type[1],
            "time": value.time,
            "interval": value.interval,
            "plugin": value.plugin,
            "plugin_instance": value.plugin_instance,
            "type": value.type,
            "type_instance": value.type_instance
        }

        self.main_queue.put(measurement)

    def shutdown_callback(self):
        self.shutdown_event.set()

    def flush_callback(self):
        self.flush_event.set()

    def update_queue_size(self):
        size = int(self.config['max_msg_size_bytes'] / self.measr_avg_size)
        self.send_queue_size = size - self.config['queue_size_decreaser']

    def get_payload(self):
        # Fill up send_queue with new messages until full.
        try:
            for i in xrange(self.send_queue_size - len(self.send_queue)):
                self.send_queue.append(self.main_queue.get_nowait())
                self.main_queue.task_done()
        except Queue.Empty:
            pass
        if self.config['min_send_threshold'] > len(self.send_queue):
            return '', 0

        # self.log("info",
        #     "Trying to pack queue with {} messages.".format(
        #         len(self.send_queue)
        #     )
        # )

        # Pack messages and try to fit into limit.
        msg = msgpack.packb(self.send_queue)
        msg_len = len(self.send_queue)
        while len(msg) > self.config['max_msg_size_bytes']:
            self.measr_avg_size = len(msg) / len(self.send_queue)
            self.update_queue_size()
            msg = msgpack.packb(
                self.send_queue[:self.send_queue_size]
            )
            msg_len = self.send_queue_size
        # self.log(
        #     "info",
        #     "Packed {} messages with total size {} bytes. {} messages in"
        #     " the send_queue left.".format(msg_len, len(msg),
        #                                    len(self.send_queue))
        # )
        return msg, msg_len

    def drop_old_messages(self):
        if not len(self.send_queue):
            return
        time_delta = self.get_time() - self.send_queue[-1]['time']
        if time_delta > self.config['max_store_timeout']:
            self.log(
                'info',
                "{0}: Drop {1} outdated messages from queue".format(
                    self.plugin_name,
                    len(self.send_queue),
                )
            )
            self.send_queue[:] = ()

    def send_loop(self):
        # Drop outdated messages from send_queue.
        self.drop_old_messages()

        payload, msg_len = self.get_payload()

        self.update_credentials()
        if not self.api_url:
            return

        while payload:
            if not self.send_data(payload):
                return
            self.send_queue[:msg_len] = ()
            self.last_flush_time = self.get_time()
            payload, msg_len = self.get_payload()

    def send_data(self, payload):
        """
        POST a collection of metrics to the API.
        """
        if (self.api_config['use_ssl'] and self.api_config['ca_verify']
                and self.api_config['ca_path']):
            verify = self.api_config['ca_path']
        else:
            verify = None

        try:
            req = requests.post(
                self.api_url,
                verify=verify,
                data=payload,
                headers=self.http_headers,
                timeout=self.config['send_timeout_secs'],
            )
            if req.status_code not in (200,):
                raise requests.exceptions.HTTPError(
                    'HTTP status in response is: {}'.format(req.status_code)
                )

        except requests.exceptions.RequestException as e:
            self.log(
                'warning',
                "{0}: Cannot send data to the API: {1}".format(
                    self.plugin_name,
                    traceback.format_exc(),
                )
            )
            return False
        return True

    def send_watchdog(self):
        last_repeat = False
        while not last_repeat:
            time.sleep(self.config['sleep_tick_interval_secs'])

            if self.shutdown_event.is_set():
                last_repeat = True
                self.flush_event.set()

            time_delta = self.get_time() - self.last_try_time
            if (time_delta < self.config['flush_interval_secs'] and
                    not self.flush_event.is_set()):
                continue

            try:
                self.send_loop()
            except Exception as e:
                msg = "{0}: Sender failed and will be restarted: {1}".format(
                    self.plugin_name,
                    traceback.format_exc()
                )
                self.log('error', msg)
            self.last_try_time = self.get_time()
            self.flush_event.clear()

    def wallarm_init(self):
        for typedb_file in self.config['types_db']:
            try:
                self.wallarm_parse_types_file(typedb_file)
            except IOError as e:
                msg = "{0}: Unable to open TypesDB file '{1}': {2}.".format(
                    self.plugin_name,
                    typedb_file,
                    str(e)
                )
                self.log('warning', msg)

        if not len(self.types):
            msg = (
                "{0}: Didn't find any valid type in TypesDB files: {1}".format(
                    self.plugin_name,
                    self.config['types_db'],
                )
            )
            self.log('error', msg)
            raise ValueError(msg)

        self.last_try_time = self.get_time()
        self.last_flush_time = self.get_time()
        self.main_queue = Queue.Queue()
        self.send_queue = []
        self.measr_avg_size = self.config['measr_avg_size']
        self.update_queue_size()
        self.shutdown_event = threading.Event()
        self.flush_event = threading.Event()

        self.send_thread = threading.Thread(target=self.send_watchdog)
        self.send_thread.start()

        collectd.register_write(self.wallarm_write)
        collectd.register_shutdown(self.shutdown_callback)
        collectd.register_flush(self.flush_callback)


plugin = WallarmApiWriter(plugin_name)
collectd.register_config(plugin.wallarm_api_writer_config)
collectd.register_init(plugin.wallarm_init)
