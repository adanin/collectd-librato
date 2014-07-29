# Copyright 2011 wallarm, Inc.
# Copyright 2014 Andrey Danin
#
# Orginal file was taken from https://github.com/wallarm/collectd-wallarm
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

import base64
import collectd
import httplib
import math
import msgpack
import os
import re
import socket
import ssl
import sys
import time
import threading
import urllib2
import yaml

from string import maketrans
from copy import copy

# NOTE: This version is grepped from the Makefile, so don't change the
# format of this line.
version = "0.0.1"

# config = { 'api_path' : '/v1/metrics',
#            'api' : 'https://metrics-api.wallarm.com',
#            'types_db' : '/usr/share/collectd/types.db',
#            'metric_prefix' : 'collectd',
#            'metric_separator' : '.',
#            'source' : None,
#            'flush_interval_secs' : 30,
#            'flush_max_measurements' : 600,
#            'flush_timeout_secs' : 15,
#            'lower_case' : False,
#            'single_value_names' : False
#            }

# Example of API config
# uuid: <some-uuid-in-hex>
# secret: <some-hex-sring
# api:
#   host: api.wallarm.com
#   port: 443
#   use_ssl: true
#   ca_path: /etc/wallarm/ca.crt
#   ca_verify: true

plugin_name = 'wallarm-msgpack.py'
types = {}

conn_obj = None
# Some default values
config = {
    'use_ssl': False,
    'ca_path': None,
    'ca_verify': False,
    'api_port': None,
    'url_path': '/',
    'types_db': '/usr/share/collectd/types.db',
    'flush_interval_secs': 2,
    'flush_timeout_secs': 10,
    'main_queue_max_length': 200000,
    'send_queue_max_length': 10000,
}


def get_time():
    """
    Return the current time as epoch seconds.
    """

    return int(time.mktime(time.localtime()))

#
# Parse the types.db(5) file to determine metric types.
#
def wallarm_parse_types_file(path):
    global types

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
                    collectd.warning('%s: cannot parse data source ' \
                                     '%s on type %s' %
                                     (plugin_name, ds, type_name))
                    continue

                v.append(ds_fields)

            types[type_name] = zip(
                *map(
                    lambda n: n[:2], v
                )
            )

def get_api_credentials():
    """
    Read a settings YAML file with API credentials
    """

    global config, plugin_name
    collectd.debug(
        "{0}: Read config file with API configuration '{1}'".format(
            plugin_name,
            config['api_conn_file'],
        )
    )
    try:
        with open(config['api_conn_file']) as fo:
            api_creds = yaml.load(fo)
    except IOError as e:
        collectd.error(
            "{0}: Cannot get API configuration from file {1}: {2}".format(
                plugin_name,
                config['api_conn_file'],
                str(e)
            )
        )
        raise e

    if 'uuid' not in api_creds or 'secret' not in api_creds:
        msg = (
            "{0}: There is no 'secret' or 'uuid' fields"
            " in API configuration file".format(plugin_name)
        )
        collectd.error(msg)
        raise Exception(msg)

    if 'api' not in api_creds:
        msg = (
            "{0}: There is no 'api' section"
            " in API configuration file".format(plugin_name)
        )
        collectd.error(msg)
        raise Exception(msg)

    if 'host' not in api_creds['api']:
        msg = (
            "{0}: There is no 'host' field in 'api' section"
            " in API configuration file".format(plugin_name)
        )
        collectd.error(msg)
        raise Exception(msg)

    config['api_uuid'] = api_creds['uuid']
    config['api_secret'] = api_creds['secret']
    config['api_host'] = api_creds['api']['host']
    config['api_port'] = api_creds['api']['port']
    config['use_ssl'] = api_creds['api']['use_ssl']
    config['ca_path'] = api_creds['api']['ca_path']
    config['ca_verify'] = api_creds['api']['ca_verify']

def create_api_url():
    global config
    scheme = 'https' if config['use_ssl'] else 'http'
    netloc = config['api_host']
    if config['api_port']:
        netloc = '{}:{}'.format(netloc, config['api_port'])

    return requests.utils.urlunparse(
        scheme,
        netloc,
        config['url_path'],
        None,
        None,
        None
    )

def build_http_auth():
    global config
    return {
        'X-Wallarm-Node': config['api_uuid'],
        'X-Wallarm-Secret': config['api_secret'],
    }

def prepare_http_headers():
    headers = {
        'Content-Type': 'application/msgpack',
    }
    headers.update(build_http_auth())
    return headers

def wallarm_config(cfg_obj):
    global config

    for child in cfg_obj.children:
        val = child.values[0]

        if child.key  == 'APIConnFile':
            config['api_conn_file'] = val
        elif child.key == 'TypesDB':
            config['types_db'] = val
        elif child.key == 'MainQueueMaxLength':
            config['main_queue_max_length'] = int(val)
        elif child.key == 'SendQueueMaxLength':
            config['send_queue_max_length'] = int(val)
        elif child.key == 'FlushIntervalSecs':
            config['flush_interval_secs'] = int(val)
        elif child.key == 'FlushTimeoutSecs':
            config['flush_timeout_secs'] = int(val)
        elif child.key == 'URLPath':
            config['url_path'] = val
        else:
            collectd.warning(
                '{0}: Unknown config key: {1}.'.format(
                    plugin_name,
                    child.key
                )
            )

    if 'api_conn_file' not in config:
        msg = '{0}: No file with API configuration provided'.format(
            plugin_name
        )
        collectd.error(msg)
        raise Exception(msg)
    get_api_credentials()

    config['http_headers'] = prepare_http_headers()
    config['api_url'] = create_api_url()

    collectd.debug(
        "{0}: Configured successfully".format(plugin_name)
    )

def create_opener():
    api_creds = config['api_connection']
    if api_creds.get('verify_ca'):
        ca_reqs = ssl.CERT_REQUIRED
    else:
        ca_reqs = ssl.CERT_NONE

    class ValidHTTPSConnection(httplib.HTTPConnection):
            "This class allows communication via SSL."

            default_port = httplib.HTTPS_PORT

            def __init__(self, *args, **kwargs):
                httplib.HTTPConnection.__init__(self, *args, **kwargs)

            def connect(self):
                "Connect to a host on a given (SSL) port."

                sock = socket.create_connection(
                    (self.host, self.port),
                    self.timeout,
                    self.source_address
                )
                if self._tunnel_host:
                    self.sock = sock
                    self._tunnel()
                self.sock = ssl.wrap_socket(
                    sock,
                    ca_certs=api_creds.get('ca_path', None),
                    cert_reqs=ca_reqs
                )

    class ValidHTTPSHandler(urllib2.HTTPSHandler):

        def https_open(self, req):
                return self.do_open(ValidHTTPSConnection, req)

    return urllib2.build_opener(ValidHTTPSHandler)

def wallarm_flush_metrics(values, data):
    """
    POST a collection of gauges and counters to wallarm.
    """

    headers = {
        'Content-Type': 'octet/stream',
        'Authorization': 'Basic %s' % config['auth_header']
        }

    # body = json.dumps({ 'gauges' : gauges, 'counters' : counters })

    msg = msgpack.packb(values)
    api_creds = config['api_connection']
    url = api_creds['api']
    req = urllib2.Request(url, msg, headers)
    f = None
    try:
        f = data['opener'].open(req, timeout = config['flush_timeout_secs'])
        response = f.read()
    except urllib2.HTTPError as error:
        body = error.read()
        collectd.warning('%s: Failed to send metrics: Code: %d. Response: %s' % \
                         (plugin_name, error.code, body))
        raise error
    except IOError as error:
        collectd.warning('%s: Error when sending metrics (%s)' % \
                         (plugin_name, error.reason))
        raise error
    finally:
        if f:
            f.close()

    # Remove sent values from queue
    last_sent_value = values[-1]
    with data['data_lock']:
        try:
            index = data['values'].index(last_sent_value)
        except ValueError:
            index = None
        if index:
            data['data_lock'] = data['data_lock'][index + 1:]

def wallarm_queue_measurements(measurement, data):
    global data, plugin_name
    # Updating shared data structures
    #
    data['data_lock'].acquire()

    queue_length = len(data['values'])
    extra_values = config['queue_max_length'] - queue_length
    # If queue is full remove the oldest value.
    if extra_values >= 0:
        data['values'] = data['values'][extra_values + 1:]
        collectd.error(
            "{0}: The queue is full. Dropping the oldest value".format(
                plugin_name
            )
        )

    data['values'].append(measurement)

    curr_time = get_time()
    last_flush = curr_time - data['last_flush_time']

    # If there is no time to flush just skip it.
    if last_flush < config['flush_interval_secs'] and \
           queue_length < config['flush_max_measurements']:
        data['data_lock'].release()
        return

    # Do nothing if another thread is sending data now.
    if data['send_lock'].locked():
        flush_values = data['values']
        send_data = False
    else:
        data['send_lock'].acquire()
        send_data = True
    data['data_lock'].release()
    
    if send_data:
        try:
            wallarm_flush_metrics(flush_values, data)
            data['last_flush_time'] = curr_time
        except urllib2.HTTPError, IOError:
            pass
        finally:
            data['send_lock'].release()

def wallarm_write(v, data=None):
    global plugin_name, types, config

    if v.type not in types:
        collectd.warning('%s: do not know how to handle type %s. ' \
                         'do you have all your types.db files configured?' % \
                         (plugin_name, v.type))
        return

    v_type = types[v.type]

    if len(v_type[0]) != len(v.values):
        collectd.warning('%s: differing number of values for type %s' % \
                         (plugin_name, v.type))
        return

    measurement = {
        "values": v.values,
        "dstypes": v_type[0],
        "dsnames": v_type[1],
        "time": v.time,
        "interval": v.interval,
        "plugin": v.plugin,
        "plugin_instance": v.plugin_instance,
        "type": v.type,
        "type_instance": v.type_instance
    }
    wallarm_queue_measurements(measurement, data)

def wallarm_init():

    try:
        wallarm_parse_types_file(config['types_db'])
    except:
        msg = '%s: ERROR: Unable to open TypesDB file: %s.' % \
              (plugin_name, config['types_db'])
        raise Exception(msg)

    data = {
        'data_lock': threading.Lock(),
        'send_lock': threading.Lock(),
        'last_flush_time': get_time(),
        'values': [],
        'opener': create_opener(),
        }

    collectd.register_write(wallarm_write, data = data)

collectd.register_config(wallarm_config)
collectd.register_init(wallarm_init)
