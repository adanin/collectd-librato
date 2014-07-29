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

import collectd
import msgpack
import os
import requests
import sys
import time
import threading
import yaml


# NOTE: This version is grepped from the Makefile, so don't change the
# format of this line.
version = "0.0.1"

# Example of API config
# uuid: <some-uuid-in-hex>
# secret: <some-hex-sring
# api:
#   host: api.wallarm.com
#   port: 443
#   use_ssl: true
#   ca_path: /etc/wallarm/ca.crt
#   ca_verify: true

plugin_name = 'wallarm_msgpack.py'
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


def wallarm_parse_types_file(path):
    """
    Parse the types.db(5) file to determine metric types.
    """

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
                    collectd.warning(
                        '{0}: cannot parse data source {1} on type {2}'.format(
                            plugin_name,
                            ds,
                            type_name
                        )
                    )
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
        raise ValueError(msg)

    if 'api' not in api_creds:
        msg = (
            "{0}: There is no 'api' section"
            " in API configuration file".format(plugin_name)
        )
        collectd.error(msg)
        raise ValueError(msg)

    if 'host' not in api_creds['api']:
        msg = (
            "{0}: There is no 'host' field in 'api' section"
            " in API configuration file".format(plugin_name)
        )
        collectd.error(msg)
        raise ValueError(msg)

    config['api_uuid'] = api_creds['uuid']
    config['api_secret'] = api_creds['secret']
    config['api_host'] = api_creds['api']['host']
    config['api_port'] = api_creds['api'].get('port')
    config['use_ssl'] = api_creds['api'].get('use_ssl')
    config['ca_path'] = api_creds['api'].get('ca_path')
    config['ca_verify'] = api_creds['api'].get('ca_verify')


def create_api_url():
    global config
    scheme = 'https' if config['use_ssl'] else 'http'
    netloc = config['api_host']
    if config['api_port']:
        netloc = '{}:{}'.format(netloc, config['api_port'])

    return requests.utils.urlunparse((
        scheme,
        netloc,
        config['url_path'],
        None,
        None,
        None
    ))


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


def wallarm_msgpack(cfg_obj):
    global config

    for child in cfg_obj.children:
        val = child.values[0]

        if child.key == 'APIConnFile':
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
        raise ValueError(msg)
    get_api_credentials()

    config['http_headers'] = prepare_http_headers()
    config['api_url'] = create_api_url()

    if config['use_ssl'] and config['ca_verify'] and not config['ca_path']:
        msg = "{0}: No CA certificate provided but it's required".format(
            plugin_name
        )
        collectd.error(msg)
        raise ValueError(msg)

    collectd.debug(
        "{0}: Configured successfully".format(plugin_name)
    )


def wallarm_flush_metrics(values, data):
    """
    POST a collection of gauges and counters to wallarm.
    """

    payload = msgpack.packb(values)
    req = requests.post(
        config['api_url'],
        verify=config['ca_path'],
        data=payload,
        headers=config['http_headers'],
        timeout=config['flush_timeout_secs'],
    )
    req.close()
    req.raise_for_status()

    # Remove sent values from queue
    last_sent_value = values[-1]
    with data['data_lock']:
        try:
            index = data['values'].index(last_sent_value)
        except ValueError:
            index = None
        if index:
            data['values'] = data['values'][index + 1:]


def wallarm_queue_measurements(measurement, data):
    global config, plugin_name
    # Updating shared data structures
    #
    data['data_lock'].acquire()

    queue_length = len(data['values'])
    extra_values = queue_length - config['main_queue_max_length']
    # If queue is full remove the oldest value.
    if extra_values >= 0:
        collectd.warning(
            "{0}: The queue is full. Remove the oldest value".format(
                plugin_name
            )
        )
        data['values'] = data['values'][extra_values + 1:]

    data['values'].append(measurement)

    curr_time = get_time()
    last_flush = curr_time - data['last_flush_time']

    # If there is no time to flush just skip it.
    if last_flush < config['flush_interval_secs']:
        data['data_lock'].release()
        return

    # Do nothing if another thread is sending data now.
    if data['send_lock'].locked():
        send_data = False
    else:
        data['send_lock'].acquire()
        flush_values = data['values']
        send_data = True
    data['data_lock'].release()

    if send_data:
        try:
            wallarm_flush_metrics(flush_values, data)
            data['last_flush_time'] = curr_time
        except requests.exceptions.RequestException as e:
            collectd.warning(
                "{0}: Cannot send data to API: {1}".format(
                    plugin_name,
                    str(e),
                )
            )
        finally:
            data['send_lock'].release()


def wallarm_write(v, data=None):
    global plugin_name, types, config

    if v.type not in types:
        collectd.warning(
            '{0}: do not know how to handle type {1}. Do you have'
            ' all your types.db files configured?'.format(
                plugin_name,
                v.type,
            )
        )
        return

    v_type = types[v.type]

    if len(v_type[0]) != len(v.values):
        collectd.warning(
            '{0}: differing number of values for type {1}'.format(
                plugin_name,
                v.type,
            )
        )
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
    global config
    try:
        wallarm_parse_types_file(config['types_db'])
    except Exception as e:
        msg = "{0}: ERROR: Unable to open TypesDB file '{1}': {2}.".format(
            plugin_name,
            config['types_db'],
            str(e)
        )
        collectd.error(msg)
        raise e

    data = {
        'data_lock': threading.Lock(),
        'send_lock': threading.Lock(),
        'last_flush_time': get_time(),
        'values': [],
        }

    collectd.register_write(wallarm_write, data=data)


collectd.register_config(wallarm_msgpack)
collectd.register_init(wallarm_init)
