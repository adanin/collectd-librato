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
import Queue
import requests
import time
import threading
import urlparse
import yaml


# NOTE: This version is grepped from the Makefile, so don't change the
# format of this line.
version = "0.0.2"

plugin_name = 'wallarm_api_writer'
types = {}

# Some default values
config = {
    'url_path': '/',
    'types_db': ['/usr/share/collectd/types.db'],
    'api': {
        'host': 'localhost',
        # 'port' depends on 'use_ssl' value
        'ca_path': '/dev/null',
        'ca_verify': False,
        'use_ssl': False,
    },
    'default_ports': {
        'http': 80,
        'https': 444,
    },
    'measr_avg_size': 200,
    'send_timeout_secs': 10,
    'flush_interval_secs': 2,
    'max_msg_size_bytes': 10000,
    'max_measr_keep_interval_secs': 10,
    'msg_size_dec_coeff': 0.98,
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


def get_api_credentials(myconfig):
    """
    Read a settings YAML file with API credentials
    """

    global plugin_name
    try:
        with open(myconfig['api_conn_file']) as fo:
            api_creds = yaml.load(fo)
    except IOError as e:
        collectd.error(
            "{0}: Cannot get API configuration from file {1}: {2}".format(
                plugin_name,
                myconfig['api_conn_file'],
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

    for key in 'uuid', 'secret':
        myconfig['api'][key] = api_creds[key]

    if 'api' not in api_creds:
        return

    for key in 'host', 'port', 'use_ssl', 'ca_path', 'ca_verify':
        if key in api_creds['api']:
            myconfig['api'][key] = api_creds['api'][key]


def create_api_url(myconfig):
    scheme = 'https' if myconfig['api']['use_ssl'] else 'http'
    port = myconfig['api'].get(
        'port',
        myconfig['default_ports'][scheme]
    )
    netloc = '{}:{}'.format(myconfig['api']['host'], port)

    return urlparse.urlunparse((
        scheme,
        netloc,
        myconfig['url_path'],
        None,
        None,
        None
    ))


def build_http_auth(myconfig):
    return {
        'X-Wallarm-Node': myconfig['api'].get('uuid', ''),
        'X-Wallarm-Secret': myconfig['api'].get('secret', ''),
    }


def prepare_http_headers(myconfig):
    headers = {
        'Content-Type': 'application/msgpack',
    }
    headers.update(build_http_auth(myconfig))
    return headers


def is_new_credentials(myconfig):
    try:
        cur_api_file_mtime = os.stat(myconfig['api_conn_file']).st_mtime
        if cur_api_file_mtime != myconfig.get('api_file_mtime', 0):
            myconfig['api_file_mtime'] = cur_api_file_mtime
            return True
    except OSError:
        pass
    return False


def update_credentials(myconfig):
    if is_new_credentials(myconfig):
        get_api_credentials(myconfig)
        myconfig['http_headers'] = prepare_http_headers(myconfig)
        myconfig['api_url'] = create_api_url(myconfig)


def wallarm_api_writer_config(cfg_obj):
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
            config['send_timeout_secs'] = int(val)
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
        msg = '{0}: No file with an API configuration provided'.format(
            plugin_name
        )
        collectd.error(msg)
        raise ValueError(msg)

    update_credentials(config)


def send_data(myconfig, payload):
    """
    POST a collection of metrics to the API.
    """

    global plugin_name
    update_credentials(myconfig)

    if (myconfig['api']['use_ssl'] and myconfig['api']['ca_verify']
            and myconfig['api']['ca_path']):
        verify = myconfig['api']['ca_path']
    else:
        verify = None

    try:
        req = requests.post(
            myconfig['api_url'],
            verify=verify,
            data=payload,
            headers=config['http_headers'],
            timeout=config['send_timeout_secs'],
        )
        req.close()
        req.raise_for_status()
    except requests.exceptions.RequestException as e:
        collectd.warning(
            "{0}: Cannot send data to the API: {1}".format(
                plugin_name,
                str(e),
            )
        )
        return False
    return True


def update_queue_size(myconfig):
    size = myconfig['max_msg_size_bytes'] / myconfig['measr_avg_size']
    myconfig['send_queue_size'] = myconfig['msg_size_dec_coeff'] * size


def pack_msg(myconfig, send_queue):
    msg = msgpack.packb(send_queue)
    while len(msg) > config['max_msg_size_bytes']:
        config['measr_avg_size'] = len(msg) / len(send_queue)
        update_queue_size(myconfig)
        msg = msgpack.packb(
            send_queue[:config['send_queue_size']]
        )
    send_queue = send_queue[config['send_queue_size']:]
    return msg


def send_loop(myconfig, mydata):
    # config = {
    #     'measr_avg_size': 200,
    #     'send_timeout_secs': 10,
    #     'flush_interval_secs': 2,
    #     'max_msg_size_bytes': 1000*1000,
    #     'max_measr_keep_interval_secs': 1800,
    #     'msg_size_dec_coeff': 0.98,
    # }
    # data = {
    #     'values': Queue.Queue(),
    # }

    update_queue_size(myconfig)
    main_queue = mydata['values']
    send_queue = []
    packed_data = None
    empty_main_queue = False
    is_retry = False
    mydata['last_flush_time'] = get_time()

    while True:
        time.sleep(myconfig['flush_interval_secs'])

        # TODO (adanin): Create a shrinker for the main_queue.
        if is_retry:
            if not send_data(myconfig, packed_data):
                continue
            is_retry = False

        while not (empty_main_queue and len(send_queue)) or not is_retry:
            # Fill up internal send_queue.
            try:
                for i in xrange(myconfig['send_queue_size'] - len(send_queue)):
                    send_queue.append(main_queue.get_nowait())
                    main_queue.task_done()
            except Queue.Empty:
                empty_main_queue = True

            # Pack send_queue but try to fit into max message size.
            packed_data = pack_msg(myconfig, send_queue)
            if not send_data(myconfig, packed_data):
                is_retry = True
                continue
            mydata['last_flush_time'] = get_time()


def send_watchdog(myconfig, mydata):
    while True
        try:
            send_loop(myconfig, mydata)
        except KeyboardInterrupt:
            return
        except Exception as e:
            msg = "{0}: Sender failed and will be restarted: {1}".format(
                plugin_name,
                str(e)
            )
            collectd.error(msg)


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

    data['values'].put(measurement)


def wallarm_init():
    global config
    for typedb_file in config['types_db']:
        try:
            wallarm_parse_types_file(typedb_file)
        except IOError as e:
            msg = "{0}: Unable to open TypesDB file '{1}': {2}.".format(
                plugin_name,
                typedb_file,
                str(e)
            )
            collectd.warning(msg)

    if not len(types):
        msg = "{0}: Didn't find any valid type in TypesDB files: {1}".format(
            plugin_name,
            config['types_db'],
        )
        collectd.error(msg)
        raise ValueError(msg)

    data = {
        'last_flush_time': get_time(),
        'values': Queue.Queue(),
        }

    send_thread = threading.Thread(target=send_watchdog, args=[config, data])
    data['sender'] = send_thread
    send_thread.start()
    collectd.register_write(wallarm_write, data=data)


collectd.register_config(wallarm_api_writer_config)
collectd.register_init(wallarm_init)
