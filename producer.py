#!/usr/bin/env python3
# -*- coding: utf-8 -*-
'''
RabbitMQ testing producer.

Usage:
    ./producer.py --help
    ./producer.py --host=<hostname> --port=<port> --exchange=<exchange> --template=<file> [--username=<username> --password=<password>]

Options:
    --help                  Shows this screen
    --host=<hostname>       IP or DNS name of node hosting the broker
    --port=<port>           TCP port on which RabbitMQ is exposed
    --exchange=<exchange>   The name of the exchange on the broker
    --template=<file>       The name of the template file that contains the messages to publish
    --username=<username>   The username for authenticating with the broker [default: guest]
    --password=<password>   The password for authenticating with the broker [default: guest]
'''

import json
import random
import string
import uuid as guid

import pika

from datetime import datetime
from docopt import docopt
from jinja2 import Environment, FileSystemLoader


CONNECTION_NAME = 'debugging-producer'


def uuid():
    return str(guid.uuid4())


def now():
    return datetime.utcnow().isoformat()


def random_int(min=0, max=10):
    return random.randint(min, max)


def random_string(length=30):
    return ''.join(random.choices(string.ascii_lowercase + string.ascii_uppercase, k=length))


def random_value(values):
    return random.choice(values)


def random_float(min=0, max=5, decimal_places=4):
    return round(random.uniform(min, max), decimal_places)


def parse_input(input_file):
    file_loader = FileSystemLoader('.')
    env = Environment(loader=file_loader)
    env.globals['uuid'] = uuid
    env.globals['now'] = now
    env.globals['random_int'] = random_int
    env.globals['random_string'] = random_string
    env.globals['random_value'] = random_value
    env.globals['random_float'] = random_float

    template = env.get_template(input_file)
    return json.loads(template.render())


def connect_to_rabbit(hostname, port, username, password):
    credentials = pika.PlainCredentials(username, password)
    conn_params = pika.ConnectionParameters(hostname, port, '/', credentials, client_properties={
        'connection_name': CONNECTION_NAME,
    })
    return pika.BlockingConnection(conn_params)


def publish(conn, exchange, msg):
    body = bytes(json.dumps(msg['body']), 'utf-8')

    channel = conn.channel()
    channel.basic_publish(exchange, msg['routing_key'], body)
    channel.close()


def main(hostname, port, exchange, username, password, input_file):
    msgs = parse_input(input_file)

    conn = connect_to_rabbit(hostname, port, username, password)

    for msg in msgs:
        input('Press Enter to publish message...')
        publish(conn, exchange, msg)
    print('Done!')

    conn.close()


if __name__ == '__main__':
    args = docopt(__doc__)
    hostname = args['--host']
    port = int(args['--port'])
    exchange = args['--exchange']
    username = args['--username']
    password = args['--password']
    input_file = args['--template']
    main(hostname, port, exchange, username, password, input_file)
