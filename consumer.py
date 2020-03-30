#!/usr/bin/env python3
'''
RabbitMQ debugging consumer.

Usage:
    ./consumer.py --help
    ./consumer.py --exchange=<exchange> [--host=<hostname> --port=<port> --username=<username> --password=<password> --topics=<topics>]

Options:
    --help                  Shows this screen
    --host=<hostname>       IP or DNS name of node hosting the broker [default: localhost]
    --port=<port>           TCP port on which RabbitMQ is exposed [default: 5672]
    --exchange=<exchange>   The name of the exchange on the broker
    --username=<username>   The username for authenticating with the broker [default: guest]
    --password=<password>   The password for authenticating with the broker [default: guest]
    --topics=<topics>       The topics to subscribe to [default: #]
'''

import json
import signal

import pika

from docopt import docopt
from pygments import highlight, lexers, formatters


RMQ_QUEUE = 'debugging-consumer'
CONNECTION_NAME = 'debugging-consumer'

channel = None


def msg_consumer(channel, method, properties, body):
    print('Routing key: \033[1m{}\033[0m'.format(method.routing_key))

    formatted_body = json.dumps(json.loads(body), indent=4)
    print(highlight(formatted_body, lexers.JsonLexer(), formatters.TerminalFormatter()))
    print('\n\n')

    channel.basic_ack(delivery_tag=method.delivery_tag)


def get_channel_to_rabbit(hostname, port, username, password):
    credentials = pika.PlainCredentials(username, password)
    conn_params = pika.ConnectionParameters(hostname, port, '/', credentials, client_properties={
        'connection_name': CONNECTION_NAME,
    })

    conn_broker = pika.BlockingConnection(conn_params)
    return conn_broker.channel()


def declare_queue(exchange, channel, routing_keys):
    channel.exchange_declare(exchange=exchange, exchange_type='topic', durable=True)

    channel.queue_declare(queue=RMQ_QUEUE, durable=True)
    for rk in routing_keys:
        channel.queue_bind(queue=RMQ_QUEUE, exchange=exchange, routing_key=rk)


def stop_consumer(signum, frame):
    global channel

    print('\nStopping consumer...')
    channel.close()


def main(hostname, port, exchange, username, password, topics):
    global channel

    signal.signal(signal.SIGINT, stop_consumer)

    routing_keys = topics.split(',')
    print(routing_keys)

    channel = get_channel_to_rabbit(hostname, port, username, password)
    declare_queue(exchange, channel, routing_keys)

    channel.basic_consume(RMQ_QUEUE, msg_consumer)
    print('Waiting for messages...\n')
    channel.start_consuming()


if __name__ == '__main__':
    args = docopt(__doc__)
    hostname = args['--host']
    port = int(args['--port'])
    exchange = args['--exchange']
    username = args['--username']
    password = args['--password']
    topics = args['--topics']
    main(hostname, port, exchange, username, password, topics)
