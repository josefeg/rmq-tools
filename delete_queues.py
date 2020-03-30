#!/usr/bin/env python3
'''
RabbitMQ debugging consumer.

Usage:
    ./delete_queues.py --help
    ./delete_queues.py [--host=<hostname> --port=<port> --username=<username> --password=<password>]

Options:
    --help                  Shows this screen
    --host=<hostname>       IP or DNS name of node hosting the broker [default: localhost]
    --port=<port>           TCP port on which RabbitMQ is exposed [default: 15672]
    --username=<username>   The username for authenticating with the broker [default: guest]
    --password=<password>   The password for authenticating with the broker [default: guest]
'''

import requests

from docopt import docopt

from requests.auth import HTTPBasicAuth


def call(hostname, port, username, password, method, queue_name=""):
    resp = requests.request(method=method,
                            url=f"http://{hostname}:{port}/api/queues/%2F/{queue_name}",
                            auth=HTTPBasicAuth(username, password))

    resp.raise_for_status()

    if len(resp.content) > 0:
        return resp.json()
    return None


def list_queues(hostname, port, username, password):
    queue_details = call(hostname, port, username, password, 'GET')
    return list(map(lambda x: x['name'], queue_details))


def delete_queue(hostname, port, username, password, queue_name):
    call(hostname, port, username, password, 'DELETE', queue_name=queue_name)


def delete_queues(hostname, port, username, password):
    queues = list_queues(hostname, port, username, password)
    print("Deleteing {} queues...".format(len(queues)))
    for queue in queues:
        delete_queue(hostname, port, username, password, queue)
    print("Done!")


def main():
    args = docopt(__doc__)
    hostname = args['--host']
    port = int(args['--port'])
    username = args['--username']
    password = args['--password']
    delete_queues(hostname, port, username, password)


if __name__ == '__main__':
    main()
