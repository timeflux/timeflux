import argparse
import os, sys
import time
import random

path = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, path  + '/../../')

from timeflux.core.sync import Server, Client

parser = argparse.ArgumentParser()
parser.add_argument('--host', help='The server address', default='')
parser.add_argument('--port', help='The server port', type=int, default=12300)
parser.add_argument('mode', help='The mode: "server" or "client".')
args = parser.parse_args()

def dummy_time(offset=1):
    time.sleep(random.random() / 2) # simulate network latency
    return time.perf_counter() + offset

if args.mode == 'server':
    server = Server(args.host, args.port, now=dummy_time)
    try:
        server.start()
    except KeyboardInterrupt:
        server.stop()

if args.mode == 'client':
    client = Client(args.host, args.port, rounds=100)
    try:
        client.sync()
    except KeyboardInterrupt:
        client.stop()

