from flask import Flask, request
import requests
import sys
import time
from bingus import create_app, views_route, resharding
from vectorclock import VectorClock
import threading
import hashlib
import math
TIME_TO_BOOT = 2.5

def find_replica_id(shards, replica_to_find):
    for id in shards:
        if replica_to_find in shards[id]:
            return id

def notify_replicas(view_points, socket_address):
    # view and vector clock at minimum should be
    # views = {a}
    # vc = {a: 0}
    # Add this replica to other replicas
    for view_address in view_points:
        while True:
            # Add Replicas that respond
            try:
                # Send PUT request
                response = requests.put(f'http://{view_address}/view', json={'view': socket_address, 'relay': True})
                metadata = response.json()
                views_route.views.add(view_address)

                # update local clock
                if 'replica_data' in metadata and (find_replica_id(views_route.shards, view_address) == views_route.shard_id):                   
                    # compare with padding
                    received_vc = metadata['replica_data']['vc']
                    # pad local clock {a: 0, b:0}
                    views_route.local_vc.update(received_vc)
                    # pad received clock {b: 0, a:0}
                    received_vc.update(views_route.local_vc)

                    # compare clocks(should equal length and share keys)
                    with views_route.update_views_lock:
                        if VectorClock(views_route.local_vc) <= VectorClock(received_vc):
                            views_route.local_vc = received_vc
                            views_route._store = metadata['replica_data']['store']
                break

            # Ignore unresponsive replicas
            except (requests.ConnectionError, requests.RequestException, requests.exceptions.HTTPError):
                break
    
    print("Replica After boot", flush=True)
    print(f"socket_addr {views_route.socket_address}\n"
          f"views {views_route.views}\n"
          f"local_clocl {views_route.local_vc}\n"
          f"shard_id {views_route.shard_id}\n"
          f"shards {views_route.shards}\n"
          f"ring_positions {views_route.ring_positions}", flush=True)
    # Start Pulse Sender Thread
    h = threading.Timer(TIME_TO_BOOT * 2, pulse_starter)
    h.start()

# Build then run Replica
def startup():
    """Set initial replica values
    socket = addr
    views = {addr}
    local_clock = {addr: 0}
    """

    views_route.socket_address = sys.argv[1]
    views_route.views.add(views_route.socket_address)
    views_route.local_vc[views_route.socket_address] = 0
    
    starting_views = sys.argv[2].split(',')
    # Check for shard_count
    try:
        if sys.argv[3]:
            shard_count = int(sys.argv[3])
            views_route.shard_count = shard_count
            partitions, ring_pos = resharding.partition_by_hash(starting_views, shard_count)
            print("GENERATED partitions: ", partitions)
            views_route.ring_positions = ring_pos
            # set shard id
            views_route.shard_id = find_replica_id(partitions, views_route.socket_address)
            # set shards
            views_route.shards = partitions
    except IndexError:
        pass
    starting_views.remove(views_route.socket_address)
    
    print(f"starting replica: {views_route.socket_address}")
    # Notify other replicas about this new instance
    t = threading.Timer(TIME_TO_BOOT, notify_replicas, args=(starting_views, views_route.socket_address))
    t.start()

    my_app = create_app()
    # https://stackoverflow.com/questions/43644083/python-thread-running-twice-when-called-once-in-main
    my_app.run(host='0.0.0.0', port=8090, debug=False)

def pulse_starter():
    views_route.periodic_pulse_sender()

if __name__ == '__main__':
    startup()
