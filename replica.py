from flask import Flask, request
import requests
import sys
import time
from bingus import create_app, views_route
from vectorclock import VectorClock
import threading
import hashlib
TIME_TO_BOOT = 2
HASH_OUTPUT_SPACE = 128
MIN_NODES_PER_SHARD = 2

def balance_partitions(shards):
    """
    Returns:
        dictionary containing shards with at least 2 nodes assigned
        Else returns None
    """
    fault_tolerant = dict()
    
    # Find shard that contain < 2 nodes
    for cur_shard in shards.keys():
        nodes = shards[cur_shard]
       
        for j in shards:
            nodes_j = shards[j]
            # Keep adding nodes until shard reaches threshold
            # try to find shard that contains > 2 nodes 
            if len(nodes_j) > MIN_NODES_PER_SHARD and len(nodes) < MIN_NODES_PER_SHARD:
                # Sort alphabetically and redistribute to shard with < 2 nodes
                nodes_j = list(sorted(nodes_j))
                # Move node from large partition into smaller partition
                node = nodes_j.pop()
                # Update the distribution of shards
                shards[j] = set(nodes_j)
                nodes.add(node)
        # Was unable to reach fault tolerance after redistribution
        if len(nodes) < MIN_NODES_PER_SHARD:
            return None
        # Add parition with at least min nodes needed
        fault_tolerant[cur_shard] = nodes
    return fault_tolerant

def partition_by_hash(replicas, shard_count):
    distribution = dict()
    for id in range(shard_count):
        distribution[id] = set()
    ring_pos = dict()
    for replica in replicas:
        # Hash each replica by address
        raw_hash = hashlib.md5(bytes(replica.encode('ascii')))
        hash_as_decimal = int(raw_hash.hexdigest(),16)
        # Calculate shard_id for replica
        shard_id = hash_as_decimal % shard_count
        distribution[shard_id].add(replica)
        # calculate position on imaginary ring
        ring_pos[replica] = hash_as_decimal % HASH_OUTPUT_SPACE
    # each partition must have at least 2 nodes
    distribution = balance_partitions(distribution)
    return (distribution, ring_pos)

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

                # Check for replicated data in response
                if 'replica_data' in metadata:
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
          f"shard_members {views_route.shard_members}\n"
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
    print(f"starting replica: {views_route.socket_address}")
    starting_views = sys.argv[2].split(',')
    # Check for shard_count
    if sys.argv[3]:
        shard_count = int(sys.argv[3])
        views_route.shard_count = shard_count
        partitions, ring_pos = partition_by_hash(starting_views, shard_count)
        print("GENERATED partitions: ", partitions)
        views_route.ring_positions = ring_pos
        # set local shard id
        views_route.shard_id = find_replica_id(partitions, views_route.socket_address)
        # set members
        views_route.shard_members = partitions[views_route.shard_id]
        # set local views
        new_views = set()
        for id in partitions:
            if id != views_route.shard_id:
                new_views.update(partitions[id])
        starting_views = new_views
    
    #starting_views.remove(views_route.socket_address)
    

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
