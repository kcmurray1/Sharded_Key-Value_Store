from bingus import resharding
from flask import Flask, request, jsonify, make_response, Blueprint
import json
import ast
import requests
from threading import Thread, Lock
from vectorclock import VectorClock
from queue import Queue
import time
import hashlib
import math

views_route = Blueprint("views", __name__)
_store = dict()
MIN_KEY_LENGTH = 50

shard_count = 0 # current # of shards in the system

# Pulse vars 
PULSE_INTERVAL = 0.5 # duration of time between pulse request sends
update_views_lock = Lock()

MIN_NODES = 2

OUTPUT_SPACE = 128
views = set()
socket_address = None # this replica's address
local_vc = dict()
request_lock = Lock()
shard_id = None # id of the shard this node belongs to
shards = dict() # {shard_id: shard_members}
ring_positions = dict()
_substore = dict()

# --------------------------------------------------------------------------------------------------------------
# Functions
# --------------------------------------------------------------------------------------------------------------
def print_replica():
    print(f"_store: {_store}, views: {views}, VC: {local_vc}", flush=True)

def get_local_causal_metadata(sender=True, sender_vc_all=None):
    if not sender:
        if not sender_vc_all:
            sender_vc_all = dict()
        sender_vc_all[str(shard_id)] = local_vc
        return {"vc" : sender_vc_all} 

    return {"vc":local_vc}

def forward(req, addr, key):
    response = requests.request(req.method, f"http://{addr}/kvs/{key}",json=req.json)
    return make_response(response.json(), response.status_code)

def consistent_hash_key(key):
    """determine which shard key needs to go to

    Returns:
        address that key should be processed at  
    """
    raw_hash = hashlib.md5(bytes(key.encode('ascii')))
    hash_as_decimal = int(raw_hash.hexdigest(),16)

    key_ring_pos =  hash_as_decimal % OUTPUT_SPACE

    # walk along ring to nearest node(clockwise)
    while key_ring_pos not in ring_positions:
        key_ring_pos = (key_ring_pos + 1) % OUTPUT_SPACE
    
    return ring_positions[key_ring_pos]

def buffer_send_kvs(req, key, addr):
    """Send update local vector clock along with request to other replicas"""
    while True:
        try:
            metadata = req.json
            metadata["causal-metadata"] = dict(sender=socket_address, vc=local_vc)
            response = requests.request(f"{req.method}", f"http://{addr}/kvs/{key}", json=metadata, timeout=10)
            if 'get-vc' in response.json() and VectorClock(response.json()['get-vc']) == VectorClock(local_vc):
                break
            if response.status_code != 503:
                break
        except requests.Timeout:
            with update_views_lock:
                update_views({addr}, removed=True)
            continue
        except (requests.ConnectionError, requests.RequestException):
            with update_views_lock:
                update_views({addr}, removed=True)
            break

def buffer_send_view(req : Flask.request_class, view : str):
    """Keep sending updated view information until received and processed (eventual consistency)"""
    global views
    metadata = req.json
    metadata["relay"] = None
    while True:
        try:
            response = requests.request(f"{req.method}", f"http://{view}/view", json=metadata)
            if response.status_code != 503:
                return
        except requests.Timeout:
            continue
        except (requests.ConnectionError, requests.RequestException):
            continue

def max_of(first : dict, second: dict):
    """Return dictionary containing max of two dictionaries"""
    keys = first.keys()
    res = dict()
    for key in keys:
        res[key] = max(first[key], second[key])
    return res

def dependency_check(sender_addr, sender_vc) -> bool:
    # Request is from client
    if not sender_addr:
        # Compare sender vc with what we have
        # Pad if unequal length from sender
        if len(sender_vc) < len(local_vc):
            for key in local_vc:
                if key not in sender_vc:
                    sender_vc[key] = local_vc[key]
        # Compare clocks
        return not (VectorClock(sender_vc) <= VectorClock(local_vc))
    # Request is from a Replica(i.e it is a relayed/broadcasted msg)
    else:
        # Make sure that both VCs have the same view before comparing
        if not VectorClock(local_vc).has_same_view_as(VectorClock(sender_vc)):
            return True

        # Compare local vector clock with received vector clock
        if sender_vc[sender_addr] != local_vc[sender_addr] + 1:
            return True
        for addr in sender_vc:
            if addr == sender_addr:
                continue
            if not (sender_vc[addr] <= local_vc[addr]):
                return True
        # There are no dependencies
        return False

def in_json(value_to_find, json):
    if value_to_find not in json:
        return None
    return json[value_to_find]

# Relay/rebroadcast a request to all replicas on the /view endpoint
def relay_view(relayed_request):
    for view in views:
        if view != socket_address:
            buffer_send_view(relayed_request, view)

# Relay/rebroadcast a request to all replicas on the /kvs endpoint
def relay_kvs(relayed_request, key):
    local_vc[socket_address] += 1
    # relay request
    for member in shards[shard_id]:
        if member != socket_address:
            # Send request until it is received or the receiver is down
            buffer_send_kvs(relayed_request, key, member)

def valid_json(json_str):
    '''
    Determine whether a string can validly convert to json.\n
    Then check if the string contains the "message" key.\n
    :RETURN: value if string contains "message" key, otherwise return None 
    '''
    try:
        # Can be converted to json, so treat it as a literal
        json_values = ast.literal_eval(json_str)
        json.loads(json_str)
        # Check for 'message' body
        if "value" not in set(json_values.keys()):
            return None
        # return value associated with 'message' body
        return json_values["value"]
        # Not in json format
    except ValueError:
        return None

def get_metadata(req):
    """Metadata of a request

    Returns: tuple in the form (sender_addr, sender_vc) 
        or (None, sender_vc)
    """
    sender = None
    meta = req.json["causal-metadata"]
    sender_vc = meta['vc']

    if "sender" in meta:
        sender = meta["sender"]
        return (sender, sender_vc,None)

    if str(shard_id) not in sender_vc:
        sender_vc[str(shard_id)] = local_vc
    return (sender, sender_vc[str(shard_id)], sender_vc)

# --------------------------------------------------------------------------------------------------------------
# Key Value Store endpoint
# --------------------------------------------------------------------------------------------------------------
@views_route.route("/kvs/<key>", methods=["GET", "PUT", "DELETE"])
def adjust_mapping(key):
    global local_vc
    global _store
    # key length must be less than 50 characters
    if len(key) > MIN_KEY_LENGTH:
        return make_response(jsonify(error="Key is too long"), 400)
    
    sender = None
    sender_vc = None
    sender_vc_all = None
    addr_to_send = None
    # Perform logic if causal-metadata is present(not None)
    if request.json["causal-metadata"]:
        sender, sender_vc, sender_vc_all = get_metadata(request)
        # hash value to determine whether to forward or proceed locally
        addr_to_send = consistent_hash_key(key)
        # request is from client and key is hashed to different replica
        if not sender and addr_to_send != socket_address:
                return forward(request, addr_to_send,key)
   
        # Check for dependencies
        dependency_result = dependency_check(sender, sender_vc)
        # There is a dependency return 503
        if dependency_result:
            return make_response({"error": "Causal dependencies not satisfied; try again later", "get-vc" : get_local_causal_metadata()["vc"]}, 503)
    else:
        addr_to_send = consistent_hash_key(key)
        if addr_to_send != socket_address:
            return forward(request, addr_to_send,key)
    # PUT request
    if request.method == "PUT":
        # Verify that request contains valid json and "value" as a key
        if not 'value' in request.json:
            return make_response(jsonify(error="PUT request does not specify a value"), 400)

        value = request.json['value']

        # Update local VC if request contains a VC
        if sender_vc:
            local_vc = max_of(local_vc, sender_vc)

        # Only broadcast delivered client requests
        if not sender:
            relay_kvs(request, key)

        # Created new mapping
        res = make_response({"result": "created", "causal-metadata": get_local_causal_metadata(sender, sender_vc_all)}, 201)
        # Replaced old mapping
        if key in _store:
            res = make_response({"result": "replaced", "causal-metadata": get_local_causal_metadata(sender, sender_vc_all)}, 200)
        # Update store
        _store[key] = value
        # possibly update _substore if key hashed to local replica
        if addr_to_send == socket_address:
            _substore[key] = value
        
        return res
    
    # Cannot process GET or DELETE requests if key does not exist in _store
    if key not in _store:
        return make_response(jsonify(error="Key does not exist"), 404)
    
    # GET Request
    if request.method == "GET":  
        return make_response({"result": "found", "value": _store[key], "causal-metadata": get_local_causal_metadata()}, 200)

    # DELETE Request
    if request.method == "DELETE":
        # Remove key from dictionary
        _store.pop(key, None)
        # possibly remove key from _substore
        if addr_to_send == socket_address:
            _substore.pop(key, None)
        # Update local VC if request contains a VC
        if sender_vc:
            local_vc = max_of(local_vc, sender_vc)

        # Only broadcast delivered client requests
        if not sender:
            relay_kvs(request, key)
        
        # Complete delete request
        return make_response({"result": "deleted", "causal-metadata": get_local_causal_metadata()}, 200)

# --------------------------------------------------------------------------------------------------------------
# View endpoint
# --------------------------------------------------------------------------------------------------------------

@views_route.route("/view", methods=["GET", "PUT", "DELETE"])
def handle_views():
    # Add view(replica)
    if request.method == "PUT":
        new_view = in_json("view", request.json)
        if not new_view:
            return make_response(jsonify(error="bad request"), 400)
        # already part of the view
        if new_view in views:
           return make_response(jsonify(result="already present", replica_data=dict(vc=get_local_causal_metadata()["vc"], store=_store)), 200)

        # relay request to all views(replicas)
        if "relay" in request.json and request.json["relay"]:
            relay_view(request)
        
        # add new view, mutex lock over the views
        with update_views_lock:
            update_views({new_view}, removed=False)

         #result  replica_data = {vc: <1,2,3>, store: _store}
        return make_response(jsonify(result="added", replica_data=dict(vc=get_local_causal_metadata()["vc"], store=_store)), 201)
    
    # Get views
    if request.method == "GET":
        with update_views_lock:
            update_views(pulse(), removed=True)
        return make_response(jsonify(view=list(views)), 200)
    
    # Remove replica by view
    if request.method == "DELETE":
        remove_view = in_json("socket-address", request.json)
        if not remove_view:
            return make_response(jsonify(error="bad request"), 400)
        # not already part of the views
        if remove_view not in views:
            return make_response(jsonify(error="View has no such replica"), 404)
        
        # relay request to all views(replicas)
        if "relay" in request.json and request.json["relay"]:
            relay_view(request)
        
        # remove view
        # mutex lock while updating views
        with update_views_lock:
            update_views({remove_view}, removed=True)

        return make_response(jsonify(result="deleted"), 200)

# --------------------------------------------------------------------------------------------------------------
# Heartbeat / Pulse endpoint
# --------------------------------------------------------------------------------------------------------------

# Simple acknowledgement to the "pulse" request sent by other replicas, to tell them that they are still up
@views_route.route("/pulse", methods=["GET"])
def respond_to_pulse():
    return make_response(jsonify(result="Ahoy!"), 200)

# Sends pulses to the /pulse endpoint at every replica in this replica's view
def pulse():
    global views
    crashed_replicas = set()
    for view in views:
        if view == socket_address:
            continue
        try:
            response = requests.get(f"http://{view}/pulse", json=dict())
        except (requests.Timeout, requests.ConnectionError, requests.RequestException, requests.exceptions.HTTPError):
            crashed_replicas.add(view)

    return crashed_replicas

# This function should always be called with a mutex
# To be called whenever the replica's views needs to be changed
def update_views(new_views, removed=False):
    global views
    global local_vc
    global shards
    if not new_views:
        return
    
    if removed:
        # remove new_views from views
        views = views.difference(new_views)
    else:
        # add new_views to views
        views = views.union(new_views)
        for view in new_views:
            if view in shards[shard_id]:
                local_vc[view] = 0

# Pulse Sender Thread
# Sends out a pulse at a regular interval, and updates the replica view if any replicas do not respond
def periodic_pulse_sender():
    global views
    while True:
        crashed_replicas = pulse()
        # Remove all crashed replicas from views
        # Only 1 thread can access update_views() at a time
        if crashed_replicas:
            with update_views_lock:
                update_views(crashed_replicas, removed=True)
        # Wait a set interval of time between pulses
        time.sleep(PULSE_INTERVAL)

# --------------------------------------------------------------------------------------------------------------
# Shard endpoints
# --------------------------------------------------------------------------------------------------------------
# Retrieve the list of all shard ids
@views_route.route("/shard/ids", methods=["GET"])
def get_shard_ids():
    shard_ids = list(shards.keys())
    return make_response({"shard-ids": shard_ids}, 200)

# Retrieve the shard id of the responding node/replica
@views_route.route("/shard/node-shard-id", methods=["GET"])
def get_shard_id():
    return make_response({"node-shard-id": shard_id}, 200)

# Look up the members of the specified shard
@views_route.route("/shard/members/<ID>", methods=["GET"])
def get_members(ID):
    global shards
    ID = int(ID)
    if ID not in shards:
        return make_response({"error": "Shard ID does not exist"}, 404) 
    else:
        return make_response({"shard-members": list(shards[ID])}, 200)

# Look up the number of kv pairs stored in the specified shard
@views_route.route("/shard/key-count/<ID>", methods=["GET"])
def get_key_count(ID):
    ID = int(ID)
    shard_ids = list(shards.keys())
    if ID not in shard_ids:
        return make_response({"error": "Shard ID does not exist"}, 404) 
    elif ID != shard_id:
        while True:
            for node in shards[ID]:
                try:
                    response = requests.get(f"http://{node}/shard/key-count/{ID}", json=dict())
                    key_count = response.json()["shard-key-count"]
                    return make_response({"shard-key-count": key_count}, 200)
                except (requests.Timeout, requests.ConnectionError, requests.RequestException, requests.exceptions.HTTPError):
                    continue
    else:
        return make_response({"shard-key-count": len(_store)}, 200)

# Assign the node <ID:PORT> to the shard <ID>
# Given JSON body {"socket-address": <IP:PORT>}
@views_route.route("/shard/add-member/<ID>", methods=["PUT"])
def add_member(ID):
    ID = int(ID)
    shard_ids = list(shards.keys())

    add_socket_address = in_json("socket-address", request.json)

    if not add_socket_address:
        return make_response(dict(error="Missing <IP:PORT> header"),400)

    if add_socket_address not in views:
        return make_response(dict(error=f"Node with port {add_socket_address} does not exist"),404)

    if add_socket_address not in views or ID not in shard_ids:
        return make_response(dict(error=f"Shard with ID {ID} does not exist"),404)

    for node in views:
        while True:
            # Forward to every replica in the system
            try:
                metadata = request.json
                metadata["shards"] = shards
                jason_friendy_shards_dictionary = to_jason_friendy_shard_dict(shards)
                metadata["shards"] = jason_friendy_shards_dictionary
                response = requests.put(f"http://{node}/assign/{ID}", json=metadata)
                break
            except (requests.Timeout, requests.ConnectionError, requests.RequestException, requests.exceptions.HTTPError):
                pass

    return make_response(dict(result="node added to shard"),200)

# Converts from dict() of set()s to a dict() of list()s
def to_jason_friendy_shard_dict(shards):
    jason_friendy_shard_dict = dict()
    for shard_m in shards:
        jason_friendy_shard_dict[shard_m] = list(shards[shard_m])
    return jason_friendy_shard_dict

# Converts from dict() of list()s to a dict() of set()s
def to_jason_unfriendy_shard_dict(shards):
    jason_unfriendy_shard_dict = dict()
    for shard_m in shards:
        jason_unfriendy_shard_dict[int(shard_m)] = set(shards[shard_m])
    return jason_unfriendy_shard_dict

# New node completely outside of the shard system (but in the views) gets added
@views_route.route("/assign/<ID>", methods=["PUT"])
def assign_to_shard(ID):
    global shards
    global shard_id
    global local_vc
    global ring_positions   
    global _substore
    ID = int(ID)

    # new node does not have shards initialized
    if not shards:
        # assign shard id and shards
        shard_id = ID
        shards = to_jason_unfriendy_shard_dict(request.json["shards"])

    # update shards at ID to contain the address of the new node
    add_socket_address = in_json("socket-address", request.json)
    shards[ID].add(add_socket_address)
    ring_positions = resharding.calculate_ring_positions(views)

    if ID == shard_id:
        local_vc[socket_address] = 0

    # check if this node is in front of the newly added node on the imaginary ring
    prev_pos = None
    if socket_address != add_socket_address:

        for ring_pos in sorted(ring_positions.keys()):
            if ring_positions[ring_pos] == socket_address:
                break
            prev_pos = ring_positions[ring_pos]
        if prev_pos == add_socket_address:
            # need to rehash our _substore
            temp_substore = dict()
            for key in _substore:
                if consistent_hash_key(key) == socket_address:
                    temp_substore[key] = _substore[key]
            _substore = temp_substore
    
    if socket_address == add_socket_address:
        # This is the newly-added node, so replicate store & vc
        local_vc = dict()
        for member in shards[ID]:
            local_vc[member] = 0
        replicate_shard_member(shards[ID])

    return make_response(dict(result="node added to shard"),200)

def replicate_shard_member(shard_members):
    global views
    global local_vc
    global _store
    global _substore
    # send to all nodes in view
    for view in views:
        while True:
            try:
                response = requests.put(f'http://{view}/view', json={'view': socket_address, 'relay': False})
                metadata = response.json()

                # update local clock based on members of new replica
                if 'replica_data' in metadata and view in shards[shard_id]:                   
                    # compare with padding
                    received_vc = metadata['replica_data']['vc']
                    # pad local clock {a: 0, b:0}
                    local_vc.update(received_vc)
                    # pad received clock {b: 0, a:0}
                    received_vc.update(local_vc)

                    # compare clocks(should equal length and share keys)
                    if VectorClock(local_vc) <= VectorClock(received_vc):
                        local_vc = received_vc
                        # New node's store should match store from members         
                        _store = metadata['replica_data']['store']
                other_replica_store = metadata['replica_data']['store']
                # update _substore to contain keys that hash to new node
                for key in other_replica_store:
                    if consistent_hash_key(key) == socket_address:
                        _substore[key] = other_replica_store[key]

                break

            # Ignore unresponsive replicas
            except (requests.ConnectionError, requests.RequestException, requests.exceptions.HTTPError):
                break

# Trigger a reshard into <INTEGER> shards, maintaining fault-tolerance
# Given JSON body {"shard-count": <INTEGER>}
@views_route.route("/shard/reshard", methods=["PUT"])
def reshard():
    global shard_id
    global shards
    global ring_positions
    global shard_count
    global _store
    global local_vc
    # validate JSON body
    new_shard_count = in_json("shard-count", request.json) 
    if not new_shard_count:
        return make_response(dict(error="Missing shard-count"),400)
    
    # validate new_shard_count
    if math.floor(len(views)/new_shard_count) < MIN_NODES:
        return make_response(dict(error="Not enough nodes to provide fault tolerance with requested shard count"), 400)
    
    # two cases
    #(1)Reshard with changed N
    reshard, reshard_ring_positions = resharding.partition_by_hash(views, new_shard_count)

    # assign possible new id
    # did this replica's shard_id change?
    new_shard_id = find_replica_id(reshard, socket_address)
    # relay the reshard request to all views(replicas) if sent from client
    if "relay" not in request.json:
        relay_req = request
        relay_req.json["relay"] = "bingus"
        relay_reshard(relay_req)

    # update this node
    update_node(reshard[new_shard_id])

    # assign vars
    shards = reshard
    ring_positions = reshard_ring_positions
    shard_count = new_shard_count
    shard_id = new_shard_id
    # Reshard complete
    return make_response(dict(result="resharded"), 200)

def find_replica_id(shards, replica_to_find):
    for id in shards:
        if replica_to_find in shards[id]:
            return id

# Relay/rebroadcast a request to all replicas on the /reshard endpoint
def relay_reshard(relayed_request):
    for view in views:
        if view != socket_address:
            buffer_send_reshard(relayed_request, view)

def buffer_send_reshard(req : Flask.request_class, view : str):
    """Keep sending updated view information until received and processed (eventual consistency)"""
    global views
    metadata = req.json
    metadata["relay"] = None
    while True:
        try:
            response = requests.request(f"{req.method}", f"http://{view}/shard/reshard", json=metadata)
            break
        except requests.Timeout:
            continue
        except (requests.ConnectionError, requests.RequestException):
            continue

@views_route.route("/get-substore", methods=["GET", "PUT"])
def handle_get_substore():
    if request.method == "GET":
        return make_response(dict(substore=_substore),200)

def update_node(new_members):
    """Update node to match information in new partition"""
    global local_vc
    global _store
    # OLD_MEMBERS(becoming uneeded)
    # start _store anew
    _store = dict()
    # add local _subtore
    _store.update(_substore)
    # populate rest of _store
    for member in new_members:
        # get member _substore
        member_res = requests.get(f"http://{member}/get-substore", json=dict())
        member_substore = member_res.json()['substore']
        # update _store with recv _substore
        _store.update(member_substore)

    # Reset vc_clock to 0
    local_vc = dict()
  
    for member in new_members:
        local_vc[member] = 0