from flask import Flask, request, jsonify, make_response, Blueprint
import json
import ast
import requests
from threading import Thread, Lock
from vectorclock import VectorClock
from queue import Queue
import time
from hashlib import md5

views_route = Blueprint("views", __name__)
_store = dict()
MIN_KEY_LENGTH = 50

shard_count = 0 # WIP - receive this info from argv in replica.py

# Pulse vars 
PULSE_INTERVAL = 0.5 # duration of time between pulse request sends
update_views_lock = Lock()

views = set()
socket_address = None # this replica's address
local_vc = dict()
request_lock = Lock()
shard_id = None
shard_members = set()
ring_positions = dict()
# --------------------------------------------------------------------------------------------------------------
# Functions
# --------------------------------------------------------------------------------------------------------------
def print_replica():
    print(f"_store: {_store}, views: {views}, VC: {local_vc}", flush=True)

def get_local_causal_metadata():
    return {"vc": local_vc}


def buffer_send_kvs(req, key, addr):
    """Send update local vector clock along with request to other replicas"""
    while True:
        try:
            metadata = req.json
            metadata["causal-metadata"] = dict(sender=socket_address, vc=local_vc)
            response = requests.request(f"{req.method}", f"http://{addr}/kvs/{key}", json=metadata, timeout=10)
            if 'get-vc' in response.json() and VectorClock(response.json()['get-vc']["vc"]) == VectorClock(local_vc):
                break
            if response.status_code != 503:
                break
        except requests.Timeout:
            with update_views_lock:
                update_views({addr}, removed=True)
            break
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
    for view in views:
        if view != socket_address:
            # Send request until it is received or the receiver is down
            buffer_send_kvs(relayed_request, key, view)

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
    if "sender" in req.json["causal-metadata"]:
        sender = req.json["causal-metadata"]["sender"]
    return (sender, req.json["causal-metadata"]["vc"])

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
    # Perform logic if causal-metadata is present(not None)
    if request.json["causal-metadata"]:
        sender, sender_vc = get_metadata(request)
        # Check for dependencies
        dependency_result = dependency_check(sender, sender_vc)
        # There is a dependency return 503
        if dependency_result:
            return make_response({"error": "Causal dependencies not satisfied; try again later", "get-vc" : get_local_causal_metadata()["vc"]}, 503)
       
    # PUT request
    if request.method == "PUT":
        # Verify that request contains valid json and "value" as a key
        # value = valid_json(request.data.decode())
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
        res = make_response({"result": "created", "causal-metadata": get_local_causal_metadata()}, 201)
        # Replaced old mapping
        if key in _store:
            res = make_response({"result": "replaced", "causal-metadata": get_local_causal_metadata()}, 200)
        # Update store
        _store[key] = value
        
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
    if not new_views:
        return
    
    if removed:
        # remove new_views from views
        views = views.difference(new_views)
    else:
        # add new_views to views
        views = views.union(new_views)
        for view in new_views:
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
    pass

# Retrieve the shard id of the responding node/replica
@views_route.route("/shard/node-shard-id", methods=["GET"])
def get_shard_id():
    pass

# Look up the members of the specified shard
@views_route.route("/shard/members/<ID>", methods=["GET"])
def get_members():
    pass

# Look up the number of kv pairs stored in the specified shard
@views_route.route("/shard/key-count/<ID>", methods=["GET"])
def get_key_count():
    pass

# Assign the node <ID:PORT> to the shard <ID>
# Given JSON body {"socket-address": <IP:PORT>}
@views_route.route("/shard/add-member/<ID>", methods=["PUT"])
def add_member():
    pass

# Trigger a reshard into <INTEGER> shards, maintaining fault-tolerance
# Given JSON body {"shard-count": <INTEGER>}
@views_route.route("/shard/reshard", methods=["PUT"])
def reshard():
    pass
