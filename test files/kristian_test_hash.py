"""Testing consistent hasning, using replica addresses
to assign location on imaginary ring"""

import hashlib
import math
NUM_SHARDS = 2
HASH_OUTPUT_SPACE = 128
 
replicas = {"alice" : "10.10.0.2", "bob" : "10.10.0.3", "carol": "10.10.0.4",
            "dave":"10.10.0.5", "erin" : "10.10.0.6", "frank" : "10.10.0.7", "grace" : "10.10.0.7"}
MIN_NODES_PER_SHARD = math.floor(len(replicas)/NUM_SHARDS)

"""Couple of approaches to assigning shard_ids
(1) Independently, where each replcia performs some calculation to determine shard_id
    Upon initial startup, replicas will have all other addresses. So they will end with the same result of this function:
    {0: {'erin'}, 1: {'frank', 'carol'}, 2: {'dave', 'bob', 'alice'}}
    Perhaps, we could then make a protocol that moves over replica by alphabetic priority(to ensure all replicas perform same update).
    So in this case alice would be moved to 0 and end up with
    {0: {'erin', 'alice'}, 1: {'frank', 'carol'}, 2: {'dave', 'bob'}}
    From this state, each shard_id has at least 2 nodes!
    Additionally, we can build our views and members accordingly
    From alice's perspective
    views = {alice, carol, bob}
    members = {'erin'}
    QUESTION from Me(kristian): Can members of a shard process requests? Or are we treating shards as like primary back-up where
    say, alice is the primary and 'erin' is the backup?

    ANSWER from Me(will): Yes, all shard members can process requests. Just like in the previous assignment, nodes are non-unique and
    do not have roles like "primary" or "backup". What we ARE interested in, however, is whether the incoming /kvs requests are
    for a key that belongs to the shard that the receiving node is a part of. If the key is in another shard than the one that
    the receiving node is a part of, then it gets forwarded (See "Forwarding requests to shards - Example" in the assignment 4 pdf).
    
(2) Communication protocol where replica communicates 
    idk man
NOTE: each shard id needs at least 2 replicas
"""

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

def assign_shard_ids(n_shards):
    distribution = dict()
    for id in range(NUM_SHARDS):
        distribution[id] = set()
    ring_pos = dict()
    for i in range(n_shards):
        distribution[i] = set()
    for replica in replicas:
        raw_hash = hashlib.md5(bytes(replicas[replica].encode('ascii')))
        hash_as_decimal = int(raw_hash.hexdigest(),16)
        shard_id = hash_as_decimal % n_shards
        distribution[shard_id].add(replica)
        ring_pos[replica] = hash_as_decimal % HASH_OUTPUT_SPACE
    return (distribution, ring_pos)



if __name__=="__main__":
    shards, ring = assign_shard_ids(NUM_SHARDS)
    print(f"Shard distribution over {NUM_SHARDS} shards: {shards}")
    print("attempting fault tolerance", balance_partitions(shards))
    print(f"Replica location on imaginary ring(OUTPUT_SPACE: {HASH_OUTPUT_SPACE}): {ring}")

    views = set()
    for id in shards:
        if id != 0:
            views.update(shards[id])
    print(views)
    # # quick test
    # shards = {0: set(), 1 : {'carol', 'alice', 'bob', 'dave', 'alfred'}, 2: {"john"}}

    # print(balance_partitions(shards))
    