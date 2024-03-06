"""Testing consistent hasning, using replica addresses
to assign location on imaginary ring"""

import hashlib

NUM_SHARDS = 3
HASH_OUTPUT_SPACE = 128
replicas = {"alice" : "10.10.0.2", "bob" : "10.10.0.3", "carol": "10.10.0.4",
            "dave":"10.10.0.5", "erin" : "10.10.0.6", "frank" : "10.10.0.7"}

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
(2) Communication protocol where replica communicates 
    idk man
NOTE: each shard id needs at least 2 replicas
"""
def assign_shard_ids(n_shards):
    distribution = dict()
    for i in range(n_shards):
        distribution[i] = set()
    for replica in replicas:
        raw_hash = hashlib.md5(bytes(replicas[replica].encode('ascii')))
        hash_as_decimal = int(raw_hash.hexdigest(),16)
        shard_id = hash_as_decimal % n_shards
        #print(f"Replica: {replica} with id: {shard_id} and hash {hash_as_decimal}")
        distribution[shard_id].add(replica)
    return distribution



if __name__=="__main__":
    res = assign_shard_ids(NUM_SHARDS)
    print(f"Shard distribution over {NUM_SHARDS} shards: {res}")