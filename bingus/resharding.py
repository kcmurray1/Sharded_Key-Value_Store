import math
import hashlib

HASH_OUTPUT_SPACE = 128
GLOBAL_MIN = 2
views = { '10.10.0.6:8090', '10.10.0.4:8090', '10.10.0.7:8090', '10.10.0.2:8090', '10.10.0.3:8090', '10.10.0.5:8090'}
def balance_shards(shards, MIN_NODES_PER_SHARD):
    """
    Returns:
        dictionary containing shards with at least 2 nodes assigned
        Else returns None
    """
    if MIN_NODES_PER_SHARD < GLOBAL_MIN:
        return "HUGE ERROR"
    
    fault_tolerant = dict()
    
    # Find shard that contain < 2 nodes
    for cur_shard_id in shards.keys():
        nodes = shards[cur_shard_id]
       
        for j in shards:
            nodes_j = shards[j]
            # Keep adding nodes until shard reaches threshold
            # try to find shard that contains > 2 nodes 
            if len(nodes_j) > MIN_NODES_PER_SHARD and len(nodes) < MIN_NODES_PER_SHARD:
                # Sort alphabetically and redistribute to shard with < 2 nodes
                nodes_j = list(sorted(nodes_j))            
                # Move node from large partition into smaller partition
                node = nodes_j.pop()
                # Update the initial_distribution of shards
                shards[j] = set(nodes_j)
                nodes.add(node)
                fault_tolerant[j] = set(nodes_j)
        # Was unable to reach fault tolerance after redistribution
        if len(nodes) < MIN_NODES_PER_SHARD:
            return None
        # Add parition with at least min nodes needed
        if cur_shard_id not in fault_tolerant:
            fault_tolerant[cur_shard_id] = nodes
        
    return fault_tolerant

def partition_by_hash(replicas, shard_count):
    initial_distribution = dict()
    for id in range(shard_count):
        initial_distribution[id] = set()
    ring_pos = dict()
    for replica in replicas:
        # Hash each replica by address
        raw_hash = hashlib.md5(bytes(replica.encode('ascii')))
        hash_as_decimal = int(raw_hash.hexdigest(),16)
        # Calculate shard_id for replica
        shard_id = hash_as_decimal % shard_count
        initial_distribution[shard_id].add(replica)
        # calculate position on imaginary ring
        ring_pos[hash_as_decimal % HASH_OUTPUT_SPACE] = replica 
    # each partition must have at least 2 nodes
    initial_distribution = balance_shards(initial_distribution, math.floor(len(replicas)/shard_count))
    return (initial_distribution, ring_pos)

if __name__=="__main__":
    res, idk = partition_by_hash(views, 3)
    print(res)