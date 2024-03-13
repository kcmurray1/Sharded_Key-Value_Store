idk = {0: {'dave', 'erin'}, 1: {'frank', 'bob', 'alice', 'carol', 'erin'}}
replicas = {"alice" : "10.10.0.2", "bob" : "10.10.0.3", "carol": "10.10.0.4",
            "dave":"10.10.0.5", "erin" : "10.10.0.6", "frank" : "10.10.0.7", "grace" : "10.10.0.7"}

shards = {0: {"alice", "bob"},1:{"carol",
            "dave", "erin", "frank", "grace"}}
shards_adv = {0: {"alice", "bob"},1:{"carol",
            "dave", "erin", "frank", "grace"}}
import math
import hashlib
"""
N = 3
{1,4}
{5,6,2,7}
{10,8,3,9}
# want to reshard N = 4
# only redistribute {2,7,3,9} among 4 shards
# randomly pick 2 to be assigned to new shard, so really, redistributed {2,7} among 4 shards

# approach 1
{1,4}
{5,6}
{10,8,3,9}
{2,7}

# approahc 2
{1,4}
{5,6,2}
{10,8,3}
{7,9}

# approach 3
# 2 and 7 will be redistributed
{1,4}
{5,6}
{10,8}
{9,3}

# Find ideal number of nodes per shard (N/# replicas)
# Find any shard less than ideal number
# find shards that have more
# more some nodes from bigger to less than ideal

"""
NUM_SHARDS = 3
MIN_NODES_PER_SHARD = len(replicas)//(NUM_SHARDS)
print(MIN_NODES_PER_SHARD)
# def balance_partitions(shards, N):
#     """
#     Returns:
#         dictionary containing N shards with at least 2 nodes assigned
#         Else returns None
#     """
#     # If n changes add it to shards
#     if len(shards) != N:
#         shards[N] = set()
#     print("Trying to balance among us ", shards)
#     fault_tolerant = dict()
#     num_nodes = 0
#     for key in shards:
#         num_nodes += len(shards[key])
#     res = pidgeon_hole(num_nodes, N)
#     pot = list()
#     for key in shards.keys():
#         length = len(shards[key])
#         if length not in res:
#             # if greater
#             if length > max(res):
#                 nodes = sorted(list(shards[key]))
#                 pot.append(nodes.pop())
#                 shards[key] = nodes

#             # if less
#             if length < 2 and pot:
#                 shards[key].add(pot.pop())

#         else:
#             res.remove(length)
#     print(res)


            


#     return shards
    
def pidgeon_hole(n, n_bins): 
    quotient = n // n_bins
    remainder = n % n_bins
    print(quotient, remainder)
    bins = [quotient for i in range(n_bins)]    
    for i in range(remainder):
        bins[i] += 1
    print(bins)
    return bins

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
       # if len(nodes) < MIN_NODES_PER_SHARD:
            #return None
        # Add parition with at least min nodes needed
        fault_tolerant[cur_shard] = nodes
    return fault_tolerant

def assign_shard_ids(n_shards):
    distribution = dict()
    for id in range(NUM_SHARDS):
        distribution[id] = set()
    for i in range(n_shards):
        distribution[i] = set()
    for replica in replicas:
        raw_hash = hashlib.md5(bytes(replicas[replica].encode('ascii')))
        hash_as_decimal = int(raw_hash.hexdigest(),16)
        shard_id = hash_as_decimal % n_shards
        distribution[shard_id].add(replica)
    return distribution

replicas = {"alice" : "10.10.0.2", "bob" : "10.10.0.3", "carol": "10.10.0.4",
            "dave":"10.10.0.5", "erin" : "10.10.0.6", "frank" : "10.10.0.7", "grace" : "10.10.0.7"}
if __name__=="__main__":
    # res = balance_partitions(shards,3)
    # print(res)
    y = {0: {'grace', 'dave', 'erin'}, 1: {'bob', 'frank', 'carol', 'alice'}}
    res = assign_shard_ids(NUM_SHARDS)
    res = balance_partitions(res)
    print(res)