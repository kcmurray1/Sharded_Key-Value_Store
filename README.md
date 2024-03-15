# CSE138 Assignment #4

# Key-to-shard mapping Mechanism

We implemented consistent hashing using MD5,with a 128 output space, to hash the positions of the nodes on the imaginary ring and to assign keys to nodes that hash close to it. All nodes in our system have a dictionary that follows the format: 

Ring_positions = `{node_hash_1: node_addr1, node_hash2: node_addr2…}`

So, upon hashing a key, we “walk” along the imaginary ring by updating its position(incrementing its value by 1) until we reach a key in ring_positions. The node that performs this then compares the address associated with the hash and its own address to determine whether it needs to forward the request or process it locally.

We chose this design because it seemed the most intuitive and it was covered in class.

# Resharding Mechanism

Upon receiving a reshard request a node will relay the request to to all nodes in the system. Independently each shard will recalculate:
## Shards, Shard_id
- Regardless of the number of shards, N, changes during a reshard, our system will attempt to balance the stores by floor(# of nodes/partitions). Every node then contains an updated shard distribution. Nodes use this new shard distribution to determine how to proceed.
## Store
- We added another attribute to each node called _substore which contains keys that ONLY hash to the node. To reduce rehashing we update the _substore along with the regular _store during any PUT and DELETE to the /kvs endpoint. Thus, when we reshard, every node will refer to their new shard distribution to retrieve _substores from new members as well as share their _substore.
## Local vector clocks
- Again, using the newly distributed shards, each node will change their vector clock to contain their new members.
Because during a reshard, there will be no requests on the key-value store to deliver, we have made the assumption that all nodes’ causal metadata will be up to date with each other and that we effectively start with a clean slate. Based on this assumption, we decided to reset the vector clocks at each node to 0 after each reshard.
Before resharding a node may have a local vc of:
`{'10.10.0.4:8090': 14, '10.10.0.6:8090': 3, '10.10.0.7:8090': 5}`
After resharding they will now have:
`{'10.10.0.4:8090': 0, '10.10.0.6:8090': 0, '10.10.0.7:8090': 0}`

Additionally, when a client sends any request afterward, we update their metadata such that it agrees with the new arrangement of our system.

The rationale behind this design was to reduce the need for rehashing to maintain the node’s ability to independently calculate the shards in the system.


# Failure Detection Mechanism

We implemented a pulse mechanism into our system. Every 0.5 seconds, each replica will ping all other replica addresses stored in its views. If a response is not received the replica will add the recipient to a list of downed replicas. After making a ping to all replicas, the local replica will remove all replicas in the downed list.

This ensures that all views in each live replica will eventually be consistent with one another after view endpoint requests are no longer sent.

A potential issue with this implementation is the possibility of a pulse message request to a replica that is currently up (and not crashed/down) getting lost (or the response to the pulse request) - leading to a false positive and the replica is removed from the view of the pulse-sending replica incorrectly.

## Causal Dependency Mechanism(slightly updated in assignment 4)

## New changes from assignment 4
Some modifications to causal metadata - we now only store values at the positions of the members of the same shard.
So now client metadata takes the form:

Client = `{'0': {'10.10.0.4:8090': 14, '10.10.0.6:8090': 3, '10.10.0.7:8090': 5}, '`
`1': {'10.10.0.2:8090': 54, '10.10.0.3:8090': 2}, '`
`'2': {'10.10.0.5:8090': 3, '10.10.0.8:8090': 16}}`

Our nodes, perform vector clock comparison the same by indexing the id its shard

For example, if a node in partition 1 is processing the client’s request. We check for dependencies by comparing 
Client[‘1’] = `{'10.10.0.2:8090': 54, '10.10.0.3:8090': 2}`

We then proceed with the following protocol

## End of new changes from assignment 4


In our system, our implementation of vector clocks consists of a dictionary where replica addresses are the keys with integer values corresponding to tracked events which in this case are msg deliveries.
Example: `{'10.10.0.4:8090': 0, '10.10.0.3:8090': 0, '10.10.0.2:8090': 1}`

### Causal Metadata:

For the KVS endpoint, we chose to have our causal metadata consist of a vector clock paired with a “sender” tag to deal with causal dependency. The sender tag is used to discern whether a request is from a replica or a client since they follow slightly different delivery protocols. The vector clock is used for comparison to establish causal dependency.

Examples:

- From client: `{“vc”: {'10.10.0.4:8090': 0, '10.10.0.3:8090': 0, '10.10.0.2:8090': 1} }`
- From replica: `{“sender” : '10.10.0.2:8090' , “vc”:  {'10.10.0.2:8090': 0, '10.10.0.3:8090': 0} }`

### Checking for Causal Dependency

When a replica receives a request that has no causal metadata (i.e., set to None), our system will deliver the request since there is no causal dependency.

When causal metadata is present (i.e., not None), then our system will check for additional metadata called “sender” which includes the address of the sending replica. Upon receiving causal metadata from a client (“sender” is absent), our system will first pad the sender’s vector clock with the replica’s vector clock.

For example, if replica Alice receives causal metadata `{“vc” : {a: 1}}` and has a local vector clock of `{a: 1, b: 1}`, then Alice will compare `{a: 1, b: 0}` with `{a:1, b:1}`.

Replicas will perform a broadcast/relay upon receiving a client PUT or DELETE request that has no causal dependencies, by buffering messages at the sender. Replicas that receive a broadcast/relay will attempt to deliver if no causal dependencies are found. Otherwise, it will return a 503 error.

#### The pseudocode for checking causal dependency upon receiving a request with metadata:

- If request from client
  - Pad the client_vc and replica_vc if they are unequal length (consist of different elements and therefore have different views)
  - No causal dependency if client_vc <= replica_vc, can proceed to deliver the incoming request
  - Else, there is a causal dependency, so the system will return a response with a 503 error
- Else
  - If sender_replica_vc and receiver_replica_vc consist of different elements
    - There is a causal dependency
  - If sender_replica_vc[sender_addr] != receiver_replica_vc[sender_addr] + 1
    - There is a causal dependency
  - If there exists k such that sender_replica_vc[k] > receiver_replica_vc[k] (where k != sender_addr)
    - There is a causal dependency
  - Otherwise, there is no causal dependency and the request can be delivered

## Possible Points of Failure Our System May be Sensitive to:
- Multiple clients sending requests to the system at the same time may lead to inconsistent key-value stores amongst the replicas (causal consistency could potentially be violated).
- We did not test any multi-client executions.
- Our system assumes that broadcasts/relay functions will always perform a complete broadcast to all other nodes, and do not crash in the middle of executing.
- If a replica crashes before relaying/broadcasting a PUT or DELETE kvs request to all replicas, there is an opportunity for the remaining active replicas to have different stores. This could violate read your write consistency, as some stores may not be up to date.
- Our system does not have a functional mechanism to rebalance the shards if their key-counts are not evenly-distributed. As a result, certain “hot-spot” nodes may become overburdened with larger stores while other nodes’ stores are less-used. We attempted to overcome this by having our sharding mechanism try to divide up the shards into partitions each with an equal number of nodes.


## Acknowledgments:
We did not consult anyone when developing our system.

## Citations:

- Stack Overflow Posts: [https://stackoverflow.com/questions/43644083/python-thread-running-twice-when-called-once-in-main](https://stackoverflow.com/questions/43644083/python-thread-running-twice-when-called-once-in-main)
This post on Stack Overflow helped us solve a major bug in our system where multiple instances of main were run (leading to multiple “pulse sender” threads per replica) due to the Debug parameter being set to “True” instead of “False” when running the Flask application.

- [https://stackoverflow.com/questions/36314137/how-to-start-a-thread-in-python-after-waiting-for-a-specific-delay](https://stackoverflow.com/questions/36314137/how-to-start-a-thread-in-python-after-waiting-for-a-specific-delay)
We used information from this post to compose an asynchronous pulse/heartbeat component for our system. In addition, to developing a notification protocol upon the creation of new replicas.

- [https://stackoverflow.com/questions/22281059/set-object-is-not-json-serializable](https://stackoverflow.com/questions/22281059/set-object-is-not-json-serializable)
We were having trouble sending shard data since we formatted it as a dictionary where keys were strings and values were sets. This post helped us understand we needed to convert the sets into a different datatype. We chose to “encode” sets as lists and then “decode” from lists to sets. We perform this in our custom methods to_jason_friendy_shard_dict(shard) and to_jason_unfriendy_shard_dict(shard) in our views_route.py file.


## Team Contributions:
Our team collectively designed and tested the SYSTEM, and we worked together in pair-programming sessions on all aspects of the project.

- **Will**: Worked on shard endpoints, and also worked on add-member and resharding mechanisms.


- **Kristian**: Worked on consistent hashing and partitioning by hash mechanisms, and updated use of vector clocks as part of causal metadata to handle new sharding scheme.
