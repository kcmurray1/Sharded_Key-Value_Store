# CSE138 Assignment #4

# How Our System Shards Keys Across Nodes

WIP

# Failure Detection Mechanism

We implemented a pulse mechanism into our system. Every 0.5 seconds, each replica will ping all other replica addresses stored in its views. If a response is not received the replica will add the recipient to a list of downed replicas. After making a ping to all replicas, the local replica will remove all replicas in the downed list.

This ensures that all views in each live replica will eventually be consistent with one another after view endpoint requests are no longer sent.

A potential issue with this implementation is the possibility of a pulse message request to a replica that is currently up (and not crashed/down) getting lost (or the response to the pulse request) - leading to a false positive and the replica is removed from the view of the pulse-sending replica incorrectly.

## Causal Dependency Mechanism

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
*WIP - update this section later for any new system weakpoints*
- Multiple clients sending requests to the system at the same time may lead to inconsistent key-value stores amongst the replicas (causal consistency could potentially be violated).
- We did not test any multi-client executions.
- Our system assumes that broadcasts/relay functions will always perform a complete broadcast to all other nodes, and do not crash in the middle of executing.
- If a replica crashes before relaying/broadcasting a PUT or DELETE kvs request to all replicas, there is an opportunity for the remaining active replicas to have different stores. This could violate read your write consistency, as some stores may not be up to date.

## Acknowledgments:
*WIP - update this section if we consult anyone*
We did not consult anyone when developing our system.

## Citations:

- Stack Overflow Posts: [https://stackoverflow.com/questions/43644083/python-thread-running-twice-when-called-once-in-main](https://stackoverflow.com/questions/43644083/python-thread-running-twice-when-called-once-in-main)
This post on Stack Overflow helped us solve a major bug in our system where multiple instances of main were run (leading to multiple “pulse sender” threads per replica) due to the Debug parameter being set to “True” instead of “False” when running the Flask application.

- [https://stackoverflow.com/questions/36314137/how-to-start-a-thread-in-python-after-waiting-for-a-specific-delay](https://stackoverflow.com/questions/36314137/how-to-start-a-thread-in-python-after-waiting-for-a-specific-delay)
We used information from this post to compose an asynchronous pulse/heartbeat component for our system. In addition, to developing a notification protocol upon the creation of new replicas.

## Team Contributions:
*WIP - update this section*
Our team collectively designed and tested the replica system, and we worked together in pair-programming sessions on all aspects of the project.

- **Will**: Primarily focused on the causal consistency for the kvs endpoint, implemented the VectorClock datatype, and worked on parts of the heartbeat/pulse endpoint system for failure detection.

- **Kristian**: Worked on the View endpoint of the system and startup protocol upon the creation of a new Replica. As well as implementing and testing our pulse mechanism to ensure eventual consistency.
