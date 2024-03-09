"""
Trying out merging and updating new vector clock format {shard_id_2: vc_1, shard_id_2, vc_2}
"""
from vectorclock import VectorClock

new_replica_vc = {0 : {'alice': 2}, 1: dict(), 2: dict()}
shard_id = 0


shards = {0: {'dave', 'erin'}, 1: {'carol', 'frank'}, 2: {'bob', 'alice'}}

def combine_vc(local, responder):
    # iterate through each shard vc
    for id in responder:
        # only update if the shard vc is larger than local one
        local_vc = local[id]
        received_vc = responder[id]

        # pad local clock {a: 0, b:0}
        local_vc.update(received_vc)
        # pad received clock {b: 0, a:0}
        received_vc.update(local_vc)
        print(f"Compare local_vc {local_vc} with recv_vc {received_vc} at shard id {id}")
        if not local_vc and received_vc:
              local[id] = received_vc
        if VectorClock(local_vc) <= VectorClock(received_vc):
                local[id] = received_vc

if __name__ == "__main__":
    new_replica_vc
    response_vc = {1 : {'carol': 0}, 0 : dict(), 2: dict()}
    combine_vc(new_replica_vc, response_vc)

    print(f"new replica vc {new_replica_vc}")

    response_vc = {1 : {'carol': 1, 'frank': 2}, 0 : dict(), 2: {'bob': 2}}
    combine_vc(new_replica_vc, response_vc)
    print(f"new replica vc {new_replica_vc}")