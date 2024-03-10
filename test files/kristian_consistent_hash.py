
import hashlib
OUTPUT_SPACE = 128
partitions = {0: {'10.10.0.4:8090', '10.10.0.6:8090'}, 1: {'10.10.0.5:8090', '10.10.0.3:8090', '10.10.0.2:8090', '10.10.0.7:8090'}}
ring_positions = {57 :'10.10.0.2:8090', 87: '10.10.0.3:8090', 114: '10.10.0.4:8090', 59: '10.10.0.5:8090', 92: '10.10.0.6:8090', 85: '10.10.0.7:8090'}
def consistent_hash_key(key):
    raw_hash = hashlib.md5(bytes(key.encode('ascii')))
    hash_as_decimal = int(raw_hash.hexdigest(),16)

    key_ring_pos =  hash_as_decimal % OUTPUT_SPACE
    print(f"initial key position {key_ring_pos}")
    # walk along ring to nearest node(clockwise)
    while key_ring_pos not in ring_positions:
        key_ring_pos = (key_ring_pos + 1) % OUTPUT_SPACE
        print(f"walking to pos: {key_ring_pos}")
    print(f"reached replica {ring_positions[key_ring_pos]} at pos {key_ring_pos}")
    return ring_positions[key_ring_pos]


if __name__=="__main__":
    res = consistent_hash_key("val1")
    print(res)