from collections import defaultdict
import time


class NodeTracker:
    def __init__(self, my_node_id):
        self.my_node_id = my_node_id
        self.nodes = defaultdict(lambda: {
            'count': 0,
            'last_seen': 0,
            'responded': 0,
            'distance': None,
        })

    def update(self, ip, port, responded=True):
        key = f"{ip}:{port}"
        self.nodes[key]['count'] += 1
        self.nodes[key]['last_seen'] = time.time()
        if responded:
            # print(f'\t+ Adding {key}')
            self.nodes[key]['responded'] += 1
        self.nodes[key]['distance'] = self.xor_distance(self.my_node_id, ip)

    def xor_distance(self, my_id, ip):
        # Optional: compute pseudo-distance using hashed IP
        import hashlib
        ip_hash = hashlib.sha1(ip.encode()).digest()
        return int.from_bytes(bytes(a ^ b for a, b in zip(my_id, ip_hash)), 'big')

    def top_nodes(self, n=10):
        return sorted(self.nodes.items(), key=lambda x: x[1]['count'], reverse=True)[:n]