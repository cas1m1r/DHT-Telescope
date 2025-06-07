import socket
import random
import bencodepy
import time
import json
from tracker import NodeTracker
from locations import *

BOOTSTRAP_NODES = [
	('router.bittorrent.com', 6881),
	('router.utorrent.com', 6881),
	('dht.transmissionbt.com', 6881),
	("dht.aelitis.com", 6881)
]


def random_node_id():
	return bytes(random.getrandbits(8) for _ in range(20))


def parse_nodes(compact_nodes):
	nodes = []
	for i in range(0, len(compact_nodes), 26):
		node_info = compact_nodes[i:i + 26]
		if len(node_info) != 26:
			continue
		nid = node_info[:20]
		ip = '.'.join(str(b) for b in node_info[20:24])
		port = int.from_bytes(node_info[24:], byteorder='big')
		nodes.append((ip, port))
	return nodes


def send_find_node(sock, addr, node_id):
	tid = b'aa'
	target_id = random_node_id()
	msg = {
		b't': tid,
		b'y': b'q',
		b'q': b'find_node',
		b'a': {
			b'id': node_id,
			b'target': target_id
		}
	}
	try:
		sock.sendto(bencodepy.encode(msg), addr)
	except Exception as e:
		print(f"[!] Failed to send to {addr}: {e}")


def listen_dht_crawler():
	node_id = random_node_id()
	visited = set()
	queue = list(BOOTSTRAP_NODES)
	t_start = time.time()
	sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
	sock.bind(('0.0.0.0', 6882))  # Use non-standard port to avoid conflict
	sock.settimeout(15)

	print("[*] Starting DHT Crawler...")

	network = {
		'Nodes': {}
	}

	while True:
		addr = queue.pop(0)
		if addr in visited:
			continue
		visited.add(addr)

		# save data
		dt = time.time() - t_start
		if round(dt) % 3 == 0:
			send_find_node(sock, addr, node_id)
			print(f'[~] Hellooooo DHT [{dt}s Elapsed]')
			# time.sleep(0.1)  # Be gentle

		if dt > 2 and len(network['Nodes'])>1 and round(dt) % 15 == 0:
			open('.lock','wb').write(b'HALT')
			print(f'[+] Logging Data...')
			open('bittorrent_network.json', 'w').write(json.dumps(network,indent=2))
			os.remove('.lock')

		try:
			data, from_addr = sock.recvfrom(65536)
			decoded = bencodepy.decode(data)
			# check queries
			if decoded.get(b'y') == b'q':
				qtype = decoded.get(b'q')
				if qtype == b'get_peers':
					info_hash = decoded[b'a'][b'info_hash']
					print(f"[+ GET_PEERS +] {addr}, info_hash: {info_hash.hex()}")
					if ip not in network['Nodes'][from_addr]['peers']:
						network['Nodes'][from_addr]['peers'].append(ip)
					if info_hash.hex() not in network['Nodes'][from_addr]['hashes'].keys():
						network['Nodes'][from_addr]['hashes'][info_hash.hex()] = {
							'addr': addr,
							'from': from_addr,
							'time': time.time()
						}
				# TODO: encode this in network{}
				elif qtype == b'announce_peer':
					try:
						info_hash = decoded[b'a'][b'info_hash']
						print(f"[+ ANNOUNCE_PEER +] {addr}, info_hash: {info_hash.hex()}")
					except TimeoutError:
						pass
			# TODO: encode this in network{}

			# check for a find_node reply
			if decoded.get(b'y') == b'r' and b'nodes' in decoded[b'r']:
				new_nodes = list(parse_nodes(decoded[b'r'][b'nodes']))
				print(f"[+ NODE REPLIED +] {from_addr[0]} [has {len(new_nodes)} peers]")
				for new_node in new_nodes:
					if new_node[0] not in network['Nodes'].keys():
						network['Nodes'][from_addr[0]] = {'peers': [n[0] for n in new_nodes], 'hashes': [], 'ip': from_addr[0], 'id': node_id.hex()}
					if new_node not in visited:
						tracker = NodeTracker(node_id)
						ip, port = new_node
						tracker.update(ip, port, responded=True)
						queue.append(new_node)
						network['Nodes'][from_addr[0]]['peers'].append(ip)
						# print(f'[+] Created Tracker for {new_node}')

		except socket.timeout:
			continue
		except Exception as e:
			print(f"[!] Error: {e}")
			continue
	print(f'[+] Logging Results')
	open('bittorrent_network.json', 'w').write(json.dumps(network, indent=2))
	print("[*] Finished crawling.")


if __name__ == '__main__':
	listen_dht_crawler()
