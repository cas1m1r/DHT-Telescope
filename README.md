# :milky_way: DHT-Telescope
DHT-Telescope is a passive and active node crawler designed to map the decentralized topology of the BitTorrent Distributed Hash Table (DHT) network.

It peers into the fabric of peer-to-peer communication—constructing an evolving constellation of nodes, peers, and info_hashes, color-coded by ASN and observed over time. This project is a tool for researchers, digital cartographers, and curious observers of the hidden structure behind global file-sharing systems.


# :satellite: Features

:mag: Active Node Crawling – Actively queries the DHT for reachable nodes using find_node, get_peers, and ping.

:globe_with_meridians: Passive Listening – Logs all incoming DHT messages from connected peers for behavioral and structural analysis.

:art: ASN Visualization – Colors nodes by Autonomous System Number (ASN) hash, revealing geographic and organizational clustering.

:spider_web: Time-Evolving Map – Logs and snapshots node movements and density over time, allowing for playback or graph analysis.


# :toolbox: Modules

crawler.py – Recursive node discovery using Kademlia logic.

visualizer.py – Transforms logs into nodes  overlaid onto a real word map, revealing physical topology. 


# :file_folder: Data Outputs


# :rocket: Getting Started
```bash
git clone https://github.com/cas1m1r/dht-telescope.git
cd dht-telescope
pip install -r requirements.txt
python crawler.py
```

:compass: Use Cases:
* :gear: Network protocol research
* :earth_africa: Decentralized system mapping
* :closed_lock_with_key: Surveillance infrastructure awareness
* :dna: Evolution of node clusters over time
* :performing_arts: Analyzing potential misuse of DHT as a dark web mirror


# :brain: Philosophy
DHT-Telescope is not just a tool—it’s an invitation to see the unseen physics of decentralized communication. Each peer is a pixel. Each packet, a pulse. And together, they form a living map of how knowledge, files, and identity drift through the dark spaces between servers.

# :scales: Ethics & Intent
This project is for educational, analytical, and research purposes only. It does not store or share content or torrent payloads—only metadata on observable node behaviors. Use responsibly and with consent when visualizing non-public infrastructure.


# :dna: Attribution
Created by cas1m1r, 2025
Part of a suite of introspective tools including CuriosityEngine and Recursive Allegory Processor.
