#!/usr/bin/env python3
# dht_crawler_v4_explorer.py — DHT harvester + active explorer + inline BEP-9 resolver
#
# Key changes from v3:
# - Actively queries the DHT (get_peers) for info_hashes we observe to harvest
#   compact peer "values" and then resolves metadata from those peers.
# - Chooses query targets by XOR distance to the info_hash (Kademlia-style) using
#   node IDs we learn, instead of random nodes — much higher hit rate.
# - Tracks transaction IDs so we can associate get_peers responses with the
#   info_hash we asked about.
# - Logs and resolves peers discovered via "values" (DB source='values').
# - Slightly more client-like peer_id (selectable fingerprints) during BEP-9.
# - Safer send/receive windows and backoffs to avoid de-prioritization.
#
# Expected effect: you should still see lots of info-hashes, but now some
# percentage should start resolving (metadata table no longer empty). Success
# rate will still be modest because many peers are NATed, uTP-only, or require
# encryption, but this materially improves yield without needing a separate
# resolver process.

import os, sys, time, json, hmac, hashlib, random, socket, selectors, argparse, sqlite3, struct, threading, queue, signal
from typing import Dict, Any, Tuple, Optional, List, Deque
from collections import deque
import libtorrent as lt

try:
    import bencodepy  # faster if available
except Exception:
    bencodepy = None
global FS_STATE
BOOTSTRAP_NODES = [
    ("router.bittorrent.com", 6881),
    ("router.utorrent.com", 6881),
    ("dht.aelitis.com", 6881),
    ("dht.anacrolix.link", 42069),
    ("dht.transmissionbt.com", 6881),
    ("dht.libtorrent.org", 25401),
    ("router.nuh.dev", 6881),
    ("bttracker.debian.org", 6881),
    ("212.129.33.59", 6881),  # router.utorrent.com backup IP
]

# ---------------- Tunables ----------------
K = 16                        # nodes in candidate replies
DEFAULT_PORT = 6881
DEPTH_DEFAULT = 6e6        # warmup sends before switching to HARVEST
SEND_WINDOW = 128             # in-flight KRPC cap
RECV_BATCH = 256              # recv drain per select tick
LOG_EVERY_SEC = 15
RATE_WINDOW_SEC = 60
TOKEN_ROTATE_SEC = 3600

REFRESH_EVERY = 15.0          # ping a few known nodes even in HARVEST
RESEED_EVERY = 5.0            # reseed crawl from good nodes when queue dries
RESEED_FANOUT = 8

DB_PATH_DEFAULT = "dht_observatory.db"
STATE_FILE_DEFAULT = "bittorrent_state.json"

# Resolver
RESOLVER_WORKERS_DEFAULT = 12
RESOLVE_TIMEOUT = 12         # per TCP peer timeout (a touch higher)
RESOLVE_BACKOFF_S = 15 * 60   # avoid spamming same (ih,ip,port)

# Explorer
EXPLORE_TICK_S = 0.05         # how often to try sending a get_peers
EXPLORE_FANOUT = 8            # number of closest nodes to ask per IH burst
EXPLORE_COOLDOWN_S = 30.0     # don't re-explore same IH too fast
TID_BYTES = 4                 # KRPC transaction id length (was 2)
MAX_OUTSTANDING_T = 1024      # cap how many unmatched queries are inflight

# Optional: sample some get_peers senders for resolution too (kept for continuity)
RESOLVE_FROM_GETPEERS_EVERY = 0   # 0 disables; we now rely on 'values'

# ---------------- Utilities ----------------
def random_node_id() -> bytes:
    return os.urandom(20)

def parse_nodes(compact: bytes) -> List[Tuple[str,int,bytes]]:
    out = []
    for i in range(0, len(compact), 26):
        s = compact[i:i+26]
        if len(s) != 26:
            continue
        nid = s[:20]
        ip = ".".join(str(b) for b in s[20:24])
        port = int.from_bytes(s[24:], "big")
        out.append((ip, port, nid))
    return out

def compact_node(ip: str, port: int, node_id: bytes) -> bytes:
    return node_id + bytes(int(x) for x in ip.split(".")) + int(port).to_bytes(2, "big")

def parse_values(values_list) -> List[Tuple[str,int]]:
    peers = []
    if not isinstance(values_list, list):
        return peers
    for v in values_list:
        if isinstance(v, (bytes, bytearray)) and len(v) == 6:
            ip = ".".join(str(b) for b in v[:4])
            port = int.from_bytes(v[4:], "big")
            peers.append((ip, port))
    return peers

# ---------------- Minimal bencode ----------------
def benc_enc(x) -> bytes:
    if isinstance(x, int): return b"i%de" % x
    if isinstance(x, (bytes, bytearray)):
        b = bytes(x)
        return str(len(b)).encode() + b":" + b
    if isinstance(x, str): return benc_enc(x.encode())
    if isinstance(x, list): return b"l" + b"".join(benc_enc(i) for i in x) + b"e"
    if isinstance(x, dict):
        items = []
        for k in sorted(x.keys(), key=lambda k: k if isinstance(k, (bytes, bytearray)) else str(k).encode()):
            kb = k if isinstance(k, (bytes, bytearray)) else str(k).encode()
            items.append(benc_enc(kb) + benc_enc(x[k]))
        return b"d" + b"".join(items) + b"e"
    raise TypeError(f"Unsupported bencode type: {type(x)}")

def benc_dec(buf: bytes, i: int = 0):
    def parse(i):
        c = buf[i:i+1]
        if c == b"i":
            j = buf.index(b"e", i+1)
            return int(buf[i+1:j]), j+1
        if c == b"l":
            i += 1; out = []
            while buf[i:i+1] != b"e":
                v, i = parse(i); out.append(v)
            return out, i+1
        if c == b"d":
            i += 1; out = {}
            while buf[i:i+1] != b"e":
                k, i = parse(i)
                if not isinstance(k, (bytes, bytearray)):
                    raise ValueError("dict key must be bytes")
                v, i = parse(i)
                out[k] = v
            return out, i+1
        if 48 <= buf[i] <= 57:
            j = buf.index(b":", i)
            ln = int(buf[i:j]); s = j+1; e = s+ln
            return buf[s:e], e
        raise ValueError(f"invalid bencode at {i}")
    obj, idx = parse(i)
    if idx != len(buf): raise ValueError("trailing bytes")
    return obj

def krpc_encode(obj: dict) -> bytes:
    if bencodepy:
        return bencodepy.encode(obj)
    return benc_enc(obj)

def krpc_decode(buf: bytes) -> dict:
    if bencodepy:
        return bencodepy.decode(buf)
    obj = benc_dec(buf)
    if not isinstance(obj, dict): raise ValueError("not a dict")
    return obj

# ---------------- Token box ----------------
class TokenBox:
    def __init__(self, secret=None, since=None):
        now = time.time()
        self._secrets = [(since or now, secret or os.urandom(16))]
    def _active(self):
        now = time.time()
        ts, sec = self._secrets[0]
        if now - ts > TOKEN_ROTATE_SEC:
            self._secrets.insert(0, (now, os.urandom(16)))
            self._secrets = self._secrets[:2]
        return [s for _, s in self._secrets]
    def issue(self, ip: str) -> bytes:
        return hmac.new(self._active()[0], ip.encode(), hashlib.sha1).digest()
    def valid(self, ip: str, token: bytes) -> bool:
        for sec in self._active():
            if hmac.new(sec, ip.encode(), hashlib.sha1).digest() == token:
                return True
        return False
    def current(self):
        return self._secrets[0][1], self._secrets[0][0]

# ---------------- DB helpers ----------------
SCHEMA_SQL = """
PRAGMA journal_mode=WAL;
PRAGMA synchronous=NORMAL;

CREATE TABLE IF NOT EXISTS announces (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  ts INTEGER NOT NULL,
  ip TEXT NOT NULL,
  port INTEGER NOT NULL,
  info_hash TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'pending',  -- pending|working|ok|error
  attempts INTEGER NOT NULL DEFAULT 0,
  last_attempt_ts INTEGER,
  source TEXT
);
CREATE INDEX IF NOT EXISTS idx_ann_status ON announces(status, ts);
CREATE INDEX IF NOT EXISTS idx_ann_ih ON announces(info_hash);

CREATE TABLE IF NOT EXISTS getpeers (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  ts INTEGER NOT NULL,
  ip TEXT NOT NULL,
  port INTEGER NOT NULL,
  info_hash TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_gp_ts ON getpeers(ts);

CREATE TABLE IF NOT EXISTS metadata (
  info_hash TEXT PRIMARY KEY,
  name TEXT,
  total_length INTEGER,
  piece_length INTEGER,
  private INTEGER,
  files_json TEXT,
  first_seen_ts INTEGER,
  last_resolved_ts INTEGER
);
"""

def db_open(path: str):
    con = sqlite3.connect(path, timeout=30, isolation_level=None)
    con.execute("PRAGMA journal_mode=WAL;")
    con.execute("PRAGMA synchronous=NORMAL;")
    con.executescript(SCHEMA_SQL)
    return con

def db_log_getpeers(db_path, ih_hex, ip, port):
    con = db_open(db_path)
    con.execute("INSERT INTO getpeers(ts,ip,port,info_hash) VALUES(?,?,?,?)",
                (int(time.time()), ip, int(port), ih_hex))
    con.close()

def db_log_announce(db_path, ih_hex, ip, port, status="pending", source="announce"):
    con = db_open(db_path)
    con.execute("""INSERT INTO announces(ts,ip,port,info_hash,status,attempts,last_attempt_ts,source)
                   VALUES(?,?,?,?,?,0,?,?)""",
                (int(time.time()), ip, int(port), ih_hex, status, int(time.time()), source))
    con.close()

def db_mark_announce(db_path, ih_hex, ip, port, status):
    con = db_open(db_path)
    con.execute("""UPDATE announces SET status=?, attempts=attempts+1, last_attempt_ts=?
                   WHERE info_hash=? AND ip=? AND port=? AND status!='ok'""",
                (status, int(time.time()), ih_hex, ip, int(port)))
    con.close()

def db_upsert_metadata(db_path, ih_hex: str, info_dict: Dict[bytes,Any]):
    # Extract a readable summary
    name = info_dict['name']
    piece_len = int(info_dict.get(b"piece length") or 0)
    private = int(info_dict.get(b"private") or 0)
    total_len = 0
    files_json = "[]"
    if b"files" in info_dict:
        files = []
        for f in info_dict[b"files"]:
            ln = int(f.get(b"length") or 0)
            parts = f.get(b"path", [])
            path = "/".join(p.decode(errors="replace") if isinstance(p,(bytes,bytearray)) else str(p) for p in parts) if isinstance(parts, list) else ""
            files.append({"length": ln, "path": path})
            total_len += ln
        files_json = json.dumps(files, ensure_ascii=False)
    else:
        total_len = int(info_dict.get(b"length") or 0)
    now = int(time.time())
    con = db_open(db_path)
    con.execute("""
        INSERT INTO metadata(info_hash, name, total_length, piece_length, private, files_json, first_seen_ts, last_resolved_ts)
        VALUES(?,?,?,?,?,?,?,?)
        ON CONFLICT(info_hash) DO UPDATE SET
          name=excluded.name,
          total_length=excluded.total_length,
          piece_length=excluded.piece_length,
          private=excluded.private,
          files_json=excluded.files_json,
          last_resolved_ts=excluded.last_resolved_ts;
    """, (ih_hex, name, total_len, piece_len, private, files_json, now, now))
    con.close()

# ---------------- TCP metadata (BEP-9) ----------------
BT_PSTR = b"BitTorrent protocol"
RESERVED = bytearray(b"\x00"*8); RESERVED[5] |= 0x10; RESERVED = bytes(RESERVED)
MSG_EXTENDED = 20
EXT_HANDSHAKE_ID = 0
METADATA_PIECE_LEN = 16384

# Client fingerprints (you may add more)
CLIENT_IDS = [b"-TR4000-", b"-qB4400-", b"-UT3530-", b"-LT2000-"]

def _send(sock: socket.socket, buf: bytes): sock.sendall(buf)

def _recv_all(sock: socket.socket, ln: int) -> bytes:
    buf = b""
    while len(buf) < ln:
        chunk = sock.recv(ln - len(buf))
        if not chunk: raise ConnectionError("socket closed")
        buf += chunk
    return buf

def _bt_handshake(sock: socket.socket, info_hash: bytes, peer_id: bytes):
    hs = bytes([len(BT_PSTR)]) + BT_PSTR + RESERVED + info_hash + peer_id
    _send(sock, hs)
    resp = _recv_all(sock, 68)
    if len(resp) != 68 or resp[0] != len(BT_PSTR) or resp[1:20] != BT_PSTR:
        raise ConnectionError("bad bt handshake")
    if not (resp[20 + 5] & 0x10):
        raise ConnectionError("peer lacks extension protocol (BEP-10)")

def _bt_read(sock: socket.socket) -> Tuple[Optional[int], bytes]:
    ln = struct.unpack(">I", _recv_all(sock, 4))[0]
    if ln == 0:
        return None, b""
    payload = _recv_all(sock, ln)
    return payload[0], payload[1:]

def _bt_ext(sock: socket.socket, ext_id: int, d: dict, raw: bytes=b""):
    payload = benc_enc(d) + raw
    _send(sock, struct.pack(">IBB", 1 + len(payload), MSG_EXTENDED, ext_id) + payload)

def filesystem_state(path, data):
    if path not in data.keys():
        data[path] = []
    for e in os.listdir(path):
        f = os.path.join(path, e)
        if os.path.isdir(f):
            data = filesystem_state(f, data)
        else:
            data[path].append(f)
    return data




def compare_filesystem_state(before, after, doDelete):
    new_elements = {'folders': [], 'files':[]}
    for folder in after.keys():
        # check if folder exists
        if folder not in before.keys():
            new_elements['folders'].append(folder)
            for child in after[folder]:
                new_elements['files'].append(child)
                if doDelete:
                    print(f'[-] Deleting {child}')
                    os.remove(child)
        # check if any parts of folder changed
        for file in after[folder]:
            try:
                if file not in before[folder]:
                    new_elements['files'].append(file)
                    if doDelete:
                        print(f'[-] Deleting {file}')
                        os.remove(file)
            except:
                pass
    if doDelete:
        for dir in new_elements['folders']:
            os.rmdir(dir)
            print(f'[-] Deleting {dir}')
    return new_elements

def fetch_metadata_from_peer(ip: str, port: int, ih_hex: str) -> bytes:
    """Return the raw bencoded 'info' dict for ih_hex from a single peer, or raise."""
    ih = bytes.fromhex(ih_hex)
    pid_prefix = random.choice(CLIENT_IDS)
    peer_id = pid_prefix + os.urandom(20 - len(pid_prefix))
    with socket.create_connection((ip, int(port)), timeout=RESOLVE_TIMEOUT) as s:
        s.settimeout(RESOLVE_TIMEOUT)
        _bt_handshake(s, ih, peer_id)
        _bt_ext(s, EXT_HANDSHAKE_ID, {b"m": {b"ut_metadata": 1}})
        ut_md = None; md_size = None
        t0 = time.time()
        while time.time() - t0 < RESOLVE_TIMEOUT:
            mid, payload = _bt_read(s)
            if mid != MSG_EXTENDED:
                continue
            ext_id = payload[0]
            header = benc_dec(payload[1:])
            if ext_id == EXT_HANDSHAKE_ID:
                m = header.get(b"m", {}) or {}
                ut_md = m.get(b"ut_metadata")
                md_size = header.get(b"metadata_size")
                if isinstance(ut_md, int) and isinstance(md_size, int):
                    break
        if not isinstance(ut_md, int) or not isinstance(md_size, int):
            raise ConnectionError("no ut_metadata or size")
        npieces = (md_size + METADATA_PIECE_LEN - 1) // METADATA_PIECE_LEN

        def req_piece(i: int):
            _bt_ext(s, ut_md, {b"msg_type": 0, b"piece": i})
            t_end = time.time() + RESOLVE_TIMEOUT
            while time.time() < t_end:
                mid, payload = _bt_read(s)
                if mid != MSG_EXTENDED or payload[0] != ut_md:
                    continue
                # ut_metadata: header dict + piece bytes
                header, idx = benc_dec(payload[1:], 0)
                if not isinstance(header, dict):
                    continue
                msg_type = int(header.get(b"msg_type", -1))
                piece_idx = int(header.get(b"piece", -1))
                if msg_type == 1 and piece_idx == i:
                    # find where header ended: re-encode header for offset
                    enc_hdr = benc_enc(header)
                    off = 1 + len(enc_hdr)
                    return header.get(b"total_size"), payload[off:]
                if msg_type == 2:
                    return None, None
            return None, None

        pieces: Dict[int, bytes] = {}
        # If total unknown, request piece 0 first to learn total_size
        if md_size <= 0:
            tot, p0 = req_piece(0)
            if tot is None or p0 is None:
                raise ConnectionError("piece 0 rejected")
            md_size = int(tot)
            npieces = (md_size + METADATA_PIECE_LEN - 1) // METADATA_PIECE_LEN
            pieces[0] = p0

        if md_size > 4 * 1024 * 1024:
            raise ConnectionError("metadata too large")

        for i in range(npieces):
            if i in pieces: continue
            tot, pb = req_piece(i)
            if pb is None:
                raise ConnectionError("piece rejected")
            pieces[i] = pb

        buf = bytearray(md_size)
        for i, chunk in pieces.items():
            start = i * METADATA_PIECE_LEN
            buf[start:start+len(chunk)] = chunk[:max(0, min(len(chunk), md_size - start))]

        # verify against ih
        info_obj = benc_dec(bytes(buf))
        raw_info = benc_enc(info_obj)
        if hashlib.sha1(raw_info).digest() != ih and hashlib.sha1(bytes(buf)).digest() != ih:
            raise ValueError("info hash mismatch")
        return raw_info  # canonical bencoded info dict

# ---------------- Resolver pool ----------------
class ResolverPool:
    def __init__(self, db_path: str):
        self.q: "queue.Queue[Tuple[str,str,int]]" = queue.Queue()
        self.db_path = db_path
        self.seen_recent: Dict[Tuple[str,str,int], float] = {}
        self.workers: List[threading.Thread] = []
        self.stop = False

    def start(self, n=RESOLVER_WORKERS_DEFAULT):
        for i in range(max(1, int(n))):
            t = threading.Thread(target=self._worker, name=f"resolver-{i}", daemon=True)
            t.start()
            self.workers.append(t)

    def enqueue(self, ih_hex: str, ip: str, port: int):
        key = (ih_hex, ip, int(port))
        now = time.time()
        last = self.seen_recent.get(key, 0.0)
        if now - last < RESOLVE_BACKOFF_S:
            return
        self.seen_recent[key] = now
        self.q.put((ih_hex, ip, int(port)))

    def _worker(self):
        while not self.stop:
            try:
                ih_hex, ip, port = self.q.get(timeout=0.25)
            except queue.Empty:
                continue
            try:
                db_mark_announce(self.db_path, ih_hex, ip, port, "working")
                try:
                    # raw_info = fetch_metadata_from_peer(ip, port, ih_hex)
                
                    raw_info = get_metadata_from_infohash(infohash_hex=ih_hex,timeout=30)
                    print(raw_info)
                    info_dict = raw_info['name'].decode('utf-8')  # dict
                    info_dict = benc_dec(info_dict)
                    db_upsert_metadata(self.db_path, ih_hex, info_dict)
                    db_mark_announce(self.db_path, ih_hex, ip, port, "ok")
                    print(f"[RESOLVED] {ih_hex[:16]}..  <- {ip}:{port}")
                except Exception as e:
                    db_mark_announce(self.db_path, ih_hex, ip, port, "error")
                    print(f"[resolve-fail] {ih_hex[:16]}.. {ip}:{port}  {e}")
            except Exception as e:
                print(f"[resolver worker] {e}")
            finally:
                try: self.q.task_done()
                except Exception: pass


def get_metadata_from_infohash(infohash_hex, timeout=30):
    ses = lt.session()
    ses.listen_on(6881, 6891)
    # enable DHT bootstrap (default btp routers are used), optionally add your bootstraps
    params = {
        'save_path': '.',
        'storage_mode': lt.storage_mode_t.storage_mode_sparse
    }
    magnet = f"magnet:?xt=urn:btih:{infohash_hex}"
    handle = lt.add_magnet_uri(ses, magnet, params)
    result = {}
    t0 = time.time()
    while not handle.has_metadata() and (time.time()-t0) < timeout:
        s = ses.wait_for_alert(1000)  # wait for alerts
        time.sleep(0.1)
    if not handle.has_metadata():
        raise TimeoutError("no metadata")
    torinfo = handle.get_torrent_info()
    # gather file list
    files = []
    for f in torinfo.files():
        files.append({"path": f.path, "size": f.size})
    result = {"name": torinfo.name(), "files": files, "total": torinfo.total_size()}
    fs_current = filesystem_state(os.getcwd(), {})
    return result


# ---------------- Main Harvester + Explorer ----------------
def main():
    ap = argparse.ArgumentParser(description="DHT harvester + explorer + ut_metadata resolver")
    ap.add_argument("--port", type=int, default=DEFAULT_PORT, help="UDP port to bind")
    ap.add_argument("--bind", type=str, default="0.0.0.0", help="Bind address")
    ap.add_argument("--depth", type=int, default=DEPTH_DEFAULT, help="Warmup sends before harvesting")
    ap.add_argument("--db", type=str, default=DB_PATH_DEFAULT, help="SQLite DB path")
    ap.add_argument("--workers", type=int, default=RESOLVER_WORKERS_DEFAULT, help="Resolver worker threads")
    ap.add_argument("--id-file", type=str, default="node_id.bin", help="Persisted DHT node id file")
    args = ap.parse_args()

    # Node ID & tokens
    if os.path.exists(args.id_file):
        my_id = open(args.id_file, "rb").read()
        if len(my_id) != 20:
            my_id = os.urandom(20)
            open(args.id_file, "wb").write(my_id)
    else:
        my_id = os.urandom(20); open(args.id_file, "wb").write(my_id)
    tokens = TokenBox()

    # Log filesystem initial state to delete anything dropped
    
    FS_STATE = filesystem_state(os.getcwd(), {})

    # DB init
    db_open(args.db).close()

    # UDP
    sel = selectors.DefaultSelector()
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.setblocking(False)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 8 * 1024 * 1024)
    sock.bind((args.bind, args.port))
    sel.register(sock, selectors.EVENT_READ)

    # State
    queue_nodes: List[Tuple[str,int]] = list(BOOTSTRAP_NODES)
    visited = set()
    nodes: Dict[str, Dict[str, Any]] = {}   # ip -> {port,id,last_seen}
    in_flight = 0
    inflight_t = []  # timestamps of sends (for simple aging)
    found = 0
    listen_mode = False
    good_nodes = set()

    stats = {"ping":0, "find_node":0, "get_peers":0, "announce_peer":0, "unique_ih": set()}
    t0 = time.time()
    last_log = last_rate = last_refresh = last_reseed = 0.0

    # Resolver pool
    pool = ResolverPool(args.db)
    pool.start(args.workers)

    # Explorer state
    explore_q: Deque[str] = deque()   # ih_hex to explore
    explore_seen: Dict[str, float] = {}
    t_to_ih: Dict[bytes, Tuple[str,float]] = {}  # tid -> (ih_hex, ts)

    # Helpers
    def age_inflight():
        nonlocal in_flight
        if not inflight_t: return
        now = time.time()
        keep = []
        for ts in inflight_t:
            if now - ts > 7.5:
                in_flight = max(0, in_flight - 1)
            else:
                keep.append(ts)
        inflight_t[:] = keep

    def send(msg: dict, addr: Tuple[str,int]):
        nonlocal in_flight
        try:
            sock.sendto(krpc_encode(msg), addr)
            in_flight += 1
            inflight_t.append(time.time())
        except Exception as e:
            print(f"[send-fail] {addr} {e}")

    def reply(addr: Tuple[str,int], t: bytes, rdict: dict):
        try:
            sock.sendto(krpc_encode({b"t": t, b"y": b"r", b"r": rdict}), addr)
        except Exception as e:
            print(f"[reply-fail] {addr} {e}")

    def reply_error(addr: Tuple[str,int], t: bytes, code=201, msg=b"Server Error"):
        try:
            sock.sendto(krpc_encode({b"t": t, b"y": b"e", b"e": [code, msg]}), addr)
        except Exception:
            pass

    def send_find_node(addr: Tuple[str,int], target=None, tid: Optional[bytes]=None):
        t = tid or os.urandom(TID_BYTES)
        if target is None: target = os.urandom(20)
        send({b"t": t, b"y": b"q", b"q": b"find_node", b"a": {b"id": my_id, b"target": target}}, addr)

    def send_get_peers(addr: Tuple[str,int], info_hash: bytes, tid: Optional[bytes]=None):
        t = tid or os.urandom(TID_BYTES)
        send({b"t": t, b"y": b"q", b"q": b"get_peers", b"a": {b"id": my_id, b"info_hash": info_hash}}, addr)
        return t

    def add_contact(ip, port, nid: Optional[bytes]):
        nodes.setdefault(ip, {"port": port, "id": nid or os.urandom(20), "last_seen": 0.0})
        nodes[ip]["port"] = port
        nodes[ip]["last_seen"] = time.time()

    def xor_distance(a: bytes, b: bytes) -> int:
        return int.from_bytes(bytes(x ^ y for x, y in zip(a, b)), "big")

    def closest_nodes(ih_hex: str, n: int) -> List[Tuple[str,int]]:
        ih = bytes.fromhex(ih_hex)
        scored = []
        for ip, meta in nodes.items():
            nid = meta.get("id")
            if isinstance(nid, (bytes, bytearray)) and len(nid) == 20:
                d = xor_distance(nid, ih)
                scored.append((d, ip, meta["port"]))
        scored.sort(key=lambda t: t[0])
        out = []
        for _, ip, port in scored[:max(1, n)]:
            out.append((ip, port))
        return out

    # Handlers
    gp_sample_ctr = 0

    def handle_query(msg: dict, addr: Tuple[str,int]):
        nonlocal gp_sample_ctr
        q = msg.get(b"q"); a = msg.get(b"a", {}); t = msg.get(b"t", b"aa")
        if q == b"ping":
            stats["ping"] += 1
            reply(addr, t, {b"id": my_id}); return

        if q == b"find_node":
            stats["find_node"] += 1
            cands = list(nodes.items())[:K]
            packed = b"".join(compact_node(ip, info["port"], info.get("id", os.urandom(20))) for ip, info in cands)
            reply(addr, t, {b"id": my_id, b"nodes": packed}); return

        if q == b"get_peers":
            stats["get_peers"] += 1
            ih = a.get(b"info_hash")
            tok = tokens.issue(addr[0])
            cands = list(nodes.items())[:K]
            packed = b"".join(compact_node(ip, info["port"], info.get("id", os.urandom(20))) for ip, info in cands)
            reply(addr, t, {b"id": my_id, b"token": tok, b"nodes": packed})
            if ih and isinstance(ih, (bytes, bytearray)) and len(ih) == 20:
                ih_hex = ih.hex()
                stats["unique_ih"].add(ih_hex)
                db_log_getpeers(args.db, ih_hex, addr[0], addr[1])
                # optional sampling of the *sender* (usually low yield)
                if RESOLVE_FROM_GETPEERS_EVERY > 0:
                    gp_sample_ctr = (gp_sample_ctr + 1) % RESOLVE_FROM_GETPEERS_EVERY
                    if gp_sample_ctr == 0:
                        pool.enqueue(ih_hex, addr[0], addr[1])
                # feed the explorer
                if ih_hex not in explore_seen:
                    explore_q.append(ih_hex)
            return

        if q == b"announce_peer":
            stats["announce_peer"] += 1
            ih = a.get(b"info_hash")
            tok = a.get(b"token", b"")
            port = int(a.get(b"port", 0)) if isinstance(a.get(b"port", 0), int) else 0
            implied = int(a.get(b"implied_port", 0)) if isinstance(a.get(b"implied_port", 0), int) else 0
            if not tokens.valid(addr[0], tok):
                reply_error(addr, t, 203, b"Bad token"); return
            use_port = addr[1] if implied == 1 or port <= 0 else port
            reply(addr, t, {b"id": my_id})
            if ih and isinstance(ih, (bytes, bytearray)) and len(ih) == 20:
                ih_hex = ih.hex()
                db_log_announce(args.db, ih_hex, addr[0], use_port, status="pending", source="announce")
                pool.enqueue(ih_hex, addr[0], use_port)
                if ih_hex not in explore_seen:
                    explore_q.append(ih_hex)
            add_contact(addr[0], use_port, nodes.get(addr[0], {}).get("id"))
            return

        reply_error(addr, t, 204, b"Method unknown")

    def handle_response(msg: dict, addr: Tuple[str,int]):
        nonlocal in_flight
        r = msg.get(b"r", {})
        tid = msg.get(b"t", b"")
        nid = r.get(b"id")
        if isinstance(nid, (bytes, bytearray)) and len(nid) == 20:
            add_contact(addr[0], addr[1], nid)
            good_nodes.add((addr[0], addr[1]))
        if b"nodes" in r:
            for ip, port, nid in parse_nodes(r[b"nodes"]):
                if (ip, port) not in visited:
                    visited.add((ip, port))
                    queue_nodes.append((ip, port))
                if ip not in nodes:
                    add_contact(ip, port, nid)
        # NEW: harvest values and map back to ih via transaction id
        if b"values" in r and tid in t_to_ih:
            ih_hex, _ = t_to_ih.get(tid, (None, 0.0))
            if ih_hex:
                peers = parse_values(r[b"values"])
                for ip, p in peers:
                    db_log_announce(args.db, ih_hex, ip, p, status="pending", source="values")
                    pool.enqueue(ih_hex, ip, p)
        # retire transaction id
        if tid in t_to_ih:
            t_to_ih.pop(tid, None)
        in_flight = max(0, in_flight - 1)

    # Explorer tick: send get_peers for queued IH to closest known nodes
    last_explore = 0.0
    def explorer_tick():
        nonlocal last_explore
        now = time.time()
        if now - last_explore < EXPLORE_TICK_S:
            return
        last_explore = now
        # throttle outstanding mapped tids
        if len(t_to_ih) >= MAX_OUTSTANDING_T:
            return
        # get next IH to explore
        while explore_q:
            ih_hex = explore_q.popleft()
            last = explore_seen.get(ih_hex, 0.0)
            if now - last >= EXPLORE_COOLDOWN_S:
                explore_seen[ih_hex] = now
                break
        else:
            return  # nothing to do
        # choose targets and send
        targets = closest_nodes(ih_hex, EXPLORE_FANOUT)
        if not targets:
            # fall back to a few good nodes
            targets = list(good_nodes)[:EXPLORE_FANOUT] or queue_nodes[:EXPLORE_FANOUT]
        ih = bytes.fromhex(ih_hex)
        for ip, port in targets:
            tid = os.urandom(TID_BYTES)
            t_to_ih[tid] = (ih_hex, now)
            send_get_peers((ip, port), ih, tid=tid)

    # Graceful exit
    def on_exit(*_):
        try:
            snap = {"uptime_s": int(time.time() - t0), "known_nodes": len(nodes)}
            open("bittorrent_network.json", "w").write(json.dumps(snap, indent=2))
        except Exception: pass
        os._exit(0)

    try:
        signal.signal(signal.SIGINT, on_exit)
        signal.signal(signal.SIGTERM, on_exit)
    except Exception:
        pass

    print("[*] DHT harvester + explorer starting…")
    print(f"    Node ID: {my_id.hex()}  UDP:{args.bind}:{args.port}  depth={args.depth}")

    last_seed = 0.0
    while True:
        # 0) expire stuck in-flight
        age_inflight()

        # 1) RECV
        events = sel.select(timeout=0.05)
        for key, _ in events:
            if key.fileobj is sock:
                for _ in range(RECV_BATCH):
                    try:
                        data, addr = sock.recvfrom(65536)
                    except BlockingIOError:
                        break
                    try:
                        msg = krpc_decode(data)
                    except Exception:
                        continue
                    y = msg.get(b"y", b"")
                    add_contact(addr[0], addr[1], nodes.get(addr[0], {}).get("id"))
                    if y == b"q": handle_query(msg, addr)
                    elif y == b"r": handle_response(msg, addr)

        # 2) Mode switch
        if not listen_mode and (found >= args.depth or len(nodes) >= args.depth or (time.time() - t0) > 18000):
            listen_mode = True
            print(f"[{int(time.time()-t0)}s] Switching to HARVEST (listen/serve+explore). "
                  f"found={found} known_nodes={len(nodes)} in_flight={in_flight}")

        # 3) Send logic
        if not listen_mode:
            budget = max(0, SEND_WINDOW - in_flight)
            while budget > 0 and queue_nodes:
                ip, port = queue_nodes.pop(0)
                send_find_node((ip, port))
                found += 1
                budget -= 1
            if not queue_nodes and found < args.depth and good_nodes:
                now = time.time()
                if now - last_seed > RESEED_EVERY:
                    last_seed = now
                    for ip, port in list(good_nodes)[:RESEED_FANOUT]:
                        send_find_node((ip, port))
                        found += 1
        else:
            now = time.time()
            if now - last_refresh > REFRESH_EVERY:
                last_refresh = now
                for ip, meta in list(nodes.items())[:4]:
                    send_find_node((ip, meta["port"]))
            # Explorer runs in harvest mode
            explorer_tick()

        # 4) Snapshot
        if time.time() - last_log > LOG_EVERY_SEC:
            last_log = time.time()
            try:
                snap = {"Summary": {
                    "uptime_s": int(time.time() - t0),
                    "known_nodes": len(nodes),
                    "visited": len(visited),
                    "in_flight": in_flight,
                    "listen_mode": listen_mode,
                    "explore_queue": len(explore_q),
                    "outstanding_t": len(t_to_ih)
                }}
                open("bittorrent_network.json", "w").write(json.dumps(snap, indent=2))
                print(f"[+] Wrote bittorrent_network.json nodes={len(nodes)} explore_q={len(explore_q)}")
            except Exception as e:
                print(f"[log-error] {e}")

        # 5) Per-minute rates
        if time.time() - last_rate > RATE_WINDOW_SEC:
            print(f"[rate {RATE_WINDOW_SEC}s] ping={stats['ping']}  find_node={stats['find_node']}  "
                  f"get_peers={stats['get_peers']}  announce_peer={stats['announce_peer']}  "
                  f"unique_infohashes={len(stats['unique_ih'])}  in_flight={in_flight}")
            stats = {"ping":0, "find_node":0, "get_peers":0, "announce_peer":0, "unique_ih": set()}
            last_rate = time.time()


if __name__ == "__main__":
    main()
