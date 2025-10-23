#!/usr/bin/env python3
# resolver.py — inspect & resolve metadata from dht_observatory.db
# Safe-by-design: only BEP-9 metadata exchange (no payload), with optional write-cage.

import argparse, json, time, sqlite3, hashlib
from pathlib import Path
from typing import Dict, Any, Tuple, Optional, List
import os, sys, socket, struct, asyncio, random, math, builtins

# ---- try to import your snippet-based lookup ----
_GETMETA = None
# try:
#     # your helper that worked in the REPL screenshot
#     from dht_crawler_v4 import get_metadata_from_infohash as _GETMETA
# except Exception:
#     _GETMETA = None

from dht_crawler_v4 import get_metadata_from_infohash

# -------------------- CONFIG --------------------
DEFAULT_DB = "dht_observatory.db"
METADATA_PIECE_LEN = 16384
BT_PSTR = b"BitTorrent protocol"
RESERVED = bytearray(b"\x00"*8); RESERVED[5] |= 0x10; RESERVED = bytes(RESERVED)  # extension bit
MSG_EXTENDED = 20
EXT_HANDSHAKE_ID = 0

# DHT routers (UDP)
ROUTERS = [
    ("router.bittorrent.com", 6881),
    ("router.utorrent.com", 6881),
    ("dht.transmissionbt.com", 6881),
    ("dht.aelitis.com", 6881),
]

# Optional write cage (prevent accidental file downloads / writes)
WRITE_GUARD_ENABLED = os.environ.get("RESOLVER_WRITE_GUARD", "1") not in ("0", "false", "False", "")
_WRITE_WHITELIST: set = set()
_REAL_OPEN = builtins.open

def allow_write(p: Path):
    try:
        _WRITE_WHITELIST.add(p.resolve())
    except Exception:
        pass

def guarded_open(file, mode='r', *a, **kw):
    if WRITE_GUARD_ENABLED and any(x in mode for x in ('w','a','x','+')):
        p = Path(file).resolve()
        if p not in _WRITE_WHITELIST:
            raise RuntimeError(f"[write-cage] refusing write to {p}")
    return _REAL_OPEN(file, mode, *a, **kw)

if WRITE_GUARD_ENABLED:
    builtins.open = guarded_open

# -------------------- UTIL --------------------
def human_bytes(n: Optional[int]) -> str:
    if n is None: return "—"
    units = ["B","KB","MB","GB","TB","PB","EB"]
    f = float(n)
    for u in units:
        if f < 1024.0: return f"{f:.1f}{u}"
        f /= 1024.0
    return f"{f:.1f}ZB"

def ago(ts: Optional[int]) -> str:
    if not ts: return "—"
    d = int(time.time()) - int(ts)
    if d < 60: return f"{d}s ago"
    if d < 3600: return f"{d//60}m ago"
    if d < 86400: return f"{d//3600}h ago"
    return f"{d//86400}d ago"

def print_table(rows: List[dict], cols: List[Tuple[str,str]], limit: int):
    if not rows:
        print("(no rows)"); return
    rows = rows[:limit]
    widths = {h: len(h) for _, h in cols}
    for r in rows:
        for key, hdr in cols:
            s = str(r.get(key,""))
            widths[hdr] = max(widths[hdr], len(s))
    line = "  ".join(hdr.ljust(widths[hdr]) for _, hdr in cols)
    print(line); print("-" * len(line))
    for r in rows:
        print("  ".join(str(r.get(key,"")).ljust(widths[hdr]) for key, hdr in cols))

def sample_filenames(files_json: Optional[str], maxn: int = 5) -> Tuple[int, str]:
    try:
        files = json.loads(files_json or "[]")
    except Exception:
        files = []
    if not files:
        return (0, "")
    names = [f.get("path","") for f in files if isinstance(f, dict)]
    return (len(names), ", ".join(names[:maxn]))

def open_db(path: str) -> sqlite3.Connection:
    if not os.path.exists(path):
        print(f"[!] DB not found: {path}", file=sys.stderr); sys.exit(2)
    con = sqlite3.connect(path, timeout=30, isolation_level=None)
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA journal_mode=WAL;")
    con.execute("PRAGMA synchronous=NORMAL;")
    return con

# -------------------- BENCODE --------------------
def benc_enc(x) -> bytes:
    if isinstance(x, int): return b"i%de" % x
    if isinstance(x, (bytes, bytearray)):
        b = bytes(x); return str(len(b)).encode()+b":"+b
    if isinstance(x, str):
        b = x.encode(); return str(len(b)).encode()+b":"+b
    if isinstance(x, list): return b"l"+b"".join(benc_enc(i) for i in x)+b"e"
    if isinstance(x, dict):
        items=[]
        for k in sorted(x.keys(), key=lambda k: k if isinstance(k,(bytes,bytearray)) else str(k).encode()):
            kb = k if isinstance(k,(bytes,bytearray)) else str(k).encode()
            items.append(benc_enc(kb)+benc_enc(x[k]))
        return b"d"+b"".join(items)+b"e"
    raise TypeError(f"unsupported type: {type(x)}")

def _bdec_any(b: bytes, i: int):
    if i >= len(b): raise ValueError("unexpected end")
    c = b[i:i+1]
    if c == b"i":
        j = b.index(b"e", i+1); return int(b[i+1:j]), j+1
    if c == b"l":
        i += 1; out=[]
        while b[i:i+1] != b"e":
            v,i=_bdec_any(b,i); out.append(v)
        return out, i+1
    if c == b"d":
        i += 1; out={}
        while b[i:i+1] != b"e":
            k,i=_bdec_any(b,i)
            if not isinstance(k,(bytes,bytearray)): raise ValueError("dict key must be bytes")
            v,i=_bdec_any(b,i); out[k]=v
        return out, i+1
    if 48 <= b[i] <= 57:
        j = b.index(b":", i); ln=int(b[i:j]); s=j+1; e=s+ln
        if e > len(b): raise ValueError("short string")
        return b[s:e], e
    raise ValueError(f"invalid bencode at {i}")

def benc_dec(buf: bytes):
    obj, idx = _bdec_any(buf, 0)
    if idx != len(buf): raise ValueError("trailing")
    return obj

# -------------------- DB VIEWS --------------------
def cmd_summary(con: sqlite3.Connection, limit: int, do_lookup: bool, name_timeout: float, cache_names: bool):
    print("# Summary")
    a_total = con.execute("SELECT COUNT(*) AS c FROM announces").fetchone()["c"]
    by_status = con.execute("SELECT status, COUNT(*) c FROM announces GROUP BY status").fetchall()
    gp_total = con.execute("SELECT COUNT(*) AS c FROM getpeers").fetchone()["c"]
    md_total = con.execute("SELECT COUNT(*) AS c FROM metadata").fetchone()["c"]
    parts = [f"announces: {a_total}"]
    if by_status:
        parts.append(" | " + "  ".join(f"{r['status']}={r['c']}" for r in by_status))
    print("".join(parts))
    print(f"get_peers: {gp_total}")
    print(f"metadata:  {md_total}\n")

    print("## Recent announces")
    # Pull recent announces and any existing metadata name in one go
    rows = con.execute("""
        SELECT a.ts, a.ip, a.port, a.info_hash, a.status, a.attempts,
               m.name AS mname, m.total_length AS msize
        FROM announces a
        LEFT JOIN metadata m ON m.info_hash = a.info_hash
        ORDER BY a.ts DESC LIMIT ?;
    """,(limit,)).fetchall()

    rows_fmt = []
    for r in rows:
        name = r["mname"] or ""
        # Best-effort lookup only when missing, and only if helper is available
        if do_lookup and not name and _GETMETA:
            try:
                info = _GETMETA(r["info_hash"], int(max(1, name_timeout)))
                if isinstance(info, dict):
                    name = (info.get('name') or "") if 'name' in info else (info.get('info',{}).get('name') or "")
                    # optionally cache into metadata
                    if cache_names and name:
                        try:
                            upsert_metadata(con, r["info_hash"], _coerce_info_to_bdict(info))
                        except Exception:
                            pass
            except Exception:
                pass  # timeout / no metadata

        rows_fmt.append({
            "when": ago(r["ts"]),
            "peer": f"{r['ip']}:{r['port']}",
            "info_hash": r["info_hash"],
            "name": name or "",
            "status": r["status"],
            "tries": r["attempts"],
        })

    print_table(
        rows_fmt,
        [("when","when"),("peer","peer"),("info_hash","info_hash"),
         ("name","name"),("status","status"),("tries","tries")],
        33
    )
    print()

    # Show most recently resolved metadata with filenames
    print("## Recent resolved (with filenames)")
    mrows = con.execute("""
        SELECT info_hash, name, total_length, files_json, last_resolved_ts
        FROM metadata
        ORDER BY last_resolved_ts DESC
        LIMIT ?;
    """,(min(10, limit),)).fetchall()
    mfmt = []
    for r in mrows:
        count, sample = sample_filenames(r["files_json"], maxn=5)
        mfmt.append({
            "when": ago(r["last_resolved_ts"]),
            "info_hash": r["info_hash"],
            "name": r["name"] or "(unnamed)",
            "size": human_bytes(r["total_length"]),
            "files": f"{count}" if count else "1",
            "sample": sample if sample else "(single-file torrent)"
        })
    print_table(
        mfmt,
        [("when","when"),("info_hash","info_hash"),("name","name"),("files","#files")],
        limit=min(10, limit)
    )
    print()

    # print("## Recent get_peers")
    # rows = con.execute("""
    #     SELECT ts, ip, port, info_hash
    #     FROM getpeers
    #     ORDER BY ts DESC LIMIT ?;
    # """,(limit,)).fetchall()
    # rows_fmt = [{"when":ago(r["ts"]),"peer":f"{r['ip']}:{r['port']}",
    #              "info_hash":r["info_hash"]} for r in rows]
    # print_table(rows_fmt,[("when","when"),("peer","peer"),("info_hash","info_hash")],limit)
    # print()

# Helper to coerce your snippet-return dict into the bdict shape expected by upsert_metadata
def _coerce_info_to_bdict(info: Dict[str, Any]) -> Dict[bytes, Any]:
    # Accepts dicts like {'name': 'X', 'size': N, 'path': '...', 'total': N} etc.
    # Produces a minimal BEP-3 info bdict-ish structure used by upsert_metadata.
    out: Dict[bytes, Any] = {}
    name = info.get("name")
    if name: out[b"name"] = name.encode(errors="replace")
    total = None
    if "total" in info and isinstance(info["total"], int):
        total = info["total"]
    elif "size" in info and isinstance(info["size"], int):
        total = info["size"]
    if total is not None:
        out[b"length"] = int(total)
    # files list if present (optional)
    files = info.get("files") or []
    if isinstance(files, list) and files:
        fl = []
        for f in files:
            try:
                ln = int(f.get("size") or f.get("length") or 0)
                p = f.get("path") or ""
                parts = [p.encode(errors="replace")] if isinstance(p, str) else []
                fl.append({b"length": ln, b"path": parts})
            except Exception:
                continue
        if fl:
            out[b"files"] = fl
            out.pop(b"length", None)
    # defaults so upsert_metadata doesn’t crash
    out.setdefault(b"piece length", 0)
    out.setdefault(b"private", 0)
    return out

def cmd_unresolved(con: sqlite3.Connection, limit: int):
    print("# Unresolved announces (pending/error/working; no metadata yet)")
    rows = con.execute("""
        SELECT a.id, a.ts, a.ip, a.port, a.info_hash, a.status, a.attempts
        FROM announces a
        LEFT JOIN metadata m ON m.info_hash=a.info_hash
        WHERE m.info_hash IS NULL
          AND (a.status='pending' OR a.status='error' OR a.status='working')
        ORDER BY a.ts DESC
        LIMIT ?;
    """,(limit,)).fetchall()
    rows_fmt=[{"id":r["id"],"when":ago(r["ts"]), "peer":f"{r['ip']}:{r['port']}",
               "info_hash":r["info_hash"], "status":r["status"], "tries":r["attempts"]} for r in rows]
    print_table(rows_fmt,[("id","id"),("when","when"),("peer","peer"),
                          ("info_hash","info_hash"),("status","status"),("tries","tries")],limit)
    print()

def cmd_metadata(con: sqlite3.Connection, limit: int, query: Optional[str]):
    print("# Resolved metadata")
    sql = "SELECT info_hash, name, total_length, piece_length, private, last_resolved_ts FROM metadata"
    params: Tuple[Any,...] = ()
    if query:
        sql += " WHERE info_hash LIKE ? OR name LIKE ?"
        q = f"%{query}%"; params=(q,q)
    sql += " ORDER BY last_resolved_ts DESC LIMIT ?"
    params = params + (limit,)
    rows = con.execute(sql, params).fetchall()
    rows_fmt=[{"when":ago(r["last_resolved_ts"]), "info_hash":r["info_hash"],
               "name":r["name"] or "(unnamed)","size":human_bytes(r["total_length"]),
               "priv":r["private"]} for r in rows]
    print_table(rows_fmt,[("when","when"),("info_hash","info_hash"),
                          ("name","name"),("size","size"),("priv","priv")],limit)
    print()

def cmd_top(con: sqlite3.Connection, limit: int):
    print("# Top infohashes by get_peers count")
    rows = con.execute("""
        SELECT info_hash, COUNT(*) AS hits, MIN(ts) first_ts, MAX(ts) last_ts
        FROM getpeers GROUP BY info_hash
        ORDER BY hits DESC, last_ts DESC LIMIT ?;
    """,(limit,)).fetchall()
    rows_fmt=[{"hits":r["hits"],"info_hash":r["info_hash"],
               "first":ago(r["first_ts"]), "last":ago(r["last_ts"])} for r in rows]
    print_table(rows_fmt,[("hits","hits"),("info_hash","info_hash"),
                          ("first","first_seen"),("last","last_seen")],limit)
    print()
    hashes = [row['info_hash'] for row in rows]
    for h in hashes:
        try:
            info = get_metadata_from_infohash(h,15)
            print(f'{h} -> {info["name"]}')
            upsert_metadata(con, h, info['name'], info)
        except TimeoutError:
            print(f'Cannot resolve {h}')
        
    
def cmd_files(con: sqlite3.Connection, ih: str):
    print(f"# Files in torrent {ih}")
    r = con.execute("SELECT name, files_json, total_length, piece_length, private FROM metadata WHERE info_hash=?",(ih,)).fetchone()
    if not r:
        r = get_metadata_from_infohash(ih, 30)
        
    print(f"name: {r['name'] or '(unnamed)'}")
    # print(f"size: {human_bytes(r['total_length'])}  piece_length: {human_bytes(r['piece_length'])}  private: {r['private']}")
    files=[]
    try:
        files=json.loads(r["files_json"] or "[]")
    except Exception:
        pass
    if not files:
        print("(no file list)");
        return
    w_size=max(len("size"), max((len(human_bytes(f.get("length",0))) for f in files), default=4))
    w_path=max(len("path"), max((len(f.get("path","")) for f in files), default=4))
    print("size".ljust(w_size),"  ","path")
    print("-"*w_size,"  ","-"*w_path)
    for f in files[:500]:
        print(human_bytes(f.get("length",0)).ljust(w_size),"  ",f.get("path",""))

def cmd_export(con: sqlite3.Connection, what: str, out: str, limit: int):
    outp = Path(out)
    allow_write(outp)  # whitelist this explicit export file
    if what=="metadata":
        rows = con.execute("""
            SELECT info_hash, name, total_length, piece_length, private, files_json, first_seen_ts, last_resolved_ts
            FROM metadata ORDER BY last_resolved_ts DESC LIMIT ?;
        """,(limit,)).fetchall()
    elif what=="announces":
        rows = con.execute("""
            SELECT id, ts, ip, port, info_hash, status, attempts, last_attempt_ts
            FROM announces ORDER BY ts DESC LIMIT ?;
        """,(limit,)).fetchall()
    elif what=="getpeers":
        rows = con.execute("""
            SELECT id, ts, ip, port, info_hash FROM getpeers
            ORDER BY ts DESC LIMIT ?;
        """,(limit,)).fetchall()
    else:
        print(f"[!] unknown export: {what}"); return
    with open(outp,"w",encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(dict(r), ensure_ascii=False)+"\n")
    print(f"[+] wrote {len(rows)} rows to {outp}")

# -------------------- METADATA-ONLY RESOLVER --------------------
def compact_nodes_to_list(compact: bytes):
    out=[]
    for i in range(0,len(compact),26):
        c=compact[i:i+26]
        if len(c)!=26: continue
        nid=c[:20]
        ip=".".join(str(x) for x in c[20:24])
        port=int.from_bytes(c[24:], "big")
        out.append((ip,port,nid))
    return out

async def dht_get_peers(info_hash: bytes, want=30, timeout=2.0) -> List[Tuple[str,int]]:
    loop = asyncio.get_running_loop()
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.setblocking(False); sock.settimeout(timeout)
    try: sock.bind(("0.0.0.0", 0))
    except Exception: pass

    def send(addr, msg):
        try: sock.sendto(benc_enc(msg), addr)
        except Exception: pass

    tid = os.urandom(2); my_id = os.urandom(20)
    q = {b"t":tid, b"y":b"q", b"q":b"get_peers", b"a":{b"id":my_id, b"info_hash":info_hash}}
    for r in ROUTERS: send(r, q)
    nodes=set(); peers=set()
    t_end=time.time()+5
    while time.time()<t_end and len(peers)<want:
        try:
            data, addr = await loop.run_in_executor(None, sock.recvfrom, 65536)
        except Exception:
            await asyncio.sleep(0.01); continue
        try:
            dec = benc_dec(data)
        except Exception:
            continue
        if dec.get(b"y")==b"r":
            r=dec.get(b"r",{})
            if b"values" in r:
                for v in r[b"values"]:
                    if len(v)!=6: continue
                    ip=".".join(str(x) for x in v[0:4])
                    port=int.from_bytes(v[4:6],"big")
                    peers.add((ip,port))
            if b"nodes" in r:
                for ip,port,_ in compact_nodes_to_list(r[b"nodes"]):
                    if (ip,port) not in nodes:
                        nodes.add((ip,port))
                        send((ip,port), {b"t":os.urandom(2), b"y":b"q", b"q":b"get_peers",
                                         b"a":{b"id":my_id, b"info_hash":info_hash}})
    try: sock.close()
    except Exception: pass
    return list(peers)[:want]

def bt_handshake_and_ut_metadata(ip: str, port: int, ih_hex: str, timeout: float) -> Optional[Dict[bytes,Any]]:
    ih = bytes.fromhex(ih_hex)
    peer_id = b"-DBRV01-"+os.urandom(12)

    def send(sock: socket.socket, buf: bytes): sock.sendall(buf)
    def recv_all(sock: socket.socket, ln: int) -> bytes:
        buf=b""
        while len(buf)<ln:
            chunk=sock.recv(ln-len(buf))
            if not chunk: raise ConnectionError("socket closed")
            buf+=chunk
        return buf

    with socket.create_connection((ip,int(port)), timeout=timeout) as s:
        s.settimeout(timeout)
        hs = bytes([len(BT_PSTR)])+BT_PSTR+RESERVED+ih+peer_id
        send(s, hs)
        resp = recv_all(s, 68)
        if len(resp)!=68 or resp[0]!=len(BT_PSTR) or resp[1:20]!=BT_PSTR:
            return None
        if not (resp[20+5] & 0x10):
            return None

        ext = benc_enc({b"m": {b"ut_metadata": 1}})
        send(s, struct.pack("!I", 2+len(ext)) + bytes([MSG_EXTENDED, 0]) + ext)

        def read_msg():
            hdr = recv_all(s, 4); (ln,) = struct.unpack("!I", hdr)
            if ln==0: return None, b""
            body = recv_all(s, ln)
            return body[0], body[1:]

        ut_id=None; meta_sz=None
        t_dead = time.time()+timeout
        while time.time()<t_dead and (ut_id is None):
            mid,payload = read_msg()
            if mid != MSG_EXTENDED: continue
            ext_id, rest = payload[0], payload[1:]
            if ext_id == 0:
                d = benc_dec(rest)
                m = d.get(b"m",{}) or {}
                if b"ut_metadata" in m and isinstance(m[b"ut_metadata"], int):
                    ut_id = int(m[b"ut_metadata"])
                if b"metadata_size" in d and isinstance(d[b"metadata_size"], int):
                    meta_sz = int(d[b"metadata_size"])
        if ut_id is None:
            return None

        def req_piece(i: int):
            req = benc_enc({b"msg_type":0, b"piece":i})
            body = bytes([MSG_EXTENDED, ut_id]) + req
            send(s, struct.pack("!I", len(body)) + body)
            t_end = time.time()+timeout
            while time.time()<t_end:
                mid,payload = read_msg()
                if mid!=MSG_EXTENDED or payload[0]!=ut_id: continue
                header, idx = _bdec_any(payload[1:], 0)
                if not isinstance(header, dict): continue
                msg_type = int(header.get(b"msg_type",-1))
                piece_idx = int(header.get(b"piece",-1))
                if msg_type==1 and piece_idx==i:
                    enc = benc_enc(header); off = 1 + len(enc)
                    return header.get(b"total_size"), payload[off:]
                if msg_type==2:
                    return None, None
            return None, None

        pieces: Dict[int, bytes] = {}
        if not meta_sz or meta_sz <= 0:
            tot,p0 = req_piece(0)
            if tot is None or p0 is None: return None
            meta_sz = int(tot); pieces[0]=p0
        if meta_sz > 4*1024*1024: return None
        n = (meta_sz + METADATA_PIECE_LEN - 1) // METADATA_PIECE_LEN
        for i in range(n):
            if i in pieces: continue
            tot,pb = req_piece(i)
            if pb is None: return None
            pieces[i]=pb

        buf = bytearray(meta_sz)
        for i,ch in pieces.items():
            start=i*METADATA_PIECE_LEN
            buf[start:start+len(ch)] = ch[:max(0, min(len(ch), meta_sz-start))]

        info = benc_dec(bytes(buf))
        raw = benc_enc(info)
        if hashlib.sha1(raw).digest()!=ih and hashlib.sha1(bytes(buf)).digest()!=ih:
            return None
        return info

def upsert_metadata(con: sqlite3.Connection, ih_hex: str, name,info: Dict[bytes,Any]):
    piece_len = int(info.get(b"piece length") or 0)
    private = int(info.get(b"private") or 0)
    total_len = 0
    files_json = "[]"
    if b"files" in info:
        files=[]
        for f in info[b"files"]:
            ln = int(f.get(b"length") or 0); total_len += ln
            parts = f.get(b"path", [])
            if isinstance(parts, list):
                path="/".join(p.decode(errors="replace") if isinstance(p,(bytes,bytearray)) else str(p) for p in parts)
            else:
                path=""
            files.append({"length": ln, "path": path})
        files_json = json.dumps(files, ensure_ascii=False)
    else:
        total_len = int(info.get(b"length") or 0)
    now = int(time.time())
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
    """,(ih_hex, name, total_len, piece_len, private, files_json, now, now))

def mark_announce(con: sqlite3.Connection, id_: int, status: str):
    con.execute("UPDATE announces SET status=?, attempts=attempts+1, last_attempt_ts=? WHERE id=?",
                (status, int(time.time()), id_))

def get_pending_announces(con: sqlite3.Connection, limit: int, since_minutes: Optional[int]) -> List[sqlite3.Row]:
    if since_minutes and since_minutes > 0:
        after = (int(time.time()) - since_minutes)*60
        rows = con.execute("""
            SELECT a.id, a.ts, a.ip, a.port, a.info_hash
            FROM announces a
            LEFT JOIN metadata m ON m.info_hash=a.info_hash
            WHERE m.info_hash IS NULL AND (a.status='pending' OR a.status='error')
              AND a.ts >= ?
            ORDER BY a.ts DESC LIMIT ?;
        """,(after, limit)).fetchall()
    else:
        rows = con.execute("""
            SELECT a.id, a.ts, a.ip, a.port, a.info_hash
            FROM announces a
            LEFT JOIN metadata m ON m.info_hash=a.info_hash
            WHERE m.info_hash IS NULL AND (a.status='pending' OR a.status='error')
            ORDER BY a.ts DESC LIMIT ?;
        """,(limit,)).fetchall()
    return rows

async def resolve_one(con: sqlite3.Connection, row: sqlite3.Row, timeout: float, per_ih: int, dht_peers: int) -> bool:
    id_, ip, port, ih = row["id"], row["ip"], int(row["port"]), row["info_hash"]
    mark_announce(con, id_, "working")
    # info = bt_handshake_and_ut_metadata(ip, port, ih, timeout)
    info = get_metadata_from_infohash(ih, timeout)
    if info:
        upsert_metadata(con, ih, info['name'],info)
        mark_announce(con, id_, "ok")
        print(f"[OK] {ih[:16]}..  via {ip}:{port}->{info['name']}")
        return True

    # try:
    #     peers = await dht_get_peers(bytes.fromhex(ih), want=dht_peers, timeout=max(2.0, timeout/2))
    # except Exception:
    #     peers = []
    # tried = 0
    # for pip, pport in peers:
    #     if tried >= per_ih: break
    #     tried += 1
    #     try:
    #         info = bt_handshake_and_ut_metadata(pip, pport, ih, timeout)
    #         if info:
    #             upsert_metadata(con, ih, info)
    #             mark_announce(con, id_, "ok")
    #             print(f"[OK] {ih[:16]}..  via {pip}:{pport} (DHT)")
    #             return True
    #     except Exception:
    #         continue
    mark_announce(con, id_, "error")
    print(f"[FAIL] {ih[:16]}..  announce {ip}:{port} ")
    return False

async def resolver_loop(db_path: str, batch: int, concurrency: int, timeout: float,
                        per_ih: int, dht_peers: int, since_minutes: Optional[int], loop_forever: bool):
    if since_minutes is None:
        since_minutes = 60
    sem = asyncio.Semaphore(max(1, concurrency))
    while True:
        con = open_db(db_path)
        rows = get_pending_announces(con, batch, since_minutes)
        if not rows:
            con.close()
            if not loop_forever:
                print("[i] nothing to resolve."); return
            await asyncio.sleep(10); continue

        async def run_one(r: sqlite3.Row):
            async with sem:
                con2 = open_db(db_path)
                try:
                    # await resolve_one(con2, r, timeout, per_ih, dht_peers)
                    id_, ip, port, ih = r["id"], r["ip"], int(r["port"]), r["info_hash"]
                    print(f'Trying to resolve {ih[:16]}')
                    mark_announce(con, id_, "working")
                    try:
                        info = bt_handshake_and_ut_metadata(ip, port, ih, timeout)
                        print(f"[OK] {ih[:16]}..  via {ip}:{port}->{info['name']}")
                    except:
                        info = get_metadata_from_infohash(ih, timeout)
                        print(f"[OK] {ih[:16]}..  via {ip}:{port}->{info['name']}")
                        pass
                    
                    if info:
                        upsert_metadata(con, ih, info['name'])
                        mark_announce(con, id_, "ok")
                        
                    else:
                        print(f'[FAIL] Unable to resolve {ih[:16]}')
                finally:
                    con2.close()

        tasks = [asyncio.create_task(run_one(r)) for r in rows]
        await asyncio.gather(*tasks, return_exceptions=True)
        con.close()
        if not loop_forever: return
        await asyncio.sleep(2)

# -------------------- PLOTTING / GEO (optional) --------------------
def _collect_ips_from_db(db_path: str, since_hours: int = 72) -> set:
    con = sqlite3.connect(db_path); con.row_factory = sqlite3.Row
    now = int(time.time()); since = now - since_hours*3600 if since_hours else None
    ips = set()
    try:
        q = "SELECT DISTINCT ip FROM getpeers" + (" WHERE ts >= ?" if since else "")
        for r in con.execute(q, (since,) if since else ()):
            if r["ip"]: ips.add(r["ip"])
    except Exception: pass
    try:
        q = "SELECT DISTINCT ip FROM announces" + (" WHERE ts >= ?" if since else "")
        for r in con.execute(q, (since,) if since else ()):
            if r["ip"]: ips.add(r["ip"])
    except Exception: pass
    con.close()
    return ips

def build_geo_cache_if_needed(db_path, cache_path="geo_cache.json", city_mmdb=None, asn_mmdb=None, since_hours=2):
    p = Path(cache_path)
    allow_write(p)
    if p.exists():
        try:
            json.loads(p.read_text(encoding="utf-8"))
            return
        except Exception:
            pass

    try:
        import geoip2.database  # optional
    except Exception:
        print("[plot] geoip2 not installed; pass an existing geo_cache.json or install geoip2.")
        with open(p, "w", encoding="utf-8") as f: f.write("{}")
        return

    city_mmdb = city_mmdb or os.environ.get("GEOIP2_CITY_DB")
    asn_mmdb  = asn_mmdb  or os.environ.get("GEOIP2_ASN_DB")
    city_r = asn_r = None
    if city_mmdb and Path(city_mmdb).exists():
        city_r = geoip2.database.Reader(city_mmdb)  # type: ignore[name-defined]
    if asn_mmdb and Path(asn_mmdb).exists():
        asn_r = geoip2.database.Reader(asn_mmdb)    # type: ignore[name-defined]
    if not (city_r or asn_r):
        with open(p,"w",encoding="utf-8") as f: f.write("{}")
        print("[plot] cannot build geo_cache.json (missing MMDBs)."); return

    ips = _collect_ips_from_db(db_path, since_hours=since_hours)
    if not ips:
        with open(p,"w",encoding="utf-8") as f: f.write("{}")
        print("[plot] No IPs to geolocate from DB."); return

    cache = {}
    filled = 0
    for ip in ips:
        city = {}; asn = {}
        if city_r:
            try:
                resp = city_r.city(ip)
                if resp and resp.location and resp.location.latitude is not None and resp.location.longitude is not None:
                    city = {"lat": float(resp.location.latitude), "lon": float(resp.location.longitude),
                            "country": (resp.country and resp.country.iso_code) or None}
            except Exception: pass
        if asn_r:
            try:
                resp = asn_r.asn(ip)
                if resp:
                    asn = {"asn": int(resp.autonomous_system_number) if resp.autonomous_system_number else None,
                           "org": resp.autonomous_system_organization or None}
            except Exception: pass
        if city or asn:
            cache[ip] = {"city": city or None, "asn": asn or None}
            filled += 1

    tmp = p.with_suffix(".tmp.json")
    with open(tmp, "w", encoding="utf-8") as f:
        f.write(json.dumps(cache))
    tmp.replace(p)
    try:
        if city_r: city_r.close()
        if asn_r: asn_r.close()
    except Exception: pass
    print(f"[plot] geo_cache.json built with {filled:,} entries")

def _load_activity(db_path: str):
    out = {}
    con = sqlite3.connect(db_path); con.row_factory = sqlite3.Row
    try:
        for r in con.execute("SELECT ip, MAX(ts) AS last_ts FROM getpeers GROUP BY ip"):
            ip = r["ip"];  ts = r["last_ts"]
            if not ip: continue
            out.setdefault(ip, {"last_ts": 0, "last_from": None})
            if ts and int(ts) > out[ip]["last_ts"]:
                out[ip]["last_ts"], out[ip]["last_from"] = int(ts), "getpeers"
    except Exception: pass
    try:
        for r in con.execute("SELECT ip, MAX(ts) AS last_ts FROM announces GROUP BY ip"):
            ip = r["ip"];  ts = r["last_ts"]
            if not ip: continue
            out.setdefault(ip, {"last_ts": 0, "last_from": None})
            if ts and int(ts) > out[ip]["last_ts"]:
                out[ip]["last_ts"], out[ip]["last_from"] = int(ts), "announce"
    except Exception: pass
    con.close()
    return out

def _hash_color(token: str) -> str:
    return "#" + hashlib.md5((token or "x").encode()).hexdigest()[:6]

def _nice_age(sec):
    if sec is None: return "never"
    if sec < 60: return f"{int(sec)}s"
    if sec < 3600: return f"{int(sec//60)}m"
    if sec < 86400: return f"{int(sec//3600)}h"
    return f"{int(sec//86400)}d"

def _ensure_folium():
    try:
        import folium
        from folium import plugins  # noqa: F401
        return folium
    except Exception as e:
        raise RuntimeError("folium not installed (pip install folium)") from e

def _write_map_refresh(html_path: Path, refresh_sec: int):
    allow_write(html_path)
    html = html_path.read_text(encoding="utf-8")
    html = html.replace("<head>", f"<head>\n<meta http-equiv=\"refresh\" content=\"{int(refresh_sec)}\">")
    html_path.write_text(html, encoding="utf-8")

def plot_live_opacity(db_path, cache_path, output,
                      half_life_min=15, cutoff_hours=24,
                      min_opacity=0.06, max_opacity=0.98,
                      min_radius=1.3, max_radius=5.2,
                      highlight_min=3.0, refresh_sec=20):
    folium = _ensure_folium()
    from folium import plugins
    cache = json.loads(Path(cache_path).read_text(encoding="utf-8"))
    activity = _load_activity(db_path)
    now = time.time(); cutoff = cutoff_hours*3600.0; hl = max(30.0, half_life_min*60.0)

    points = []
    for ip, entry in cache.items():
        c = (entry.get("city") or {})
        lat, lon = c.get("lat"), c.get("lon")
        if lat is None or lon is None: continue
        org = ((entry.get("asn") or {}).get("org")) or "Unknown org"
        meta = activity.get(ip, {})
        last_ts = meta.get("last_ts"); last_from = meta.get("last_from")
        age = (now - last_ts) if last_ts else None
        if age is None or age > cutoff: continue
        score = 0.5 ** (age / hl)
        s = max(0.0, min(1.0, score)) ** 0.9
        opacity = min_opacity + (max_opacity - min_opacity) * s
        radius  = min_radius + (max_radius - min_radius) * s
        fresh   = bool(age is not None and age <= (highlight_min*60))
        points.append({"ip":ip,"lat":lat,"lon":lon,"org":org,"age":age,
                       "last_from":last_from,"opacity":opacity,"radius":radius,"fresh":fresh})

    if not points:
        raise RuntimeError("No points within cutoff; build/refresh geo cache or widen cutoff.")

    m = folium.Map(location=[17, 23], zoom_start=2.75, tiles="CartoDB dark_matter")
    plugins.Fullscreen(position="topright").add_to(m)
    plugins.MiniMap(toggle_display=True).add_to(m)
    plugins.MousePosition(position="bottomleft", prefix="lat/lon", separator=" , ").add_to(m)
    plugins.MeasureControl(position="topright", primary_length_unit="kilometers").add_to(m)

    layer = folium.FeatureGroup(name="ASN color • exponential recency", show=True)
    for p in points:
        color = _hash_color(p["org"])
        folium.CircleMarker(
            location=[p["lat"], p["lon"]],
            radius=p["radius"], color=color, fill=True, fill_color=color,
            fill_opacity=p["opacity"], opacity=p["opacity"], weight=0.6,
            tooltip=f"{p['ip']} • {p['org']} • last {p['last_from']} {_nice_age(p['age'])} ago"
        ).add_to(layer)
        if p["fresh"]:
            folium.CircleMarker(
                location=[p["lat"], p["lon"]],
                radius=p["radius"]+2.0, color="#ffffff", fill=False,
                opacity=min(1.0, p["opacity"]+0.25), weight=1.4
            ).add_to(layer)
    layer.add_to(m)

    con = sqlite3.connect(db_path)
    gp5 = con.execute("SELECT COUNT(*) FROM getpeers WHERE ts >= strftime('%s','now')-300").fetchone()[0]
    an5 = con.execute("SELECT COUNT(*) FROM announces WHERE ts >= strftime('%s','now')-300").fetchone()[0]
    con.close()
    footer = (
        '<div style="position: fixed; bottom: 12px; right: 14px; z-index: 9999;'
        'background: rgba(20,20,20,0.78); color: #ddd; padding: 8px 10px;'
        'border-radius: 8px; font-size: 12px;">'
        f'<div><b>Last updated:</b> {time.strftime("%Y-%m-%d %H:%M:%S UTC", time.gmtime(now))}</div>'
        f'<div><b>Events (last 5m):</b> get_peers {gp5:,} • announce {an5:,}</div>'
        '<div style="opacity:.75">Color = ASN org • Opacity & size = recency</div>'
        '</div>'
    )
    m.get_root().html.add_child(folium.Element(footer))
    folium.LayerControl(collapsed=False).add_to(m)

    out = Path(output); allow_write(out)
    m.save(str(out))
    _write_map_refresh(out, refresh_sec)
    print(f"[plot] wrote {out.resolve()}")

def plot_constellation(db_path, cache_path, output,
                       half_life_min=15, cutoff_hours=24,
                       target_points=3500, cell_top=3,
                       min_opacity=0.06, max_opacity=0.98,
                       min_radius=1.3, max_radius=5.2,
                       highlight_min=3.0, refresh_sec=20):
    folium = _ensure_folium()
    from folium import plugins
    cache = json.loads(Path(cache_path).read_text(encoding="utf-8"))
    activity = _load_activity(db_path)
    now = time.time(); cutoff = cutoff_hours*3600.0; hl = max(30.0, half_life_min*60.0)
    pts = []
    for ip, entry in cache.items():
        c = (entry.get("city") or {})
        lat, lon = c.get("lat"), c.get("lon")
        if lat is None or lon is None: continue
        org = ((entry.get("asn") or {}).get("org")) or "Unknown org"
        meta = activity.get(ip, {})
        last_ts = meta.get("last_ts"); last_from = meta.get("last_from")
        age = (now - last_ts) if last_ts else None
        if age is None or age > cutoff: continue
        score = 0.5 ** (age / hl); s = max(0, min(1, score)) ** 0.9
        opacity = min_opacity + (max_opacity - min_opacity) * s
        radius  = min_radius + (max_radius - min_radius) * s
        fresh   = bool(age is not None and age <= (highlight_min*60))
        pts.append({"ip":ip,"lat":lat,"lon":lon,"org":org,"age":age,"last_from":last_from,
                    "opacity":opacity,"radius":radius,"fresh":fresh,"score":s})
    if not pts:
        raise RuntimeError("No points within cutoff.")

    # grid thinning
    lat_min, lat_max = min(p["lat"] for p in pts), max(p["lat"] for p in pts)
    lon_min, lon_max = min(p["lon"] for p in pts), max(p["lon"] for p in pts)
    lat_rng, lon_rng = max(1e-6, lat_max-lat_min), max(1e-6, lon_max-lon_min)
    N, target = len(pts), max(200, target_points)
    if N > target:
        cells_needed = max(1, int(math.ceil(N / max(1, cell_top))))
        aspect = lat_rng / lon_rng
        rows = max(1, int(math.sqrt(cells_needed * aspect)))
        cols = max(1, int(math.sqrt(cells_needed / aspect)))
        cell_h, cell_w = lat_rng/rows, lon_rng/cols
        buckets = {}
        for p in pts:
            r = min(rows-1, int((p["lat"]-lat_min)/cell_h)) if cell_h>0 else 0
            c = min(cols-1, int((p["lon"]-lon_min)/cell_w)) if cell_w>0 else 0
            buckets.setdefault((r,c), []).append(p)
        thin = []
        for items in buckets.values():
            items.sort(key=lambda x: x["score"], reverse=True)
            thin.extend(items[:cell_top])
        pts = thin

    m = folium.Map(location=[17, 23], zoom_start=3, tiles="CartoDB dark_matter")
    plugins.Fullscreen(position="topright").add_to(m)
    plugins.MiniMap(toggle_display=True).add_to(m)
    plugins.MousePosition(position="bottomleft", prefix="lat/lon", separator=" , ").add_to(m)
    plugins.MeasureControl(position="topright", primary_length_unit="kilometers").add_to(m)

    layer = folium.FeatureGroup(name=f"Constellation (thinned)", show=True)
    for p in pts:
        color = _hash_color(p["org"])
        folium.CircleMarker(location=[p["lat"], p["lon"]], radius=p["radius"],
                            color=color, fill=True, fill_color=color,
                            fill_opacity=p["opacity"], opacity=p["opacity"], weight=0.6,
                            tooltip=f"{p['ip']} • {p['org']} • last {p['last_from']} {_nice_age(p['age'])} ago").add_to(layer)
        if p["fresh"]:
            folium.CircleMarker(location=[p["lat"], p["lon"]], radius=p["radius"]+2.0,
                                color="#ffffff", fill=False, opacity=min(1.0, p["opacity"]+0.25), weight=1.4).add_to(layer)
    layer.add_to(m)
    folium.LayerControl(collapsed=False).add_to(m)

    out = Path(output); allow_write(out)
    m.save(str(out))
    _write_map_refresh(out, refresh_sec)
    print(f"[plot] wrote {out.resolve()}")

# -------------------- CLI --------------------
def main():
    ap = argparse.ArgumentParser(description="Inspect and resolve torrents from dht_observatory.db")
    ap.add_argument("--db", default=DEFAULT_DB, help="Path to dht_observatory.db")
    ap.add_argument("--limit", type=int, default=50, help="Max rows to show/export per section")
    sub = ap.add_subparsers(dest="cmd")

    sp_sum = sub.add_parser("summary", help="Show overall counts and recent activity (optionally plot)")
    sp_sum.add_argument("--lookup", action="store_true",
                        help="Best-effort fetch names for recent announces without writing to DB")
    sp_sum.add_argument("--name-timeout", type=float, default=6.0,
                        help="Seconds to wait per best-effort name lookup")
    sp_sum.add_argument("--cache-names", action="store_true",
                        help="If set with --lookup, upsert looked-up metadata into the DB")
    sp_sum.add_argument("--plot", choices=["live","constellation"],
                        help="Generate an HTML map and exit after summary")
    sp_sum.add_argument("--plot-output", default="bt_map.html")
    sp_sum.add_argument("--geo-cache", default="geo_cache.json")
    sp_sum.add_argument("--city-mmdb", help="GeoLite2-City.mmdb (optional; used to build geo_cache.json if missing)")
    sp_sum.add_argument("--asn-mmdb",  help="GeoLite2-ASN.mmdb  (optional)")
    sp_sum.add_argument("--half-life-min", type=float, default=15.0)
    sp_sum.add_argument("--cutoff-hours", type=float, default=24.0)
    sp_sum.add_argument("--target-points", type=int, default=3500)
    sp_sum.add_argument("--cell-top", type=int, default=3)
    sp_sum.add_argument("--refresh-sec", type=int, default=20)

    sub.add_parser("unresolved", help="List pending/error announces lacking metadata")

    sp = sub.add_parser("metadata", help="List resolved metadata")
    sp.add_argument("--query", help="Filter by name or info_hash substring")

    sub.add_parser("top", help="Top infohashes by get_peers count")

    fp = sub.add_parser("files", help="Show file list for a torrent")
    fp.add_argument("--ih", required=True)

    ex = sub.add_parser("export", help="Export rows to JSONL")
    ex.add_argument("--what", required=True, choices=["metadata","announces","getpeers"])
    ex.add_argument("--out", required=True)

    rp = sub.add_parser("resolve", help="Resolve metadata (BEP-9) for announces; runs out-of-process")
    rp.add_argument("--batch", type=int, default=1, help="Rows per pass")
    rp.add_argument("--concurrency", type=int, default=24, help="Concurrent resolves")
    rp.add_argument("--timeout", type=float, default=8.0, help="Per-peer timeout (s)")
    rp.add_argument("--per-ih", type=int, default=8, help="Max peers tried per infohash (after announce)")
    rp.add_argument("--dht-peers", type=int, default=30, help="Peers to fetch via DHT get_peers")
    rp.add_argument("--since-minutes", type=int, default=120*60, help="Only resolve announces newer than this many minutes")
    rp.add_argument("--loop", action="store_true", help="Keep resolving as new announces arrive")

    args = ap.parse_args()

    # prime write whitelist for expected outputs
    allow_write(Path(args.db))
    if hasattr(args, "geo_cache"):  allow_write(Path(args.geo_cache))
    if hasattr(args, "plot_output"): allow_write(Path(args.plot_output))

    con = open_db(args.db)

    if args.cmd in (None, "summary"):
        if args.lookup and _GETMETA is None:
            print("[!] --lookup requested but dht_crawler_v4.get_metadata_from_infohash not importable.", file=sys.stderr)
        cmd_summary(con, args.limit, bool(args.lookup), float(getattr(args, "name_timeout", 6.0)), bool(getattr(args, "cache_names", False)))
        if getattr(args, "plot", None):
            build_geo_cache_if_needed(
                db_path=args.db,
                cache_path=getattr(args, "geo_cache", "geo_cache.json"),
                city_mmdb=getattr(args, "city_mmdb", None),
                asn_mmdb=getattr(args, "asn_mmdb", None),
                since_hours=72
            )
            try:
                if args.plot == "live":
                    plot_live_opacity(
                        db_path=args.db, cache_path=getattr(args, "geo_cache", "geo_cache.json"),
                        output=getattr(args, "plot_output", "bt_map.html"),
                        half_life_min=getattr(args, "half_life_min", 15.0),
                        cutoff_hours=getattr(args, "cutoff_hours", 24.0),
                        refresh_sec=getattr(args, "refresh_sec", 20)
                    )
                else:
                    plot_constellation(
                        db_path=args.db, cache_path=getattr(args, "geo_cache", "geo_cache.json"),
                        output=getattr(args, "plot_output", "bt_map.html"),
                        half_life_min=getattr(args, "half_life_min", 15.0),
                        cutoff_hours=getattr(args, "cutoff_hours", 24.0),
                        target_points=getattr(args, "target_points", 3500),
                        cell_top=getattr(args, "cell_top", 3),
                        refresh_sec=getattr(args, "refresh_sec", 20)
                    )
            except Exception as e:
                print(f"[plot] error: {e}", file=sys.stderr)

    elif args.cmd == "unresolved":
        cmd_unresolved(con, args.limit)
    elif args.cmd == "metadata":
        cmd_metadata(con, args.limit, getattr(args, "query", None))
    elif args.cmd == "top":
        cmd_top(con, args.limit)
    elif args.cmd == "files":
        cmd_files(con, args.ih)
    elif args.cmd == "export":
        cmd_export(con, args.what, args.out, args.limit)
    elif args.cmd == "resolve":
        con.close()
        asyncio.run(resolver_loop(
            db_path=args.db,
            batch=args.batch,
            concurrency=args.concurrency,
            timeout=args.timeout,
            per_ih=args.per_ih,
            dht_peers=args.dht_peers,
            since_minutes=args.since_minutes,
            loop_forever=args.loop
        ))
        return
    else:
        print("[!] unknown command")
    con.close()

if __name__ == "__main__":
    main()
 