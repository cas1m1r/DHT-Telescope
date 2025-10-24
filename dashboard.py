#!/usr/bin/env python3
"""
DHT Observatory Dashboard - Real-time monitoring of torrent hash resolution
Usage: python dashboard.py [--db path/to/dht_observatory.db] [--port 5000]
"""

from flask import Flask, render_template, jsonify, send_from_directory
import sqlite3
import json
import time
import argparse
from pathlib import Path
import os

app = Flask(__name__)
DB_PATH = "dht_observatory.db"


def get_db():
	"""Create database connection"""
	if not os.path.exists(DB_PATH):
		raise FileNotFoundError(f"Database not found: {DB_PATH}")
	con = sqlite3.connect(DB_PATH, timeout=10)
	con.row_factory = sqlite3.Row
	return con


def ago(ts):
	"""Human readable time ago"""
	if not ts:
		return "—"
	d = int(time.time()) - int(ts)
	if d < 60:
		return f"{d}s"
	if d < 3600:
		return f"{d // 60}m"
	if d < 86400:
		return f"{d // 3600}h"
	return f"{d // 86400}d"


def human_bytes(n):
	"""Human readable bytes"""
	if n is None:
		return "—"
	units = ["B", "KB", "MB", "GB", "TB"]
	f = float(n)
	for u in units:
		if f < 1024.0:
			return f"{f:.1f}{u}"
		f /= 1024.0
	return f"{f:.1f}PB"


@app.route('/')
def index():
	"""Serve main dashboard"""
	return render_template('dashboard.html')


@app.route('/api/stats')
def api_stats():
	"""Get overall statistics"""
	try:
		con = get_db()
		
		# Overall counts
		total_announces = con.execute("SELECT COUNT(*) as c FROM announces").fetchone()["c"]
		total_getpeers = con.execute("SELECT COUNT(*) as c FROM getpeers").fetchone()["c"]
		total_metadata = con.execute("SELECT COUNT(*) as c FROM metadata").fetchone()["c"]
		
		# Status breakdown
		status_counts = {}
		for row in con.execute("SELECT status, COUNT(*) as c FROM announces GROUP BY status").fetchall():
			status_counts[row["status"]] = row["c"]
		
		# Recent activity (last 5 minutes)
		five_min_ago = int(time.time()) - 300
		recent_announces = con.execute(
			"SELECT COUNT(*) as c FROM announces WHERE ts >= ?",
			(five_min_ago,)
		).fetchone()["c"]
		recent_getpeers = con.execute(
			"SELECT COUNT(*) as c FROM getpeers WHERE ts >= ?",
			(five_min_ago,)
		).fetchone()["c"]
		
		# Network stats from JSON if available
		network_stats = {}
		if os.path.exists("bittorrent_network.json"):
			try:
				with open("bittorrent_network.json", "r") as f:
					data = json.load(f)
					network_stats = data.get("Summary", {})
			except:
				pass
		
		con.close()
		
		return jsonify({
			"total_announces": total_announces,
			"total_getpeers": total_getpeers,
			"total_metadata": total_metadata,
			"status_counts": status_counts,
			"recent_announces_5m": recent_announces,
			"recent_getpeers_5m": recent_getpeers,
			"network": network_stats,
			"timestamp": int(time.time())
		})
	except Exception as e:
		return jsonify({"error": str(e)}), 500


@app.route('/api/resolved')
def api_resolved():
	"""Get recently resolved metadata"""
	try:
		limit = int(request.args.get('limit', 50))
		con = get_db()
		
		rows = con.execute("""
            SELECT info_hash, name, total_length, last_resolved_ts
            FROM metadata
            ORDER BY last_resolved_ts DESC
            LIMIT ?
        """, (limit,)).fetchall()
		
		results = []
		for r in rows:
			results.append({
				"info_hash": r["info_hash"],
				"name": r["name"] or "(unnamed)",
				"size": human_bytes(r["total_length"]),
				"size_bytes": r["total_length"],
				"ago": ago(r["last_resolved_ts"]),
				"timestamp": r["last_resolved_ts"]
			})
		
		con.close()
		return jsonify(results)
	except Exception as e:
		return jsonify({"error": str(e)}), 500


@app.route('/api/announces')
def api_announces():
	"""Get recent announces"""
	try:
		limit = int(request.args.get('limit', 50))
		con = get_db()
		
		rows = con.execute("""
            SELECT a.ts, a.ip, a.port, a.info_hash, a.status, a.attempts,
                   m.name
            FROM announces a
            LEFT JOIN metadata m ON m.info_hash = a.info_hash
            ORDER BY a.ts DESC
            LIMIT ?
        """, (limit,)).fetchall()
		
		results = []
		for r in rows:
			results.append({
				"info_hash": r["info_hash"],
				"peer": f"{r['ip']}:{r['port']}",
				"status": r["status"],
				"attempts": r["attempts"],
				"name": r["name"] or "",
				"ago": ago(r["ts"]),
				"timestamp": r["ts"]
			})
		
		con.close()
		return jsonify(results)
	except Exception as e:
		return jsonify({"error": str(e)}), 500


@app.route('/api/getpeers')
def api_getpeers():
	"""Get recent get_peers requests"""
	try:
		limit = int(request.args.get('limit', 30))
		con = get_db()
		
		rows = con.execute("""
            SELECT ts, ip, port, info_hash
            FROM getpeers
            ORDER BY ts DESC
            LIMIT ?
        """, (limit,)).fetchall()
		
		results = []
		for r in rows:
			results.append({
				"info_hash": r["info_hash"],
				"peer": f"{r['ip']}:{r['port']}",
				"ago": ago(r["ts"]),
				"timestamp": r["ts"]
			})
		
		con.close()
		return jsonify(results)
	except Exception as e:
		return jsonify({"error": str(e)}), 500


# HTML Template
TEMPLATE = '''<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>DHT Observatory Dashboard</title>
    <style>
        * {
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }

        body {
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            background: #0a0e27;
            color: #e4e4e7;
            overflow: hidden;
        }

        .container {
            display: flex;
            flex-direction: column;
            height: 100vh;
            padding: 20px;
            gap: 20px;
        }

        .header {
            text-align: center;
            padding-bottom: 10px;
            border-bottom: 2px solid #1e293b;
        }

        .header h1 {
            font-size: 2rem;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            -webkit-background-clip: text;
            -webkit-text-fill-color: transparent;
            margin-bottom: 5px;
        }

        .header p {
            color: #94a3b8;
            font-size: 0.9rem;
        }

        .stats-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 15px;
        }

        .stat-card {
            background: linear-gradient(135deg, #1e293b 0%, #0f172a 100%);
            border: 1px solid #334155;
            border-radius: 12px;
            padding: 20px;
            transition: transform 0.2s, box-shadow 0.2s;
        }

        .stat-card:hover {
            transform: translateY(-2px);
            box-shadow: 0 8px 16px rgba(0, 0, 0, 0.3);
        }

        .stat-card .label {
            color: #94a3b8;
            font-size: 0.85rem;
            text-transform: uppercase;
            letter-spacing: 0.5px;
            margin-bottom: 8px;
        }

        .stat-card .value {
            font-size: 1.8rem;
            font-weight: bold;
            color: #f1f5f9;
        }

        .stat-card.highlight .value {
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            -webkit-background-clip: text;
            -webkit-text-fill-color: transparent;
        }

        .content-grid {
            display: grid;
            grid-template-columns: 1fr 1fr;
            gap: 20px;
            flex: 1;
            overflow: hidden;
        }

        .panel {
            background: linear-gradient(135deg, #1e293b 0%, #0f172a 100%);
            border: 1px solid #334155;
            border-radius: 12px;
            padding: 20px;
            display: flex;
            flex-direction: column;
            overflow: hidden;
        }

        .panel h2 {
            font-size: 1.2rem;
            margin-bottom: 15px;
            color: #f1f5f9;
            display: flex;
            align-items: center;
            gap: 10px;
        }

        .status-badge {
            display: inline-block;
            padding: 2px 8px;
            border-radius: 12px;
            font-size: 0.75rem;
            font-weight: bold;
            text-transform: uppercase;
        }

        .status-pending { background: #fbbf24; color: #000; }
        .status-working { background: #3b82f6; color: #fff; }
        .status-ok { background: #10b981; color: #fff; }
        .status-error { background: #ef4444; color: #fff; }

        .list {
            flex: 1;
            overflow-y: auto;
            padding-right: 10px;
        }

        .list::-webkit-scrollbar {
            width: 8px;
        }

        .list::-webkit-scrollbar-track {
            background: #0f172a;
            border-radius: 4px;
        }

        .list::-webkit-scrollbar-thumb {
            background: #334155;
            border-radius: 4px;
        }

        .list::-webkit-scrollbar-thumb:hover {
            background: #475569;
        }

        .list-item {
            background: #0f172a;
            border: 1px solid #1e293b;
            border-radius: 8px;
            padding: 12px;
            margin-bottom: 10px;
            transition: all 0.2s;
            animation: slideIn 0.3s ease-out;
        }

        @keyframes slideIn {
            from {
                opacity: 0;
                transform: translateX(-20px);
            }
            to {
                opacity: 1;
                transform: translateX(0);
            }
        }

        .list-item:hover {
            border-color: #667eea;
            box-shadow: 0 4px 12px rgba(102, 126, 234, 0.1);
        }

        .list-item .hash {
            font-family: 'Courier New', monospace;
            color: #818cf8;
            font-size: 0.85rem;
            word-break: break-all;
        }

        .list-item .name {
            color: #f1f5f9;
            font-weight: 500;
            margin: 5px 0;
            overflow: hidden;
            text-overflow: ellipsis;
            white-space: nowrap;
        }

        .list-item .meta {
            display: flex;
            justify-content: space-between;
            align-items: center;
            color: #94a3b8;
            font-size: 0.8rem;
            margin-top: 5px;
        }

        .pulse {
            display: inline-block;
            width: 8px;
            height: 8px;
            border-radius: 50%;
            background: #10b981;
            animation: pulse 2s infinite;
            margin-right: 8px;
        }

        @keyframes pulse {
            0%, 100% { opacity: 1; }
            50% { opacity: 0.5; }
        }

        .loading {
            text-align: center;
            color: #94a3b8;
            padding: 20px;
        }

        @media (max-width: 1024px) {
            .content-grid {
                grid-template-columns: 1fr;
            }
        }
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>🌐 DHT Observatory</h1>
            <p>Real-time BitTorrent DHT monitoring and metadata resolution</p>
        </div>

        <div class="stats-grid">
            <div class="stat-card highlight">
                <div class="label">Resolved Torrents</div>
                <div class="value" id="stat-metadata">—</div>
            </div>
            <div class="stat-card">
                <div class="label">Total Announces</div>
                <div class="value" id="stat-announces">—</div>
            </div>
            <div class="stat-card">
                <div class="label">Known Nodes</div>
                <div class="value" id="stat-nodes">—</div>
            </div>
            <div class="stat-card">
                <div class="label">Get Peers (5m)</div>
                <div class="value" id="stat-getpeers">—</div>
            </div>
        </div>

        <div class="content-grid">
            <div class="panel">
                <h2><span class="pulse"></span>Resolved Metadata</h2>
                <div class="list" id="resolved-list">
                    <div class="loading">Loading...</div>
                </div>
            </div>

            <div class="panel">
                <h2><span class="pulse"></span>Recent Announces</h2>
                <div class="list" id="announces-list">
                    <div class="loading">Loading...</div>
                </div>
            </div>
        </div>
    </div>

    <script>
        let lastResolvedTimestamp = 0;
        let lastAnnounceTimestamp = 0;

        function formatNumber(num) {
            return new Intl.NumberFormat().format(num);
        }

        async function updateStats() {
            try {
                const response = await fetch('/api/stats');
                const data = await response.json();

                document.getElementById('stat-metadata').textContent = formatNumber(data.total_metadata || 0);
                document.getElementById('stat-announces').textContent = formatNumber(data.total_announces || 0);
                document.getElementById('stat-nodes').textContent = formatNumber(data.network?.known_nodes || 0);
                document.getElementById('stat-getpeers').textContent = formatNumber(data.recent_getpeers_5m || 0);
            } catch (err) {
                console.error('Stats update failed:', err);
            }
        }

        async function updateResolved() {
            try {
                const response = await fetch('/api/resolved?limit=50');
                const data = await response.json();
                const container = document.getElementById('resolved-list');

                if (data.length === 0) {
                    container.innerHTML = '<div class="loading">No resolved metadata yet...</div>';
                    return;
                }

                // Only update if we have new items
                const newestTimestamp = data[0]?.timestamp || 0;
                if (newestTimestamp <= lastResolvedTimestamp && container.children.length > 0) {
                    return;
                }
                lastResolvedTimestamp = newestTimestamp;

                container.innerHTML = data.map(item => `
                    <div class="list-item">
                        <div class="hash">${item.info_hash}</div>
                        <div class="name" title="${item.name}">${item.name}</div>
                        <div class="meta">
                            <span>${item.size}</span>
                            <span>${item.ago} ago</span>
                        </div>
                    </div>
                `).join('');
            } catch (err) {
                console.error('Resolved update failed:', err);
            }
        }

        async function updateAnnounces() {
            try {
                const response = await fetch('/api/announces?limit=50');
                const data = await response.json();
                const container = document.getElementById('announces-list');

                if (data.length === 0) {
                    container.innerHTML = '<div class="loading">No announces yet...</div>';
                    return;
                }

                // Only update if we have new items
                const newestTimestamp = data[0]?.timestamp || 0;
                if (newestTimestamp <= lastAnnounceTimestamp && container.children.length > 0) {
                    return;
                }
                lastAnnounceTimestamp = newestTimestamp;

                container.innerHTML = data.map(item => `
                    <div class="list-item">
                        <div class="hash">${item.info_hash}</div>
                        ${item.name ? `<div class="name" title="${item.name}">${item.name}</div>` : ''}
                        <div class="meta">
                            <span>${item.peer}</span>
                            <span class="status-badge status-${item.status}">${item.status}</span>
                            <span>${item.ago} ago</span>
                        </div>
                    </div>
                `).join('');
            } catch (err) {
                console.error('Announces update failed:', err);
            }
        }

        // Initial load
        updateStats();
        updateResolved();
        updateAnnounces();

        // Update intervals
        setInterval(updateStats, 5000);      // Stats every 5s
        setInterval(updateResolved, 3000);   // Resolved every 3s
        setInterval(updateAnnounces, 2000);  // Announces every 2s
    </script>
</body>
</html>
'''

# Save template
os.makedirs('templates', exist_ok=True)
with open('templates/dashboard.html', 'w', encoding='utf-8') as f:
	f.write(TEMPLATE)

# Fix missing import
from flask import request


def main():
	parser = argparse.ArgumentParser(description='DHT Observatory Dashboard')
	parser.add_argument('--db', default='dht_observatory.db', help='Path to database')
	parser.add_argument('--port', type=int, default=5000, help='Port to run on')
	parser.add_argument('--host', default='127.0.0.1', help='Host to bind to')
	args = parser.parse_args()
	
	global DB_PATH
	DB_PATH = args.db
	
	print(f"[*] Starting DHT Observatory Dashboard")
	print(f"    Database: {DB_PATH}")
	print(f"    URL: http://{args.host}:{args.port}")
	print(f"[*] Press Ctrl+C to stop")
	
	app.run(host=args.host, port=args.port, debug=False, threaded=True)


if __name__ == '__main__':
	main()