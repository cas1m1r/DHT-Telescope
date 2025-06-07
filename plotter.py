import geoip2.database
import hashlib
import folium
import json
import os

asn_loc = os.path.join(os.getcwd(),'GeoLite2-ASN_20250527','GeoLite2-ASN.mmdb')
asn_reader = geoip2.database.Reader(asn_loc)


def lookup_asn(ip):
    try:
        response = asn_reader.asn(ip)
        return {
            "asn": response.autonomous_system_number,
            "org": response.autonomous_system_organization
        }
    except:
        return None

def load_cached_locations():
	cache_path = 'geo_cache.json'
	if not os.path.isfile(cache_path):
		print(f'[X] No cached Data Found')
		result = {}
	else:
		result = json.loads(open(cache_path,'r').read())
	return result



def fill_map_marker(m,geo, ip):
	colors = {'node': '77', 'peer': 'green'}
	sizes  = {'node': 0.2, 'peer': 0.1}
	asn_info = lookup_asn(ip)
	org = asn_info['org'] if asn_info else 'unknown'

	# Assign color by organization hash

	hex_tone = hashlib.md5(org.encode()).hexdigest()[:4]
	colors = {'node': f'#00{hex_tone}', 'peer': f'#{hex_tone}00'}
	try:
		hex_color = colors[geo['state']]
		if type(geo) != None:
			folium.CircleMarker(
				location=[geo['lat'], geo['lon']],
				radius=sizes[geo['state']],
				color=hex_color,
				fill_color=hex_color,
				fill_opacity=0.1,
				tooltip=f"{ip}\n{org}"
			).add_to(m)
	except:
		pass
	# folium.CircleMarker(
	# 	location=[geo["lat"], geo["lon"]],
	# 	radius=sizes[label],
	# 	color=colors[label],
	# 	fill=True,
	# 	fill_opacity=0.5
	# 	# tooltip=f"{ip} - {len(data['infohashes'])} hashes"
	# ).add_to(m)
	return m


def make_map(geo_data):
	m = folium.Map(location=[20, 0], zoom_start=2.77,tiles='CartoDB dark_matter')
	for ip in geo_data.keys():
		if len(ip.split('.'))>3:
			geo = geo_data[ip]
			if "lat" in geo and "lon" in geo:
				m = fill_map_marker(m, geo, ip)
				for peer in geo['peers']:
					m = fill_map_marker(m, peer, ip)
	m.save('bt_mapping.html')
	with open("bt_mapping.html", "r") as f:
		html = f.read()

	html = html.replace(
		"<head>",
		"<head>\n<meta http-equiv=\"refresh\" content=\"30\">"
	)

	with open("bt_mapping.html", "w") as f:
		f.write(html)


def update():
	make_map(load_cached_locations())
