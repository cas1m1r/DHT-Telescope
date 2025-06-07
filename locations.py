import json, os
from pathlib import Path
import geoip2.database

# Initialize the reader
db_loc = os.path.join(os.getcwd(),'GeoLite2-City_20250523','GeoLite2-City.mmdb')
city_reader = geoip2.database.Reader(db_loc)


def lookup_ip(ip, label):
    try:
        response = city_reader.city(ip)
        result = {
            "country": response.country.name,
            "city": response.city.name,
            "lat": response.location.latitude,
            "lon": response.location.longitude,
            "state": label
        }
    except:
        result = None
    return result


def update_geo():
    # load network
    network = json.loads(open('bittorrent_network.json','r').read())
    ip_list = list(network['Nodes'].keys())
    # get ip list
    cache_path = Path("geo_cache.json")
    geo_cache = {}
    # Look up and print
    for ip in ip_list:
        try:
            # print(f'Logging Location of {ip}')
            if ip not in geo_cache.keys():
                geo_cache[ip] = lookup_ip(ip, "node")
                geo_cache[ip]['peers'] = []
                for peer in network['Nodes'][ip]['peers']:
                    if peer not in geo_cache[ip]['peers']:
                        geo_cache[ip]['peers'].append(lookup_ip(peer, "peer"))
        except geoip2.errors.AddressNotFoundError:
            print(f"{ip} → Location not found")
    # Save cache
    open("geo_cache.json",'w').write(json.dumps(geo_cache,indent=2))

