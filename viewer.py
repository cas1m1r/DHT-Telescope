import os
import random
from datetime import datetime
from plotter import *
from locations import update_geo
import json
import time
import os


def visualize(timeout):
	start = time.time()
	plotting = True
	t = datetime.fromtimestamp(time.time())
	folder = f'Snapshots_{t.month}_{t.day}_{t.year}'

	snap_shot_interval = 30
	if not os.path.isdir(folder):
		os.mkdir(folder)

	while plotting:
		try:
			dt = round(time.time() - start)
			if dt%timeout==0:
				if os.path.isfile('bittorrent_network.json') and not os.path.isfile('.lock'):
					print(f'[+] Updating Map')
					update_geo()
					update()
				elif not os.path.isfile('bittorrent_network.json'):
					print(f'Making Blank map')
					make_map({})

				time.sleep(1 * random.randint(1, timeout)/timeout)
			else:
				print(f'Waiting...{dt}')
				time.sleep(1 * random.randint(1, timeout)/timeout)
			# make snapshots/backups
			if dt % snap_shot_interval == 0:
				t = datetime.fromtimestamp(time.time())
				print(f'[+] Making Network Snapshot [{t.hour}:{t.min}:{t.second} {t.month}/{t.day}]')
				snap_fold = os.path.join(os.getcwd(),folder,f'{t.hour}_{t.minute}_{t.second}')
				if not os.path.isdir(snap_fold):
					os.mkdir(snap_fold)
				os.system(f'copy geo_cache.json {snap_fold}')
				os.system(f'copy bt_mapping.html {snap_fold}')
		except KeyboardInterrupt:
			print(f'[+] Shutting down Observatory')
			break
		except:
			pass


if __name__ == '__main__':
	visualize(7)
