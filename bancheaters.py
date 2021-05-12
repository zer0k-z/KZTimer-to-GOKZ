import mysql.connector
import urllib.request
import json 
from datetime import datetime



# =========================================
# Helpers
# =========================================
def steamid_to_usteamid(steamid):
	steamid_split = steamid.split(':')
	y = int(steamid_split[1])
	z = int(steamid_split[2])
	return z * 2 + y



# =========================================
# Globals
# =========================================
limit = 150
db = mysql.connector.connect(host='localhost', user='root', password='Aa1234567', database='abc')


# =========================================
# Caching players
# =========================================
print('Caching players... ', end='', flush=True)

cursor = db.cursor()
cursor.execute('SELECT SteamID32, Alias FROM Players')
fetched_players = cursor.fetchall()
players = {}

for p in fetched_players:
	steam_id = p[0]
	alias = p[1]
	players[steam_id] = alias

print('done!', flush=True)



# =========================================
# Caching players
# =========================================

offset = 0

while True:
	with urllib.request.urlopen('http://kztimerglobal.com/api/v1.0/bans?offset={}&limit={}'.format(offset, limit)) as url:
		data = json.loads(url.read().decode())
		if len(data) <= 0:
			break

		for b in data:
			steam_id = steamid_to_usteamid(b['steam_id'])
			if steam_id not in players:
				continue

			expires_on = datetime.fromisoformat(b['expires_on'])
			if expires_on < datetime.now():
				print('! {}'.format(steam_id), flush=True)
				continue

			print('* {}'.format(steam_id), flush=True)

			cursor = db.cursor()
			cursor.execute('UPDATE Players SET Cheater = 1 WHERE SteamID32 = {}'.format(steam_id))

	offset += limit
