import mysql.connector



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
db = mysql.connector.connect(host='localhost', user='root', password='', database='abc')



# =========================================
# Clear tables
# =========================================
print('Clearing tables... ', end='', flush=True)

cursor = db.cursor()
cursor.execute('TRUNCATE TABLE Players')
cursor.execute('TRUNCATE TABLE Maps')
cursor.execute('TRUNCATE TABLE MapCourses')
cursor.execute('TRUNCATE TABLE Times')
cursor.execute('TRUNCATE TABLE StartPosition')
cursor.execute('TRUNCATE TABLE VBPosition')

db.commit()
print('done!')



# =========================================
# Convert players
# =========================================
print('Converting players table... ', end='', flush=True)

cursor = db.cursor()
cursor.execute('SELECT SteamID, Name, Country, LastSeen FROM KZT_Players')
players = cursor.fetchall()

cursor = db.cursor()
for p in players:
	steam_id = p[0]
	alias = p[1]
	country = p[2]
	last_seen = p[3]

	account_id = steamid_to_usteamid(steam_id)

	sql = 'INSERT INTO Players (SteamID32, Alias, Country, IP, Cheater, LastPlayed, Created) VALUES (%s, %s, %s, %s, %s, %s, %s)'
	val = (account_id, alias, country, '', 0, last_seen, last_seen)

	cursor.execute(sql, val)

db.commit()
print('done!')



# =========================================
# Convert maps
# =========================================
print('Converting maps table... ', end='', flush=True)

cursor = db.cursor()
cursor.execute('INSERT INTO Maps (Name) SELECT MapName FROM KZT_Times GROUP BY MapName ORDER BY MapName ASC')

db.commit()
print('done!')



# =========================================
# Convert maps
# =========================================
print('Converting maps courses table... ', end='', flush=True)

cursor = db.cursor()
cursor.execute('INSERT INTO MapCourses (MapID, Course) SELECT MapID, 0 FROM Maps')

db.commit()
print('done!')



# =========================================
# Convert times
# =========================================
print('Converting times table... ', end='', flush=True)

# TP times
cursor = db.cursor()
cursor.execute('SELECT KZT_Times.SteamID, MapCourses.MapCourseID, CAST(FLOOR(KZT_Times.RunTime * 1000) AS INT), KZT_Times.Teleports ' \
			   'FROM KZT_Times ' \
			   'INNER JOIN Maps ON Maps.Name = KZT_Times.MapName ' \
			   'INNER JOIN MapCourses ON MapCourses.MapID = Maps.MapID ' \
			   'WHERE KZT_Times.RunTime > 0')
times_tp = cursor.fetchall()

cursor = db.cursor()
for t in times_tp:
	steam_id = t[0]
	map_course_id = t[1]
	run_ticks = t[2]
	teleports = t[3]

	account_id = steamid_to_usteamid(steam_id)

	sql = 'INSERT INTO Times (SteamID32, MapCourseID, Mode, Style, RunTime, Teleports) VALUES (%s, %s, %s, %s, %s, %s)'
	val = (account_id, map_course_id, 2, 0, run_ticks, teleports)

	cursor.execute(sql, val)

# PRO times
cursor = db.cursor()
cursor.execute('SELECT KZT_Times.SteamID, MapCourses.MapCourseID, CAST(FLOOR(KZT_Times.RunTimePro * 1000) AS INT) ' \
			   'FROM KZT_Times ' \
			   'INNER JOIN Maps ON Maps.Name = KZT_Times.MapName ' \
			   'INNER JOIN MapCourses ON MapCourses.MapID = Maps.MapID ' \
			   'WHERE KZT_Times.RunTimePro > 0')
times_pro = cursor.fetchall()

cursor = db.cursor()
for t in times_pro:
	steam_id = t[0]
	map_course_id = t[1]
	run_ticks = t[2]

	account_id = steamid_to_usteamid(steam_id)

	sql = 'INSERT INTO Times (SteamID32, MapCourseID, Mode, Style, RunTime, Teleports) VALUES (%s, %s, %s, %s, %s, %s)'
	val = (account_id, map_course_id, 2, 0, run_ticks, 0)

	cursor.execute(sql, val)

db.commit()
print('done!')



# =========================================
# Merge gokz players
# =========================================
print('Merging players table... ', end='', flush=True)

cursor = db.cursor()
cursor.execute('INSERT INTO Players (SteamID32, Alias, Country, IP, Cheater, LastPlayed, Created) ' \
			   'SELECT t.SteamID32, t.Alias, t.Country, t.IP, t.Cheater, t.LastPlayed, t.Created ' \
			   'FROM GOKZ_Players t ' \
			   'ON DUPLICATE KEY UPDATE Alias=t.Alias, Country=t.Country, IP=t.IP, Cheater=t.Cheater, LastPlayed=t.LastPlayed, Created=t.Created')

db.commit()
print('done!')



# =========================================
# Merge gokz times
# =========================================
print('Merging times table... ', end='', flush=True)

cursor = db.cursor()
cursor.execute('INSERT INTO Times (SteamID32, MapCourseID, Mode, Style, RunTime, Teleports) ' \
			   'SELECT GOKZ_Times.SteamID32, MapCourses.MapCourseID, GOKZ_Times.Mode, GOKZ_Times.Style, GOKZ_Times.RunTime, GOKZ_Times.Teleports ' \
			   'FROM GOKZ_Times ' \
			   'INNER JOIN GOKZ_MapCourses ON GOKZ_MapCourses.MapCourseID = GOKZ_Times.MapCourseID ' \
			   'INNER JOIN GOKZ_Maps ON GOKZ_Maps.MapID = GOKZ_MapCourses.MapID ' \
			   'INNER JOIN Maps ON Maps.Name = GOKZ_Maps.Name ' \
			   'INNER JOIN MapCourses ON MapCourses.MapID = Maps.MapID AND MapCourses.Course = GOKZ_MapCourses.Course ' \
			   'ON DUPLICATE KEY UPDATE Times.SteamID32=Times.SteamID32')

db.commit()
print('done!')
