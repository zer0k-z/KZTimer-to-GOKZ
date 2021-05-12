import sqlite3
from sqlite3 import Error

# Purpose: Add GOKZ tables to the KZTimer database along with the corresponding data

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
database = r"D:\Archive\old_db.sq3"
db = sqlite3.connect(database)



# =========================================
# Create tables
# =========================================

cursor = db.cursor()

cursor.execute("\
CREATE TABLE IF NOT EXISTS Players ( \
    SteamID32 INTEGER NOT NULL, \
    Alias TEXT, \
    Country TEXT, \
    IP TEXT, \
    Cheater INTEGER NOT NULL DEFAULT '0', \
    LastPlayed TIMESTAMP NULL DEFAULT NULL, \
    Created TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, \
    CONSTRAINT PK_Player PRIMARY KEY (SteamID32))")

cursor.execute("\
CREATE TABLE IF NOT EXISTS Maps ( \
    MapID INTEGER NOT NULL, \
    Name VARCHAR(32) NOT NULL UNIQUE, \
    LastPlayed TIMESTAMP NULL DEFAULT NULL, \
    Created TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, \
    CONSTRAINT PK_Maps PRIMARY KEY (MapID))")

cursor.execute("\
CREATE TABLE IF NOT EXISTS MapCourses ( \
    MapCourseID INTEGER NOT NULL, \
    MapID INTEGER NOT NULL, \
    Course INTEGER NOT NULL, \
    Created INTEGER NOT NULL DEFAULT CURRENT_TIMESTAMP, \
    CONSTRAINT PK_MapCourses PRIMARY KEY (MapCourseID), \
    CONSTRAINT UQ_MapCourses_MapIDCourse UNIQUE (MapID, Course), \
    CONSTRAINT FK_MapCourses_MapID FOREIGN KEY (MapID) REFERENCES Maps(MapID) \
    ON UPDATE CASCADE ON DELETE CASCADE)")

cursor.execute("\
CREATE TABLE IF NOT EXISTS Times ( \
    TimeID INTEGER NOT NULL, \
    SteamID32 INTEGER NOT NULL, \
    MapCourseID INTEGER NOT NULL, \
    Mode INTEGER NOT NULL, \
    Style INTEGER NOT NULL, \
    RunTime INTEGER NOT NULL, \
    Teleports INTEGER NOT NULL, \
    Created INTEGER NOT NULL DEFAULT CURRENT_TIMESTAMP, \
    CONSTRAINT PK_Times PRIMARY KEY (TimeID), \
    CONSTRAINT FK_Times_SteamID32 FOREIGN KEY (SteamID32) REFERENCES Players(SteamID32) \
    ON UPDATE CASCADE ON DELETE CASCADE, CONSTRAINT FK_Times_MapCourseID \
    FOREIGN KEY (MapCourseID) REFERENCES MapCourses(MapCourseID) \
    ON UPDATE CASCADE ON DELETE CASCADE)")

cursor.execute("\
CREATE TABLE IF NOT EXISTS Jumpstats ( \
    JumpID INTEGER NOT NULL, \
    SteamID32 INTEGER NOT NULL, \
    JumpType INTEGER NOT NULL, \
    Mode INTEGER NOT NULL, \
    Distance INTEGER NOT NULL, \
    IsBlockJump INTEGER NOT NULL, \
    Block INTEGER NOT NULL, \
    Strafes INTEGER NOT NULL, \
    Sync INTEGER NOT NULL, \
    Pre INTEGER NOT NULL, \
    Max INTEGER NOT NULL, \
    Airtime INTEGER NOT NULL, \
    Created INTEGER NOT NULL DEFAULT CURRENT_TIMESTAMP, \
    CONSTRAINT PK_Jumpstats PRIMARY KEY (JumpID), \
    CONSTRAINT FK_Jumpstats_SteamID32 FOREIGN KEY (SteamID32) REFERENCES Players(SteamID32) \
    ON UPDATE CASCADE ON DELETE CASCADE)")

cursor.execute("\
CREATE TABLE IF NOT EXISTS VBPosition ( \
	SteamID32 INTEGER NOT NULL, \
	MapID INTEGER NOT NULL, \
	X REAL NOT NULL, \
	Y REAL NOT NULL, \
	Z REAL NOT NULL, \
	Course INTEGER NOT NULL, \
	IsStart INTEGER NOT NULL, \
	CONSTRAINT PK_VBPosition PRIMARY KEY (SteamID32, MapID, IsStart), \
    CONSTRAINT FK_VBPosition_SteamID32 FOREIGN KEY (SteamID32) REFERENCES Players(SteamID32), \
    CONSTRAINT FK_VBPosition_MapID FOREIGN KEY (MapID) REFERENCES Maps(MapID) \
    ON UPDATE CASCADE ON DELETE CASCADE)")

cursor.execute("\
CREATE TABLE IF NOT EXISTS StartPosition ( \
	SteamID32 INTEGER NOT NULL, \
	MapID INTEGER NOT NULL, \
	X REAL NOT NULL, \
	Y REAL NOT NULL, \
	Z REAL NOT NULL, \
	Angle0 REAL NOT NULL, \
	Angle1 REAL NOT NULL, \
	CONSTRAINT PK_StartPosition PRIMARY KEY (SteamID32, MapID), \
    CONSTRAINT FK_StartPosition_SteamID32 FOREIGN KEY (SteamID32) REFERENCES Players(SteamID32) \
    CONSTRAINT FK_StartPosition_MapID FOREIGN KEY (MapID) REFERENCES Maps(MapID) \
    ON UPDATE CASCADE ON DELETE CASCADE)")


# =========================================
# Convert players
# =========================================
print('Converting players table... ', end='', flush=True)

cursor = db.cursor()
cursor.execute('SELECT SteamID, Name, Country, LastSeen FROM playerrank')
players = cursor.fetchall()

cursor = db.cursor()
for p in players:
	steam_id = p[0]
	alias = p[1]
	country = p[2]
	last_seen = p[3]

	account_id = steamid_to_usteamid(steam_id)

	sql = 'INSERT INTO Players (SteamID32, Alias, Country, IP, Cheater, LastPlayed, Created) VALUES (?, ?, ?, ?, ?, ?, ?)'
	val = (account_id, alias, country, '', 0, last_seen, last_seen)

	cursor.execute(sql, val)

db.commit()
print('done!')



# =========================================
# Convert maps
# =========================================
print('Converting maps table... ', end='', flush=True)

cursor = db.cursor()
cursor.execute('INSERT INTO Maps (Name) SELECT MapName FROM playertimes GROUP BY MapName ORDER BY MapName ASC')

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
cursor.execute('SELECT playertimes.SteamID, MapCourses.MapCourseID, CAST(playertimes.RunTime * 1000 AS INT), playertimes.Teleports ' \
			   'FROM playertimes ' \
			   'INNER JOIN Maps ON Maps.Name = playertimes.MapName ' \
			   'INNER JOIN MapCourses ON MapCourses.MapID = Maps.MapID ' \
			   'WHERE playertimes.RunTime > 0')
times_tp = cursor.fetchall()

cursor = db.cursor()
for t in times_tp:
	steam_id = t[0]
	map_course_id = t[1]
	run_ticks = t[2]
	teleports = t[3]

	account_id = steamid_to_usteamid(steam_id)

	sql = 'INSERT INTO Times (SteamID32, MapCourseID, Mode, Style, RunTime, Teleports) VALUES (?, ?, ?, ?, ?, ?)'
	val = (account_id, map_course_id, 2, 0, run_ticks, teleports)

	cursor.execute(sql, val)

# PRO times
cursor = db.cursor()
cursor.execute('SELECT playertimes.SteamID, MapCourses.MapCourseID, CAST(playertimes.RunTimePro * 1000 AS INT) ' \
			   'FROM playertimes ' \
			   'INNER JOIN Maps ON Maps.Name = playertimes.MapName ' \
			   'INNER JOIN MapCourses ON MapCourses.MapID = Maps.MapID ' \
			   'WHERE playertimes.RunTimePro > 0')
times_pro = cursor.fetchall()

cursor = db.cursor()
for t in times_pro:
	steam_id = t[0]
	map_course_id = t[1]
	run_ticks = t[2]

	account_id = steamid_to_usteamid(steam_id)

	sql = 'INSERT INTO Times (SteamID32, MapCourseID, Mode, Style, RunTime, Teleports) VALUES (?, ?, ?, ?, ?, ?)'
	val = (account_id, map_course_id, 2, 0, run_ticks, 0)

	cursor.execute(sql, val)

db.commit()
print('done!')