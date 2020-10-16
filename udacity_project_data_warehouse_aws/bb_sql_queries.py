import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events_table;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs_table;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# SELECT

select_db_tables = ("""
select * from information_schema.tables where table_schema != 'pg_catalog' and table_schema != 'information_schema'
""")

# CREATE TABLES

songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplays(
                                songplay_id         INTEGER IDENTITY(0,1),
                                start_time          timestamp,
                                user_id             INTEGER,
                                level               TEXT,
                                song_id             TEXT,
                                artist_id           TEXT,
                                session_id          INTEGER,
                                location            TEXT,
                                user_agent          TEXT
);""")

user_table_create = ("""CREATE TABLE IF NOT EXISTS users(
                                user_id             INTEGER NOT NULL PRIMARY KEY,
                                first_name          TEXT,
                                last_name           TEXT,
                                gender              TEXT,
                                level               TEXT
);""")

song_table_create = ("""CREATE TABLE IF NOT EXISTS songs(
                                song_id             TEXT NOT NULL PRIMARY KEY,
                                title               TEXT,
                                artist_id           TEXT,
                                year                INTEGER,
                                duration            NUMERIC
);""")

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artists(
                                artist_id           TEXT NOT NULL PRIMARY KEY,
                                name                TEXT,
                                location            TEXT,
                                latitude            NUMERIC,
                                longitude           NUMERIC
);""")

time_table_create = ("""CREATE TABLE IF NOT EXISTS time(              
                                start_time          timestamp NOT NULL PRIMARY KEY,
                                hour                INTEGER,
                                day                 INTEGER,
                                week                INTEGER,
                                month               INTEGER,
                                year                INTEGER,
                                weekday             INTEGER     
);""")

staging_events_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_events_table (
                                staging_events_id       INTEGER IDENTITY(0,1),
                                artist                  TEXT,
                                auth                    TEXT,
                                first_name              TEXT,
                                gender                  TEXT,
                                item_in_session         INTEGER,                                                                
                                last_name               TEXT,                                                               
                                length                  NUMERIC,                                                              
                                level                   TEXT,
                                location                TEXT,
                                method                  TEXT,
                                page                    TEXT,
                                registration            BIGINT,
                                session_id              INTEGER,
                                song                    TEXT,
                                status                  INTEGER,
                                ts                      BIGINT,
                                user_agent              TEXT,
                                user_id                 INTEGER
);""")

staging_songs_table_create = ("""CREATE  TABLE IF NOT EXISTS staging_songs_table(
                                staging_songs_id    INTEGER IDENTITY(0,1),
                                num_songs           INTEGER,
                                artist_id           TEXT,
                                song_id             TEXT,
                                artist_latitude     NUMERIC,
                                artist_longitude    NUMERIC,
                                artist_location     TEXT,
                                artist_name         TEXT,
                                title               TEXT,
                                duration            NUMERIC,
                                year                INTEGER
);""")

# STAGING TABLES

staging_events_copy = ("""
copy staging_events_table from {}
credentials 'aws_iam_role={}'
compupdate off region 'us-west-2'
json {};
""").format(config.get("S3", "LOG_DATA"), config.get("IAM_ROLE", "ARN"), config.get("S3", "LOG_JSONPATH"))

staging_songs_copy = ("""
copy staging_songs_table from {}
credentials 'aws_iam_role={}'
compupdate off region 'us-west-2'
JSON 'auto' truncatecolumns;
""").format(config.get("S3", "SONG_DATA"), config.get("IAM_ROLE", "ARN"))

# FINAL TABLES

songplay_table_insert = ("""INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
SELECT 
    '1970-01-01'::date + ts/1000 * interval '1 second' as start_time,
    user_id,
    level,
    song_id,
    artist_id,
    session_id,
    location,
    user_agent
FROM (
    SELECT se.ts, se.user_id, se.level, sa.song_id, sa.artist_id, se.session_id, se.location, se.user_agent
    FROM staging_events_table se
    JOIN
        (
        SELECT songs.song_id, artists.artist_id, songs.title, artists.name, songs.duration
        FROM songs
        JOIN artists
            ON songs.artist_id = artists.artist_id) AS sa
    ON (sa.title = se.song
        AND sa.name = se.artist
        AND sa.duration = se.length
    )
    WHERE se.page = 'NextSong'
);
""")

user_table_insert = ("""INSERT INTO users (user_id, first_name, last_name, gender, level)
SELECT
    DISTINCT se.user_id,
    se.first_name,
    se.last_name,
    se.gender,
    se.level
FROM staging_events_table se
WHERE se.user_id IS NOT null AND se.ts = (select max(ts) FROM staging_events_table s WHERE s.user_id = se.user_id) AND se.page='NextSong'
;
""")

song_table_insert = ("""INSERT INTO songs (song_id, title, artist_id, year, duration)
SELECT 
    DISTINCT s.song_id,
    s.title,
    s.artist_id,
    s.year,
    s.duration
FROM staging_songs_table s;
""")

artist_table_insert = ("""INSERT INTO artists (artist_id, name, location, latitude, longitude)
SELECT
    DISTINCT s.artist_id,
    s.artist_name,
    s.artist_location,
    s.artist_latitude,
    s.artist_longitude 
FROM staging_songs_table s
WHERE artist_id IS NOT null
;
""")

time_table_insert = ("""INSERT INTO time (start_time, hour, day, week, month, year, weekday)
SELECT
    time_ts,
    EXTRACT(HOUR FROM time_ts) As hour,
    EXTRACT(DAY FROM time_ts) As day,
    EXTRACT(WEEK FROM time_ts) As week,
    EXTRACT(MONTH FROM time_ts) As month,
    EXTRACT(YEAR FROM time_ts) As year,
    EXTRACT(DOW FROM time_ts) As weekday
FROM (
SELECT
    DISTINCT s.ts,
    '1970-01-01'::date + s.ts/1000 * interval '1 second' as time_ts
FROM staging_events_table s
WHERE s.page='NextSong'
);
"""
)

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]

select_db_statements = [select_db_tables]

drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]

copy_table_queries = [staging_events_copy, staging_songs_copy]

insert_table_queries = [user_table_insert, song_table_insert, artist_table_insert, time_table_insert, songplay_table_insert]