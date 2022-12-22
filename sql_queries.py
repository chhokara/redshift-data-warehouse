import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')
LOG_DATA = config.get('S3', 'LOG_DATA')
LOG_JSONPATH = config.get('S3', 'LOG_JSONPATH')
SONG_DATA = config.get('S3', 'SONG_DATA')
ARN = config.get('IAM_ROLE', 'ARN')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events CASCADE"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs CASCADE"
songplay_table_drop = "DROP TABLE IF EXISTS songplays CASCADE"
user_table_drop = "DROP TABLE IF EXISTS users CASCADE"
song_table_drop = "DROP TABLE IF EXISTS songs CASCADE"
artist_table_drop = "DROP TABLE IF EXISTS artists CASCADE"
time_table_drop = "DROP TABLE IF EXISTS time CASCADE"

# CREATE TABLES

staging_events_table_create= ("""
    CREATE TABLE staging_events (
        artist varchar,
        auth varchar,
        firstName varchar,
        gender varchar,
        itemInSession int,
        lastName varchar,
        length float,
        level varchar,
        location varchar,
        method varchar,
        page varchar,
        registration bigint,
        sessionId int,
        song varchar,
        status int,
        ts timestamp,
        userAgent varchar,
        userId int
    )
""")

staging_songs_table_create = ("""
    CREATE TABLE staging_songs (
        num_songs int,
        artist_id varchar,
        artist_latitude double precision,
        artist_longitude double precision,
        artist_location varchar,
        artist_name varchar,
        song_id varchar,
        title varchar,
        duration numeric,
        year int
    )
""")

user_table_create = ("""
    CREATE TABLE users (
        user_id int PRIMARY KEY,
        first_name varchar,
        last_name varchar,
        gender varchar,
        level varchar
    )
    diststyle all
""")

song_table_create = ("""
    CREATE TABLE songs (
        song_id varchar PRIMARY KEY,
        title varchar NOT NULL,
        artist_id varchar,
        year int,
        duration numeric NOT NULL
    )
    diststyle all
""")

artist_table_create = ("""
    CREATE TABLE artists (
        artist_id varchar PRIMARY KEY,
        name varchar NOT NULL,
        location varchar,
        latitude double precision,
        longitude double precision
    )
    diststyle all
""")

time_table_create = ("""
    CREATE TABLE time (
        start_time timestamp PRIMARY KEY distkey,
        hour int,
        day int,
        week int,
        month int,
        year int,
        weekday int
    )
""")

songplay_table_create = ("""
    CREATE TABLE songplays (
        songplay_id int IDENTITY(0,1) PRIMARY KEY,
        start_time timestamp NOT NULL distkey,
        user_id int NOT NULL,
        level varchar,
        song_id varchar,
        artist_id varchar,
        session_id int NOT NULL,
        location varchar,
        user_agent varchar,
        FOREIGN KEY (start_time) REFERENCES time (start_time),
        FOREIGN KEY (user_id) REFERENCES users (user_id),
        FOREIGN KEY (song_id) REFERENCES songs (song_id),
        FOREIGN KEY (artist_id) REFERENCES artists (artist_id)
    )
""")

# STAGING TABLES

staging_events_copy = ("""
    copy staging_events from {}
    credentials 'aws_iam_role={}'
    json {}
    region 'us-west-2'
    timeformat 'epochmillisecs'
""").format(LOG_DATA, ARN, LOG_JSONPATH)

staging_songs_copy = ("""
    copy staging_songs from {}
    credentials 'aws_iam_role={}'
    json 'auto'
    region 'us-west-2'
""").format(SONG_DATA, ARN)

# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    SELECT ts        AS start_time,
           userId    AS user_id,
           level,
           song_id,
           artist_id,
           sessionId AS session_id,
           location,
           userAgent AS user_agent
    FROM staging_events, staging_songs
    WHERE staging_events.artist = staging_songs.artist_name AND
          staging_events.song = staging_songs.title AND
          staging_events.length = staging_songs.duration AND
          staging_events.page = 'NextSong' AND
          staging_events.userId IS NOT NULL
""")

user_table_insert = ("""
    INSERT INTO users (user_id, first_name, last_name, gender, level)
    SELECT userId    AS user_id,
           firstName AS first_name,
           lastName  AS last_name,
           gender,
           level
    FROM staging_events
    WHERE staging_events.page = 'NextSong' AND 
          staging_events.userId IS NOT NULL
""")

song_table_insert = ("""
    INSERT INTO songs (song_id, title, artist_id, year, duration)
    SELECT song_id,
           title,
           artist_id,
           year,
           duration
    FROM staging_songs
""")

artist_table_insert = ("""
    INSERT INTO artists (artist_id, name, location, latitude, longitude)
    SELECT artist_id,
           artist_name      AS name,
           artist_location  AS location,
           artist_latitude  AS latitude,
           artist_longitude AS longitude
    FROM staging_songs
""")

time_table_insert = ("""
    INSERT INTO time (start_time, hour, day, week, month, year, weekday)
    SELECT ts                       AS start_time,
           EXTRACT(hour FROM ts)    AS hour,
           EXTRACT(day FROM ts)     AS day,
           EXTRACT(week FROM ts)    AS week,
           EXTRACT(month FROM ts)   AS month,
           EXTRACT(year FROM ts)    AS year,
           EXTRACT(dow FROM ts)     AS weekday
    FROM staging_events
    WHERE staging_events.page = 'NextSong' AND 
          staging_events.userId IS NOT NULL
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create, song_table_create, artist_table_create, time_table_create, songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop, songplay_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
