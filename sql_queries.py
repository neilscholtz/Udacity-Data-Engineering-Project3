import configparser

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES
# STAGING TABLES
staging_events_table_create= ("""
    CREATE TABLE IF NOT EXISTS staging_events (
        artist VARCHAR,
        auth VARCHAR, 
        firstName VARCHAR,
        gender VARCHAR,
        itemInSession VARCHAR,
        lastName VARCHAR,
        length VARCHAR,
        level VARCHAR,  
        location VARCHAR, 
        method VARCHAR,
        page VARCHAR, 
        registration VARCHAR, 
        sessionId INT SORTKEY DISTKEY,
        song VARCHAR, 
        status INT,
        ts BIGINT, 
        userAgent VARCHAR, 
        userId INT
    )
""")

staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_songs (
        artist_id VARCHAR(25) SORTKEY DISTKEY,
        artist_latitude VARCHAR,
        artist_location VARCHAR(500),
        artist_longitude VARCHAR,
        artist_name VARCHAR(500),
        duration FLOAT, 
        num_songs INT,
        song_id VARCHAR,
        title VARCHAR(500), 
        year INT
    )
""")

# ANALYTICAL TABLES
songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplays (
        songplay_id BIGINT IDENTITY(0,1) PRIMARY KEY SORTKEY, 
        start_time TIMESTAMP NOT NULL REFERENCES time(start_time),
        user_id INT NOT NULL REFERENCES users(user_id) DISTKEY,
        level VARCHAR(5), 
        song_id VARCHAR(25) REFERENCES songs(song_id) NOT NULL,
        artist_id VARCHAR(25) REFERENCES artists(artist_id) NOT NULL,
        session_id INT,
        location VARCHAR(200),
        user_agent VARCHAR(200)
    )
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users (
        user_id INT PRIMARY KEY SORTKEY, 
        first_name VARCHAR(100),
        last_name VARCHAR(100),
        gender VARCHAR(1),
        level VARCHAR(5)
    ) DISTSTYLE ALL;
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs (
        song_id VARCHAR(25) PRIMARY KEY SORTKEY,
        title VARCHAR(200),
        artist_id VARCHAR(25),
        year INT, 
        duration DECIMAL(9)
    )
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists (
        artist_id VARCHAR(25) PRIMARY KEY SORTKEY,
        name VARCHAR(200),
        location VARCHAR(200),
        latitude DECIMAL(9),
        longitude DECIMAL(9)
    ) DISTSTYLE ALL;
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time (
        start_time TIMESTAMP PRIMARY KEY SORTKEY,
        hour INT,
        day INT,
        week INT,
        month INT,
        year INT,
        weekday INT
    ) DISTSTYLE ALL;
""")

# STAGING TABLES

staging_events_copy = ("""
    COPY staging_events FROM {}
    credentials 'aws_iam_role={}'
    format as json {}
    STATUPDATE ON
    region 'us-west-2';
""").format(config.get('S3', 'LOG_DATA'), config.get('IAM_ROLE','ARN'), config.get('S3','LOG_JSONPATH'))

staging_songs_copy = ("""
    COPY staging_songs FROM {}
    credentials 'aws_iam_role={}'
    format as json 'auto'
    ACCEPTINVCHARS AS '^'
    STATUPDATE ON
    region 'us-west-2'
""").format(config.get('S3','SONG_DATA'), config.get('IAM_ROLE','ARN'))

# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
        SELECT
            timestamp 'epoch' + e.ts / 1000 * interval '1 second' as start_time, 
            e.userId, 
            e.level, 
            s.song_id, 
            s.artist_id, 
            e.sessionId, 
            e.location, 
            e.userAgent
        FROM staging_events e
        JOIN staging_songs s ON e.artist=s.artist_name
        WHERE e.page = 'NextSong'
""")

user_table_insert = ("""
    INSERT INTO users (user_id, first_name, last_name, gender, level)
        SELECT 
            DISTINCT userId,
            firstName,
            lastName,
            gender,
            level
        FROM staging_events
        WHERE page = 'NextSong'
            
""")

song_table_insert = ("""
    INSERT INTO songs (song_id, title, artist_id, year, duration)
        SELECT 
            DISTINCT song_id,
            title,
            artist_id,
            year,
            duration
        FROM staging_songs
""")

artist_table_insert = ("""
    INSERT INTO artists (artist_id, name, location, latitude, longitude)
        SELECT 
            DISTINCT artist_id,
            artist_name, 
            artist_location,
            artist_latitude,
            artist_longitude
        FROM staging_songs
""")

time_table_insert = ("""
    INSERT INTO time (start_time, hour, day, week, month, year, weekday)
        SELECT
            DISTINCT TIMESTAMP 'epoch' + se.ts/1000 * INTERVAL '1 second' AS start_time,
            EXTRACT (hour FROM start_time) AS hour,
            EXTRACT (day FROM start_time) AS day,
            EXTRACT (week FROM start_time) AS week,
            EXTRACT (month FROM start_time) AS month,
            EXTRACT (year FROM start_time) AS year,
            EXTRACT (weekday FROM start_time) AS weekday
        FROM staging_events AS se
        WHERE se.page='NextSong'
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create, song_table_create, artist_table_create, time_table_create, songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
