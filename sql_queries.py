import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artis;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

staging_events_table_create= ("""
    CREATE TABLE IF NOT EXISTS staging_events(
        artist          VARCHAR,
        auth            VARCHAR,
        firstName       VARCHAR,
        gender          VARCHAR,
        itemInSession   INTEGER,
        lastName        VARCHAR,
        length          DECIMAL,
        level           VARCHAR,
        location        VARCHAR,
        method          VARCHAR,
        page            VARCHAR,
        registration    VARCHAR,
        sessionId       BIGINT,
        song            VARCHAR,
        status          INTEGER,
        ts              BIGINT,
        userAgent       VARCHAR,
        userId          INTEGER
    );
""")

staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_songs(
        num_songs           INTEGER, 
        artist_id           VARCHAR     NOT NULL,
        artist_latitude     DECIMAL,
        artist_longitude    DECIMAL,
        artist_location     VARCHAR,
        artist_name         VARCHAR     NOT NULL,
        song_id             VARCHAR     NOT NULL,
        title               VARCHAR     NOT NULL,
        duration            DECIMAL     NOT NULL,
        year                INTEGER    
    );
""")

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplays(
        songplay_id     INTEGER     IDENTITY(0,1),
        start_time      TIMESTAMP   DISTKEY SORTKEY,
        user_id         INTEGER     NOT NULL,
        level           VARCHAR,
        song_id         VARCHAR,
        artist_id       VARCHAR,
        session_id      BIGINT,
        location        VARCHAR,
        user_agent      VARCHAR
    );
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users(
        user_id         VARCHAR     PRIMARY KEY DISTKEY,
        first_name      VARCHAR     NOT NULL,
        last_name       VARCHAR     NOT NULL,
        gender          VARCHAR,
        level           VARCHAR     NOT NULL
    );
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs(
        song_id         VARCHAR     PRIMARY KEY DISTKEY,
        title           VARCHAR     NOT NULL,
        artist_id       VARCHAR     NOT NULL,
        year            INTEGER,
        duration        DECIMAL     NOT NULL
    );
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists(
        artist_id       VARCHAR     PRIMARY KEY DISTKEY,
        name            VARCHAR     NOT NULL,
        location        VARCHAR,
        latitude        DECIMAL,
        longitude       DECIMAL
    );
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time(
        start_time  TIMESTAMP   NOT NULL SORTKEY DISTKEY,
        hour        INTEGER     NOT NULL,
        day         INTEGER     NOT NULL,
        week        INTEGER     NOT NULL,
        month       INTEGER     NOT NULL,
        year        INTEGER     NOT NULL,
        weekday     INTEGER     NOT NULL
    );
""")

# STAGING TABLES

staging_events_copy = (f"""
    COPY staging_events 
    FROM {config.get('S3','LOG_DATA')}
    IAM_ROLE {config.get('IAM_ROLE', 'ARN')}
    JSON {config.get('S3','LOG_JSONPATH')}
    region 'us-west-2'
    COMPUPDATE OFF;
""")

staging_songs_copy = (f"""
    COPY staging_songs
    FROM {config.get('S3','SONG_DATA')}
    IAM_ROLE {config.get('IAM_ROLE', 'ARN')}
    JSON 'auto'
    region 'us-west-2'
    COMPUPDATE OFF;
""")

# FINAL TABLES

songplay_table_insert = ("""
        INSERT INTO 
            songplayS (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
        (
            SELECT DISTINCT 
                TIMESTAMP 'epoch' + (event.ts / 1000)  * INTERVAL '1 second',
                event.userId ,
                event.level ,
                songs.song_id ,
                songs.artist_id ,
                event.sessionId ,
                event.location ,
                event.userAgent 
            FROM 
                staging_events event
                LEFT JOIN staging_songs songs ON event.song = songs.title 
                                                 AND event.artist = songs.artist_name 
            WHERE 
                event.page = 'NextSong'
        );
""")

user_table_insert = ("""
    INSERT INTO 
        users (user_id, first_name, last_name, gender, level)
        (
            SELECT DISTINCT 
                userId ,
                firstName ,
                lastName ,
                gender,
                level
            FROM 
                staging_events
            WHERE 
                userId IS NOT NULL
                AND page = 'NextSong'
        );
""")

song_table_insert = ("""
    INSERT INTO 
        songs (song_id, title, artist_id, year, duration)
        (
            SELECT DISTINCT 
                song_id,
                title,
                artist_id,
                year,
                duration
            FROM 
                staging_songs
            WHERE 
                song_id IS NOT NULL
    );
""")

artist_table_insert = ("""
    INSERT INTO 
        artists (artist_id, name, location, latitude, longitude)
        (
            SELECT DISTINCT 
                artist_id,
                artist_name,
                artist_location,
                artist_latitude,
                artist_longitude
            FROM 
                staging_songs
            WHERE 
                artist_id IS NOT NULL
        );
""")

time_table_insert = ("""
    INSERT INTO 
        time (start_time, hour, day, week, month, year, weekday)
        (
            SELECT DISTINCT 
                TIMESTAMP 'epoch' + (ts / 1000)  * INTERVAL '1 second' AS start_time,
                EXTRACT(hour FROM start_time),
                EXTRACT(day FROM start_time),
                EXTRACT(week FROM start_time),
                EXTRACT(month FROM start_time),
                EXTRACT(year FROM start_time),
                EXTRACT(dayofweek FROM start_time)
            FROM 
                staging_events
            WHERE 
                ts IS NOT NULL
                AND page = 'NextSong'
        );
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
