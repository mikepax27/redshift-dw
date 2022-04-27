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
        auth            VARCHAR(20),
        firstName       VARCHAR(20),
        gender          VARCHAR(1),
        itemInSession   INTEGER,
        lastName        VARCHAR(20),
        length          NUMERIC(18,8),
        level           VARCHAR(20),
        location        VARCHAR,
        method          VARCHAR(6),
        page            VARCHAR(20),
        registration    BIGINT,
        sessionId       BIGINT,
        song            VARCHAR,
        status          INTEGER,
        ts              TIMESTAMP,
        userAgent       VARCHAR,
        userId          INTEGER
    );
""")

staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_songs(
        num_songs           INTEGER, 
        artist_id           VARCHAR(20)     NOT NULL,
        artist_latitude     NUMERIC(18,8),
        artist_longitude    NUMERIC(18,8),
        artist_location     VARCHAR,
        artist_name         VARCHAR         NOT NULL,
        song_id             VARCHAR(20)     NOT NULL,
        title               VARCHAR         NOT NULL,
        duration            NUMERIC(18,8)   NOT NULL,
        year                INTEGER    
    );
""")

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplays(
        songplay_id     VARCHAR(20)     PRIMARY KEY DISTKEY,
        start_time      TIMESTAMP                   SORTKEY,
        user_id         VARCHAR(20)     NOT NULL,
        level           VARCHAR(20),
        song_id         VARCHAR(20),
        artist_id       VARCHAR(20),
        session_id      VARCHAR(20),
        location        VARCHAR,
        user_agent      VARCHAR
    );
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users(
        user_id         VARCHAR(20)     PRIMARY KEY DISTKEY,
        first_name      VARCHAR(20)     NOT NULL,
        last_name       VARCHAR(20)     NOT NULL,
        gender          VARCHAR(1),
        level           VARCHAR(5)      NOT NULL
    );
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs(
        song_id         VARCHAR(20)     PRIMARY KEY DISTKEY,
        title           VARCHAR         NOT NULL,
        artist_id       VARCHAR(20)     NOT NULL,
        year            INTEGER,
        duration        NUMERIC(18,8)   NOT NULL
    );
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists(
        artist_id       VARCHAR(20)     PRIMARY KEY DISTKEY,
        name            VARCHAR         NOT NULL,
        location        VARCHAR,
        lattitude       NUMERIC(18,8),
        longitude       NUMERIC(18,8)
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
    CREDENTIALS 'aws_iam_role={config.get('IAM_ROLE', 'ARN')}'
    JSON {config.get('S3','LOG_JSONPATH')}
    region 'us-west-2'
    COMPUPDATE OFF;
""").format()

staging_songs_copy = (f"""
    COPY staging_songs
    FROM {config.get('S3','SONG_DATA')}
    CREDENTIALS 'aws_iam_role={config.get('IAM_ROLE', 'ARN')}'
    JSON 'auto'
    region 'us-west-2'
    COMPUPDATE OFF;
""").format()

# FINAL TABLES

songplay_table_insert = ("""
        INSERT INTO 
            songplay (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
        (
            SELECT DISTINCT 
                event.ts ,
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
                name,
                location,
                latitude,
                longitude
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
                ts,
                EXTRACT(hour FROM ts),
                EXTRACT(day FROM ts),
                EXTRACT(week FROM ts),
                EXTRACT(month FROM ts),
                EXTRACT(year FROM ts),
                EXTRACT(dayofweek FROM ts)
            FROM 
                staging_events
            WHERE 
                ts IS NOT NULL
        );
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
