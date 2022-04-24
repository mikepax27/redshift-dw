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
        artist          VARCHAR(50),
        auth            VARCHAR(20),
        firstName       VARCHAR(20),
        gender          VARCHAR(1),
        itemInSession   INTEGER,
        lastName        VARCHAR(20),
        length          NUMERIC(18,8),
        level           VARCHAR(20),
        location        VARCHAR(50),
        method          VARCHAR(6),
        page            VARCHAR(20),
        registration    BIGINT,
        sessionId       BIGINT,
        song            VARCHAR(50),
        status          INTEGER,
        ts              TIMESTAMP,
        userAgent       VARCHAR(50),
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
        artist_name         VARCHAR(50)     NOT NULL,
        song_id             VARCHAR(20)     NOT NULL,
        title               VARCHAR(50)     NOT NULL,
        duration            NUMERIC(18,8)   NOT NULL,
        year                INTEGER    
    );
""")

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplays(
        songplay_id     VARCHAR(20)     NOT NULL,
        start_time      TIMESTAMP,
        user_id         VARCHAR(20)     NOT NULL,
        level           VARCHAR(20),
        song_id         VARCHAR(20)     NOT NULL,
        artist_id       VARCHAR(20)     NOT NULL,
        session_id      VARCHAR(20)     NOT NULL,
        location        VARCHAR(50),
        user_agent      VARCHAR(50)
    );
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users(
        user_id         VARCHAR(20)     NOT NULL,
        first_name      VARCHAR(20)     NOT NULL,
        last_name       VARCHAR(20)     NOT NULL,
        gender          VARCHAR(1),
        level           VARCHAR(5)      NOT NULL
    );
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs(
        song_id         VARCHAR(20)     NOT NULL,
        title           VARCHAR(50)     NOT NULL,
        artist_id       VARCHAR(20)     NOT NULL,
        year            INTEGER,
        duration        NUMERIC(18,8)   NOT NULL
    );
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists(
        artist_id       VARCHAR(20)     NOT NULL,
        name            VARCHAR(50)     NOT NULL,
        location        VARCHAR(50),
        lattitude       NUMERIC(18,8),
        longitude       NUMERIC(18,8)
    );
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time(
        start_time  TIMESTAMP   NOT NULL,
        hour        INTEGER     NOT NULL,
        day         INTEGER     NOT NULL,
        week        INTEGER     NOT NULL,
        month       INTEGER     NOT NULL,
        year        INTEGER     NOT NULL,
        weekday     INTEGER     NOT NULL
    );
""")

# STAGING TABLES

staging_events_copy = ("""
""").format()

staging_songs_copy = ("""
""").format()

# FINAL TABLES

songplay_table_insert = ("""
""")

user_table_insert = ("""
""")

song_table_insert = ("""
""")

artist_table_insert = ("""
""")

time_table_insert = ("""
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
