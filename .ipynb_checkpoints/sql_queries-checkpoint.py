import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')
ARN=config.get('IAM_ROLE', 'ARN')
LOG_JSONPATH=config.get('S3', 'LOG_JSONPATH')

# DROP TABLES

staging_events_table_drop = "drop table if exists staging_events"
staging_songs_table_drop = "drop table if exists staging_songs"
songplay_table_drop = "drop table if exists songplay"
user_table_drop = "drop table if exists users"
song_table_drop = "drop table if exists songs"
artist_table_drop = "drop table if exists artists"
time_table_drop = "drop table if exists time"

# CREATE TABLES

#Staging Tables

staging_events_table_create= ("""
    CREATE TABLE staging_events(
        event_id      BIGINT identity(0, 1),
        artist        VARCHAR,
        auth          VARCHAR,
        firstName     VARCHAR,
        gender        VARCHAR,
        itemInSession BIGINT,
        lastName      VARCHAR,
        lenght        FLOAT,
        level         VARCHAR,
        location      VARCHAR,
        method        VARCHAR,
        page          VARCHAR,
        registration  BIGINT,
        sessionId     BIGINT,
        song          VARCHAR,
        status        BIGINT,
        ts            BIGINT,
        userAgent     VARCHAR,
        userId        BIGINT        
        )       
""")

staging_songs_table_create = ("""
    CREATE TABLE staging_songs(
        num_songs        INT, 
        artist_id        VARCHAR,
        artist_latitude  VARCHAR, 
        artist_longitude VARCHAR, 
        artist_location  VARCHAR, 
        artist_name      VARCHAR, 
        song_id          VARCHAR, 
        title            VARCHAR,
        duration         FLOAT, 
        year             INT
    )
""")


#Analytics table

songplay_table_create = ("""
    CREATE TABLE songplay(
        songplay_id     BIGINT IDENTITY (0,1) NOT NULL PRIMARY KEY,
        start_time      TIMESTAMP NOT NULL,
        user_id         INT NOT NULL SORTKEY DISTKEY,
        level           VARCHAR,
        song_id         VARCHAR NOT NULL,
        artist_id       VARCHAR NOT NULL,
        sessionId       INT,
        location        VARCHAR,
        userAgent       VARCHAR        
    );
""")

user_table_create = ("""
    CREATE TABLE users(
        user_id      INT NOT NULL PRIMARY KEY,
        first_name   VARCHAR, 
        last_name    VARCHAR, 
        gender       VARCHAR, 
        level        VARCHAR
    )diststyle all;
""")

song_table_create = ("""
    CREATE TABLE songs(
        song_id      VARCHAR NOT NULL PRIMARY,
        title        VARCHAR, 
        artist_id    VARCHAR, 
        year         INT, 
        duration     FLOAT
    );
""")

artist_table_create = ("""
    CREATE TABLE artists(
        artist_id    VARCHAR NOT NULL PRIMARY KEY, 
        name         VARCHAR, 
        location     VARCHAR, 
        latitude     VARCHAR, 
        longitude    VARCHAR
    )diststyle all;
""")

time_table_create = ("""
    CREATE TABLE time(
        start_time  TIMESTAMP NOT NULL PRIMARY KEY, 
        hour        INT, 
        day         INT, 
        week        INT, 
        month       INT, 
        year        INT, 
        weekday     INT
    )diststyle all;
""")

# STAGING TABLES

staging_events_copy = ("""
copy {} from 's3://udacity-dend/{}' 
credentials 'aws_iam_role={}'
format as json {}
region 'us-west-2';
""").format('staging_events', 'log_data', ARN, LOG_JSONPATH)

staging_songs_copy = ("""
copy {} from 's3://udacity-dend/{}' 
credentials 'aws_iam_role={}'
format as json 'auto'
region 'us-west-2';
""").format('staging_songs', 'song_data', ARN)

# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO songplay(
                                        start_time, 
                                        user_id, 
                                        level, 
                                        song_id, 
                                        artist_id, 
                                        sessionId, 
                                        location, 
                                        userAgent)
    SELECT DISTINCT TIMESTAMP 'epoch' + se.ts/1000 \
               * INTERVAL '1 second'    AS start_time,
                   se.userId            AS user_id,
                   se.level             AS level,
                   ss.song_id           AS song_id,
                   ss.artist_id         AS artist_id,
                   se.sessionId         AS sessionId,
                   se.location          AS location,
                   se.userAgent         AS userAgent
    FROM staging_events AS se
    JOIN staging_songs AS ss
        ON (se.artist = ss.artist_name)
    WHERE se.page = 'NextSong';
        
""")

user_table_insert = ("""
    INSERT INTO users (
                                        user_id,
                                        first_name,
                                        last_name,
                                        gender,
                                        level)
    SELECT DISTINCT se.userId           AS user_id,
            se.firstName                AS first_name,
            se.lastName                 AS last_name,
            se.gender                   AS gender,
            se.level                    AS level
    FROM staging_events AS se
    WHERE se.page = 'NextSong';
""")

song_table_insert = ("""
    INSERT INTO songs (                 song_id,
                                        title,
                                        artist_id,
                                        year,
                                        duration)
    SELECT  DISTINCT ss.song_id         AS song_id,
            ss.title                    AS title,
            ss.artist_id                AS artist_id,
            ss.year                     AS year,
            ss.duration                 AS duration
    FROM staging_songs AS ss;
""")

artist_table_insert = ("""
    INSERT INTO artists (               artist_id,
                                        name,
                                        location,
                                        latitude,
                                        longitude)
    SELECT  DISTINCT ss.artist_id       AS artist_id,
            ss.artist_name              AS name,
            ss.artist_location          AS location,
            ss.artist_latitude          AS latitude,
            ss.artist_longitude         AS longitude
    FROM staging_songs AS ss;
""")

time_table_insert = ("""
    INSERT INTO time (                  start_time,
                                        hour,
                                        day,
                                        week,
                                        month,
                                        year,
                                        weekday)
    SELECT  DISTINCT TIMESTAMP 'epoch' + se.ts/1000 \
                * INTERVAL '1 second'        AS start_time,
            EXTRACT(hour FROM start_time)    AS hour,
            EXTRACT(day FROM start_time)     AS day,
            EXTRACT(week FROM start_time)    AS week,
            EXTRACT(month FROM start_time)   AS month,
            EXTRACT(year FROM start_time)    AS year,
            EXTRACT(week FROM start_time)    AS weekday
    FROM    staging_events AS se
    WHERE se.page = 'NextSong';
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]

drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]

copy_table_queries = [staging_events_copy, staging_songs_copy]

insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
