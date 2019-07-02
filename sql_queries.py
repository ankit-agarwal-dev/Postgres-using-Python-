""" Database SQL statement (Postgres Compatible) 
This script is to define quesries used to create/drop/query database.
"""
    
# DROP FACT AND DIMENSION TABLES QUERIES

songplay_table_drop = "drop table if exists songplays;"
user_table_drop = "drop table if exists users;"
song_table_drop = "drop table if exists songs;"
artist_table_drop = "drop table if exists artists;"
time_table_drop = "drop table if exists time;"

# DROP TEMPORARY TABLES QUERIES

time_tmp_table_drop = "drop table if exists time_tmp;"
users_tmp_table_drop = "drop table if exists users_tmp;"

# CREATE FACT AND DIMENSION TABLES QUERIES

songplay_table_create = ("""create table if not exists songplays(songplay_id serial primary key,start_time timestamp not null,user_id int not null,level varchar(100) not null, song_id varchar(255) not null, artist_id varchar(255) not null, session_id smallint not null,location varchar(255) not null, user_agent varchar(255) not null);""")
 
user_table_create = ("""create table if not exists users (user_id int primary key, first_name varchar(255) not null, last_name varchar(255), gender char(1) not null, level varchar(100) not null);""")

song_table_create = ("""create table if not exists songs(song_id varchar(255) primary key, title varchar(255)  not null, artist_id varchar(255) not null, year int not null, duration float not null);""")

artist_table_create = ("""create table if not exists artists(artist_id varchar(255) primary key, name varchar(255) not null, location varchar(255) not null, latitude float, longitude float);""")

time_table_create = ("""create table if not exists time(start_time timestamp primary key, hour smallint not null, day smallint not null, week smallint not null, month smallint not null, year smallint not null, weekday smallint not null);""")

# CREATE TEMPORARY TABLES QUERIES

time_tmp_table_create = ("""create table if not exists time_tmp as select * from time where 1=0;""")
users_tmp_table_create = ("""create table if not exists users_tmp as select * from users where 1=0;""")

# INSERT BULK RECORD INTO TEMPORARY TABLE QUERIES

time_tmp_table_bulk_insert = ("""copy time_tmp from '/home/workspace/time_tmp.csv' DELIMITER '~';""")
users_tmp_table_bulk_insert = ("""copy users_tmp from '/home/workspace/users_tmp.csv' DELIMITER '~';""")

# INSERT FROM TEMPORARY TABLE QUERIES

time_table_insert_tmp = ("""INSERT INTO time SELECT * FROM time_tmp ON CONFLICT DO NOTHING;""") 
users_table_insert_tmp = ("""INSERT INTO users SELECT DISTINCT ON (user_id) * FROM users_tmp ON CONFLICT (user_id) do update set level = EXCLUDED.level;""") 

# INSERT RECORDS QUERIES

songplay_table_insert = ("""insert into songplays(start_time,user_id,level, song_id, artist_id, session_id,location, user_agent) 
values (%s,%s,%s,%s,%s,%s,%s,%s);""")

user_table_insert = ("""insert into users(user_id, first_name, last_name, gender,level) 
values(%s,%s,%s,%s,%s) on conflict (user_id) do update set level = EXCLUDED.level;""")

song_table_insert = ("""insert into songs(song_id, title, artist_id,year,duration) 
values(%s,%s,%s,%s,%s) on conflict (song_id) do nothing;""")

artist_table_insert = ("""insert into artists(artist_id, name, location, latitude, longitude) values(%s,%s,%s,%s,%s)
on conflict (artist_id) do update 
set 
location = excluded.location,
latitude = excluded.latitude,
longitude = excluded.longitude;""")

time_table_insert = ("""insert into time(start_time, hour, day, week, month, year, weekday) values(%s,%s,%s,%s,%s,%s,%s)
on conflict (start_time) do nothing;""")

time_table_insert_bulk = ("""copy time from '/home/workspace/temp_time.csv (format text) DELIMITER '~'""")

# FIND SONGS QUERY

song_select = ("""select song_id,artists.artist_id from songs join artists on artists.artist_id = songs.artist_id where title =%s and name=%s and duration=%s;""")

# QUERY LISTS

create_tmp_table_queries = [time_tmp_table_create, users_tmp_table_create]
drop_tmp_table_queries = [time_tmp_table_drop, users_tmp_table_drop]
create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
