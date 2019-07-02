"""Load data sparkifydb database Fact and Dimentions tables
This script is used to process and load data into sparkifydb Database 
tables. 

This file can also be imported as a module and contains the following
functions:

    * process_song_file - Processing Song file and load data 
    * process_log_file - Processing Song file and load data
    * process_data - IIterating all files with in a directory
    * main - the main function of the script
"""

# Importing system libraries
import os
import glob
import psycopg2
import pandas as pd

# Importing user defined libraries
from sql_queries import *


def process_song_file(cur, filepath):
    """Processing song data file and loading data into Dimensions. 
    
    Parameters
    ___________
        cur : psycopg2 cursor object
            Cursor object for sparkifydb
         filepath : string
             Path of the file to be loaded
  
    Returns
    ___________
        None
    """
    
    # open song file
    df = pd.read_json(filepath, lines=True)

    # insert song record
    song_data = list(df[["song_id","title","artist_id","year","duration"]].values[0])
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    artist_data =   list(df[['artist_id', 'artist_name', 'artist_location',\
                             'artist_latitude','artist_longitude']].values[0])
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    """Processing log data file and loading data into Dimensions and Facts. 
    
    Parameters
    ___________
        cur : psycopg2 cursor object
            Cursor object for sparkifydb
        filepath : string
            Path of the file to be loaded
  
    Returns
    ___________
        None
    """
    
    # open log file
    df = pd.read_json(filepath, lines=True)
    
    # filter by NextSong action
    df = df[df['page']=="NextSong"]

    # convert timestamp column to datetime
    t = pd.to_datetime(df['ts'])
    
    # insert time data records
    time_data = [t,t.dt.hour,t.dt.day,t.dt.weekofyear,t.dt.month,t.dt.year,t.dt.dayofweek]
    column_labels = ("start_time", "hour", "day", "week", "month", "year","weekday")
    time_df = pd.DataFrame(dict(zip(column_labels, time_data)))
    
    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, row)
    
    # load user table
    user_df = df[['userId','firstName','lastName','gender','level']]

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = False, False
            
        # converting numeric value into timestamp 
        ts_value=pd.to_datetime(row.ts, unit='ms')
        
        # insert songplay record
        songplay_data = (ts_value, row.userId, row.level, songid, artistid,\
                         row.sessionId, row.location, row.userAgent)
        if songid:
            cur.execute(songplay_table_insert, songplay_data)
        

def process_data(cur, conn, filepath, func):
    """Processing log data file and loading data into Dimensions and Facts. 
    
    Parameters
    ___________
        cur : psycopg2 cursor object
            Cursor object for sparkifydb
        conn : psycopg2 connection object
            Connection object for sparkifydb
        filepath : string
             Path of the file to be loaded
        func : string
             Name of Function to be called for processing data   
        
    Returns
    ___________
        None
    """
    
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    
    # creating sparkify DB connection
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    # Processing Song file data and inserting to Dimensions (Song & Artist)
    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    
    # Processing Log file data and inserting to Dimensions (Time & User) and Fact (Songplay)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)
    
    # closing the connection
    conn.close()


if __name__ == "__main__":
    main()