import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    """
    This function processes a song file.

    The song file is provided via the filepath input.

    This function extracts the song information and then inserts said information into the song table.
    The function then does the same thing for artist information.

    Inputs:
        cur = cursor variable
        filepath = song filepath
    """
    # open song file
    # read the file's contents through the pandas read_json method which returns a dataframe a dataframe
    df = pd.read_json(path_or_buf=filepath, orient='records', lines=True)

    # insert song record
    # Gather the first set of values from the 2d matrix returned from a dataframe then create a list out of those values
    song_data = df[['song_id', 'artist_id', 'title', 'year', 'duration']].values[0].tolist()

    # execute insert
    cur.execute(song_table_insert, song_data)

    # insert artist record
    # Gather the first set of values from the 2d matrix returned from a dataframe then create a list out of those values
    artist_data = df[
        ['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']
    ].values[0].tolist()

    # execute insert
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    """
    This function processes a log file filled with song play history.

    The log file is provided via the filepath input.

    This function extracts the song play information in the log file.
    Afterwards time dimension is created and updated with accurate extracted time information.
    Then a select query is used to parse the song and artist information from their respective tables.
    Then the song play information is inserted into the songplay table.

    Inputs:
        cur = cursor variable
        filepath = log filepath
    """
    # open log file
    # read the file's contents through the pandas read_json method which returns a dataframe a dataframe
    df = pd.read_json(path_or_buf=filepath, orient='records', lines=True)

    # filter by NextSong action
    # only keep the records that have a NextSong value in the page column
    df = df.loc[df['page'].isin(['NextSong'])]

    # convert timestamp column to datetime using pd.to_datetime
    t = df
    t['ts'] = pd.to_datetime(t['ts'])

    # insert time data records
    # initialize a dictionary to store the extracted date values
    date_dim = {}

    # take each datetime category from the original timestamp value
    # and create a list out of all the different time-based categorical values
    for i in t.index.tolist():
        try:
            date_dim[i] = [t['ts'][i].isoformat(), t['ts'].dt.hour[i], t['ts'].dt.day[i], t['ts'].dt.week[i],
                           t['ts'].dt.month[i], t['ts'].dt.year[i], t['ts'].dt.weekday[i]]
        except:
            print("Error at time extraction index: " + i)

    time_data = date_dim
    column_labels = ['timestamp', 'hour', 'day', 'week', 'month', 'year', 'DayOfWeek']

    # initialize a time dataframe from the extracted data-based values
    # so that we can iter over the rows in the dataframe when inserting into the time table
    time_df = pd.DataFrame.from_dict(date_dim, orient='index', columns=column_labels)

    # iterate over rows and insert date-based data into the time table
    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    # initialize a user_df with a specific number of columns from the log_based extraction dataframe
    user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']]

    # drop duplicates so we have no issues inserting into the users table
    user_df = user_df.drop_duplicates()

    # insert user records
    # iter over rows in the user_df and insert into users table
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():

        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()

        if results:
            songid, artistid = results
            print(songid, artistid)
        else:
            songid, artistid = None, None

        # insert songplay record
        # Create a tuple of various values from the row taken from the log based df

        songplay_data = (row.ts, row.userId, row.level, songid, artistid,
                         row.sessionId, row.location, row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """
    This function processes through all song and log files.

    While going through each file this function will use the passed in functions to process the
    files and ingest the information into the specified tables: songplays, songs, artists, time and users

    Inputs:
        cur = cursor object
        conn = connection object
        filepath = directory of song or log files
        func = either song processing function or log processing function
    """
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root, '*.json'))
        for f in files:
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
    """
    This function connects to the sparkifydb then creates a cursor objects.

    Afterwards the function uses the process data function to cycle through all
    the log and songs files to insert the data into the different tables:
    songs, songplays, users, artists, and time.

    """
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()