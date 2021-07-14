# Sparkify Music Data Warehousing Design Project
## The purpose of this database is to implement a star schema like design with a songplays fact table and supporting dimensions being: songs, artists, time, and users.

## The goal of this database design is to have a analytical table, songplays, where users of this fact table are able to query against it to have an easy way of accessing the log files from the music streaming service.

If a user of this data wants to gather more information about a record in the fact table(for example, getting more information about who's songplay history with regards to whom the user is looking at in the fact table) then they can join with the other dimension tables(users to get the user information) in order to get more information about the songplay records.

My database schema design was to implement the not null constraint among all identifiers, such that, there are no primary keys without a value. However, there were a few null records within the logs for the songplay table, so I removed the clause for the fact table's song_id and artist_id.  

I have also implemented a uniqueness constraint for every primary key in the schema, besides the start_time column in the time dataset as that's unecessary.

Furthermore the ETL pipeline design is pretty straight forward for the songs data files.  For the songs data files, the etl pipeline will process each file individually and insert a new row into the songs table for every new song encountered; while, doing the same process for artists found in the song files.

On the other hand, with regards to the logs data the etl pipeline processed the files in a similar sequential order, but scanned the entire log file for users and time information.  After scanning the log file, time and user based information was extracted and inserted into the time/user dimensions.

Finally, the various information is gathered from the log_files and generated into a fact table called songplays.

## How to run python scripts
1) First, run create_tables.py to initialize table and database creation, while dropping any tables that were created previously.
2) Second, run etl.py to ingest data into the various tables: songplays, songs, artists, users, and time

Example Query to get a users listening history:

SELECT users.user_id, users.first_name, users.last_name, users.level, count(songplay_id) 
FROM users 
JOIN songplays 
    ON songplays.user_id = users.user_id 
WHERE users.user_id IN ('91', '73') 
GROUP BY 1, 2, 3, 4 ;
