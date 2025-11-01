import os
from sqlite3 import connect
import sys
import numpy as np
import pandas as pd
from ingest_library import *

#sets up database
#this creates the table and indexes
#however, it does not yet fill them with actual values
def setup_hoot_db(db_name):
    
    #creates a "connection"
    connection = connect(db_name)
    
    #creates a mean of running scripts on that connnection
    cursor = connection.cursor()


    #creates file metadata table
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS file_metadata (
        filename TEXT PRIMARY KEY,
        import_timestamp INTEGER,
        file_hash TEXT,
        success BOOLEAN,
        last_attempt_timestamp INTEGER
    )
    ''')
    connection.commit()
    
    #creates raw device data table
    cursor.execute('''CREATE TABLE IF NOT EXISTS device_data_raw (
        filename TEXT,
        entry TEXT, 
        data_type TEXT, 
        value TEXT, 
        timestamp REAL, 
        match_time REAL, 
        subsystem TEXT,
        assembly TEXT, 
        subassembly TEXT, 
        component TEXT, 
        metric TEXT, 
        boolean_value TEXT, 
        numeric_value REAL)''')
    connection.commit()
    
    #creates device telemetry table
    cursor.execute('''CREATE TABLE IF NOT EXISTS device_telemetry (
        match_time REAL,
        subsystem TEXT,
        assembly TEXT,
        subassembly TEXT,
        component TEXT,
        position REAL,
        velocity REAL,
        voltage REAL,
        current REAL,
        temperature REAL)''')
    connection.commit()
    
    #creates device_stats table
    cursor.execute('''CREATE TABLE IF NOT EXISTS device_stats (
        subsystem TEXT,
        assembly TEXT,
        subassembly TEXT,
        component TEXT,
        avg_velocity REAL,
        min_velocity REAL,
        max_velocity REAL,
        stddev_velocity REAL,
        avg_voltage REAL,  
        min_voltage REAL,
        max_voltage REAL,
        stddev_voltage REAL,
        avg_current REAL,
        min_current REAL,
        max_current REAL,
        stddev_current REAL,
        avg_temperature REAL,
        min_temperature REAL,
        max_temperature REAL,
        stddev_temperature REAL,
        avg_position REAL,
        min_position REAL,
        max_position REAL,
        stddev_position REAL)''')
    connection.commit()
    
    return connection

def is_file_already_imported(connection, filehash):
    """
    Check if a file has already been successfully imported by comparing its hash
    against existing entries in file_metadata table.
    
    Args:
        connection: SQLite database connection
        filehash: File hash
        
    Returns:
        tuple: (bool, str) - (is_duplicate, existing_filename)
        where is_duplicate indicates if file was previously imported
        and existing_filename contains the name of the duplicate file (if found)
    """
    try:
        cursor = connection.cursor()
        
        # Query for successful imports with matching hash
        cursor.execute('''
            SELECT filename 
            FROM file_metadata 
            WHERE file_hash = ? AND success = 1
        ''', (filehash,))
        
        result = cursor.fetchone()
        
        if result:
            return True, result[0]
        return False, None
        
    except Exception as e:
        print(f"Error checking file hash: {e}")
        return False, None

def hoot_timestamp_convert(df):
    
    #now calcuate match_time.
    print('adding match time')
    df['match_time'] = df.timestamp - df['timestamp'].min()

    return df

if __name__ == "__main__":
    print("Starting")
    
    if len(sys.argv) != 2:
        print(f"Usage: {sys.argv[0]} <file>", file=sys.stderr)
        sys.exit(1)
    
    filepath = sys.argv[1]
    
    #only use the filename for the key, but use the path to calculate the hash
    filename = os.path.basename(filepath)
    hash = calculate_file_hash(filepath)
    
    #open the db/connection
    #db/hoot.db
    
    conn = setup_hoot_db("db/hoot.db")
    
    (is_duplicate, existing_filename) = is_file_already_imported(conn, hash)

    if is_duplicate:
        print(f"File {filename} has already been imported as {existing_filename}. Skipping.")
        sys.exit(0)
        
    update_file_metadata(conn, filename, hash, 0)
    
    #prepares dataframe
    print("Reading logfile")
    hoot_df = read_device_logfile(filepath)
    print("Fixing datatypes")
    hoot_df = fix_datatypes(hoot_df)
    
    hoot_df = hoot_timestamp_convert(hoot_df)
    
    #grabs hoot datamap 
    if "rio" in filename:
        hoot_map_df = pd.read_csv('datamaps/rio_devices_map.csv')
    else:
        hoot_map_df = pd.read_csv('datamaps/drivetrain_devices_map.csv')
    
    #merges the dataframe and datamap
    hoot_df = hoot_df.merge(right=hoot_map_df, how='left', on='entry')
    
    #creates telemetry and statistics dataframes
    (device_telemetry_df, device_stats_df) = read_device_data_raw(hoot_df)
    
    #adds additional filename key
    hoot_df['filename'] = filename
    write_dataframe(hoot_df, 'device_data_raw', conn)
    
    if device_telemetry_df is not None:
        write_dataframe(device_telemetry_df, 'device_telemetry', conn)
        
    if device_stats_df is not None:
        write_dataframe(device_stats_df, 'device_stats', conn)
    
    update_file_metadata(conn, filename, hash, 1)

    close_db(conn)

    