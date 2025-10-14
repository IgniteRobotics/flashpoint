from datetime import datetime
import hashlib
import os
from sqlite3 import connect
import subprocess
import sys

import numpy as np
import pandas as pd

import csv_converter

def calculate_file_hash(filepath):
    """Calculate SHA-256 hash of a file"""
    sha256_hash = hashlib.sha256()
    with open(filepath, "rb") as f:
        for byte_block in iter(lambda: f.read(4096), b""):
            sha256_hash.update(byte_block)
    return sha256_hash.hexdigest()

#sets up database
#this creates the table and indexes
#however, it does not yet fill them with actual values
def setup_db(db_name):
    
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

#fills the metadata table
def update_file_metadata(connection, filename, hash, success):
    """Update or insert file metadata"""
    cursor = connection.cursor()
    current_time = int(datetime.now().timestamp())
    
    try:
        cursor.execute('''
        INSERT OR REPLACE INTO file_metadata 
        (filename, import_timestamp, file_hash, success, last_attempt_timestamp)
        VALUES (?, ?, ?, ?, ?)
        ''', (filename, current_time, hash, success, current_time))
         
        connection.commit()
    except Exception as e:
        print(f"Error updating metadata for {filename}: {e}")
        connection.rollback()

#converts logfile into a .csv file and reads a dataframe from it
def read_logfile(filepath):
    pos = filepath.rfind(".")
    output_csv = "./converted_data/hoot_converted_logs/" + filepath[:pos].split("/")[-1] + ".gz"
    try:
        #attempts to open the .gz file if it exists
        open(output_csv)
        print(".gz file already exists...")
    except FileNotFoundError:
        print(f'Converting {filepath}')
        #convert hoot file to wpilog
        output_wpilog = "./converted_data/hoot_converted_logs/" + filepath[:pos].split("/")[-1] + ".wpilog"
        subprocess.run(["./executables/owlet.exe", "-f", "wpilog", filepath, output_wpilog])
        #convert wpilog file to csv file
        csv_converter.csv_convert(output_wpilog, "./converted_data/hoot_converted_logs/")
        #remove wpilog intermediate
        os.remove(output_wpilog)
    #read csv into dataframe
    print(f'Reading into dataframe')
    colnames=['entry', 'data_type', 'value', 'timestamp']
    df = pd.read_csv(output_csv, quotechar='|', header=None, names=colnames)
    return (df)

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

#fix datatypes
#things have been a string up until now
def fix_datatypes(df):
    df_boolean_log_data = df[df['data_type'] == 'boolean']
    df_boolean_log_data['boolean_value'] = df_boolean_log_data['value']
    df_numerical_log_data = df[(df['data_type'] == 'int64') | (df['data_type'] == 'double') | (df['data_type'] == 'float')]
    df_numerical_log_data['numeric_value'] = pd.to_numeric(df_numerical_log_data['value'])
    df_other_log_data = df[(df['data_type'] != 'boolean') & (df['data_type'] != 'int64') & (df['data_type'] != 'double') & (df['data_type'] != 'float')]

    df_log_data = pd.concat([df_boolean_log_data, df_numerical_log_data, df_other_log_data])
    df_log_data['timestamp'] = df_log_data['timestamp'].astype('int64')

    return df_log_data

#writes data frame to table via connection
def write_dataframe(df, tablename, connection, filename = None):

    df.to_sql(tablename, connection, if_exists='append', index=False)
    connection.commit()

    if filename is not None:
        df.to_csv(filename, index=False)
        
# safely closes connection
def close_db(connection):
    connection.commit()
    connection.close()
    
def hoot_timestamp_convert(df):
    
    #now calcuate match_time.
    print('adding match time')
    df['match_time'] = df.timestamp - df['timestamp'].min()

    return df
    
def read_device_data_raw(df):
     #creates new telemetry dataframe
    intermediate_df = df[df['metric'].notnull()]
    
    #drops irrelevant columns
    intermediate_df.drop(columns = ['entry', 'boolean_value', 'value', 'timestamp', 'data_type'], inplace = True)
    
    has_voltage_data = True
    has_current_data = True
    has_velocity_data = True
    has_position_data = True
    has_temp_data = True
    
    try:
        voltage_df = intermediate_df.loc[(intermediate_df['metric'] == 'VOLTAGE')]
    except KeyError:
        has_voltage_data = False
    
    try:
        current_df = intermediate_df.loc[(intermediate_df['metric'] == 'CURRENT')]
    except KeyError:
        has_current_data = False  
    
    try:
        velocity_df = intermediate_df.loc[(intermediate_df['metric'] == 'VELOCITY')]
    except KeyError:
        has_velocity_data = False      
        
    try:
        position_df = intermediate_df.loc[(intermediate_df['metric'] == 'POSITION')]
    except KeyError:
        has_position_data = False  
    
    try:
        temp_df = intermediate_df.loc[(intermediate_df['metric'] == 'TEMP')]
    except KeyError:
        has_temp_data = False
        
    
    if not has_voltage_data and not has_current_data and not has_velocity_data and not has_position_data and not has_temp_data:
        return(None, None)
        
    #removes data that is not necessary for this dataframe
    #telemetry_df = telemetry_df.loc[(telemetry_df['metric'] == 'VOLTAGE') 
    #                               |(telemetry_df['metric'] == 'CURRENT')
    #                               |(telemetry_df['metric'] == 'VELOCITY')
    #                               |(telemetry_df['metric'] == 'POSITION')
    #                               |(telemetry_df['metric'] == 'TEMP')]
    
    intermediate_df.drop(columns = ['metric', 'numeric_value'], inplace = True)
    intermediate_df.drop_duplicates(inplace = True)
    telemetry_df = intermediate_df.copy(True)
    
    intermediate_df.drop(columns = 'match_time', inplace = True)
    intermediate_df.drop_duplicates(inplace = True)
    stats_df = intermediate_df
   
    if has_voltage_data:
        voltage_df.drop(columns = 'metric', inplace = True)
        voltage_df.rename(columns = {'numeric_value':'voltage'}, inplace = True)
        telemetry_df = pd.merge(telemetry_df, voltage_df, on = ['match_time', 'subsystem', 'assembly', 'subassembly', 'component'], how = 'outer')
        stats_df = pd.merge(stats_df, voltage_df.drop(columns = 'match_time')
                            .groupby(['subsystem', 'assembly', 'subassembly', 'component'], dropna = False, as_index = False).agg(
                                avg_voltage = ('voltage', 'mean'),
                                min_voltage = ('voltage', 'min'),
                                max_voltage = ('voltage', 'max'),
                                stddev_voltage = ('voltage', 'std')
                            ), on = ['subsystem', 'assembly', 'subassembly', 'component'], how = 'outer')
    else:
        telemetry_df['voltage'] = np.nan
        stats_df['avg_voltage'] = np.nan
        stats_df['min_voltage'] = np.nan
        stats_df['max_voltage'] = np.nan
        stats_df['stddev_voltage'] = np.nan
    
    if has_current_data:
        current_df.drop(columns = 'metric', inplace = True)
        current_df.rename(columns = {'numeric_value':'current'}, inplace = True)
        telemetry_df = pd.merge(telemetry_df, current_df, on = ['match_time', 'subsystem', 'assembly', 'subassembly', 'component'], how = 'outer')
        stats_df = pd.merge(stats_df, current_df.drop(columns = 'match_time')
                            .groupby(['subsystem', 'assembly', 'subassembly', 'component'], dropna = False, as_index = False).agg(
                                avg_current = ('current', 'mean'),
                                min_current = ('current', 'min'),
                                max_current = ('current', 'max'),
                                stddev_current = ('current', 'std')
                            ), on = ['subsystem', 'assembly', 'subassembly', 'component'], how = 'outer')
    else:
        telemetry_df['current'] = np.nan
        stats_df['avg_current'] = np.nan
        stats_df['min_current'] = np.nan
        stats_df['max_current'] = np.nan
        stats_df['stddev_current'] = np.nan
        
    if has_velocity_data:
        velocity_df.drop(columns = 'metric', inplace = True)
        velocity_df.rename(columns = {'numeric_value':'velocity'}, inplace = True)
        telemetry_df = pd.merge(telemetry_df, velocity_df, on = ['match_time', 'subsystem', 'assembly', 'subassembly', 'component'], how = 'outer')
        stats_df = pd.merge(stats_df, velocity_df.drop(columns = 'match_time')
                            .groupby(['subsystem', 'assembly', 'subassembly', 'component'], dropna = False, as_index = False).agg(
                                avg_velocity = ('velocity', lambda x : x.abs().mean()),
                                min_velocity = ('velocity', 'min'),
                                max_velocity = ('velocity', 'max'),
                                stddev_velocity = ('velocity', lambda x: x.abs().std())
                            ), on = ['subsystem', 'assembly', 'subassembly', 'component'], how = 'outer')
    else:
        telemetry_df['velocity'] = np.nan
        stats_df['avg_velocity'] = np.nan
        stats_df['min_velocity'] = np.nan
        stats_df['max_velocity'] = np.nan
        stats_df['stddev_velocity'] = np.nan
    
    if has_position_data:
        position_df.drop(columns = 'metric', inplace = True)
        position_df.rename(columns = {'numeric_value':'position'}, inplace = True)
        telemetry_df = pd.merge(telemetry_df, position_df, on = ['match_time', 'subsystem', 'assembly', 'subassembly', 'component'], how = 'outer')
        stats_df = pd.merge(stats_df, position_df.drop(columns = 'match_time')
                            .groupby(['subsystem', 'assembly', 'subassembly', 'component'], dropna = False, as_index = False).agg(
                                avg_position = ('position', 'mean'),
                                min_position = ('position', 'min'),
                                max_position = ('position', 'max'),
                                stddev_position = ('position', 'std')
                            ), on = ['subsystem', 'assembly', 'subassembly', 'component'], how = 'outer')
    else:
        telemetry_df['position'] = np.nan
        stats_df['avg_position'] = np.nan
        stats_df['min_position'] = np.nan
        stats_df['max_position'] = np.nan
        stats_df['stddev_position'] = np.nan
    
    if has_temp_data:
        temp_df.drop(columns = 'metric', inplace = True)
        temp_df.rename(columns = {'numeric_value':'temperature'}, inplace = True)
        telemetry_df = pd.merge(telemetry_df, temp_df, on = ['match_time', 'subsystem', 'assembly', 'subassembly', 'component'], how = 'outer')
        stats_df = pd.merge(stats_df, temp_df.drop(columns = 'match_time')
                            .groupby(['subsystem', 'assembly', 'subassembly', 'component'], dropna = False, as_index = False).agg(
                                avg_temperature = ('temperature', 'mean'),
                                min_temperature = ('temperature', 'min'),
                                max_temperature = ('temperature', 'max'),
                                stddev_temperature = ('temperature', 'std')
                            ), on = ['subsystem', 'assembly', 'subassembly', 'component'], how = 'outer')
    
    else:
        telemetry_df['temperature'] = np.nan
        stats_df['avg_temperature'] = np.nan
        stats_df['min_temperature'] = np.nan
        stats_df['max_temperature'] = np.nan
        stats_df['stddev_temperature'] = np.nan
            
    return(telemetry_df, stats_df)

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
    
    conn = setup_db("db/hoot.db")
    
    (is_duplicate, existing_filename) = is_file_already_imported(conn, hash)

    if is_duplicate:
        print(f"File {filename} has already been imported as {existing_filename}. Skipping.")
        sys.exit(0)
    
    read_logfile(filepath)
        
    update_file_metadata(conn, filename, hash, 0)
    
    
    #prepares dataframe
    print("Reading logfile")
    hoot_df = read_logfile(filepath)
    print("Fixing datatypes")
    hoot_df = fix_datatypes(hoot_df)
    
    hoot_df = hoot_timestamp_convert(hoot_df)
    
    #grabs hoot datamap 
    if "rio" in filename:
        hoot_map_df = pd.read_csv('datamaps/hoot_rio_map.csv')
    else:
        hoot_map_df = pd.read_csv('datamaps/hoot_drivetrain_map.csv')
    
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

    