from datetime import datetime
import hashlib
import os
from sqlite3 import connect
import subprocess
import sys

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

    update_file_metadata(conn, filename, hash, 0)
    
    #prepares dataframe
    hoot_df = fix_datatypes(read_logfile(filepath))
    
    #grabs hoot datamap
    hoot_map_df = pd.read_csv('datamaps/hoot_map.csv')
    
    #merges the dataframe and datamap
    hoot_df = hoot_df.merge(right=hoot_map_df, how='left', on='entry')
    
    #adds additional filename key
    hoot_df['filename'] = filename
    
    update_file_metadata(conn, filename, hash, 1)
    
    write_dataframe(hoot_df, 'device_data_raw', conn)
    
    close_db(conn)