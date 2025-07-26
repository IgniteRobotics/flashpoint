import os
import pandas as pd
import sys
from sqlite3 import connect 
import csv_converter 
import json
import numpy as np
import hashlib
from datetime import datetime

#calculates file hash from a file path 
#so that the database has a means of connection to the log file
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


    #creates file metadata table and indices
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

    #creates log metadata table and indices
    cursor.execute('''CREATE TABLE IF NOT EXISTS log_metadata (
        filename TEXT PRIMARY KEY, 
        build_date TEXT, 
        commit_hash TEXT, 
        git_date TEXT, 
        git_branch TEXT, 
        project_name TEXT, 
        git_dirty TEXT, 
        event TEXT, 
        event_year TEXT,
        match_id TEXT, 
        replay_num TEXT, 
        match_type TEXT, 
        is_red_alliance TEXT, 
        station_num TEXT)''')
    connection.commit()
    
    #creates raw device data table and indices
    cursor.execute('''CREATE TABLE IF NOT EXISTS device_data_raw (
        filename TEXT PRIMARY KEY,
        event_year TEXT, 
        event TEXT, 
        match_id REAL, 
        replay_num REAL, 
        entry TEXT, 
        data_type TEXT, 
        value TEXT, 
        timestamp REAL, 
        match_time REAL, 
        subsystem TEXT,
        assembly TEXT, 
        subassembly TEXT, 
        component TEXT, 
        metric TEXT , 
        boolean_value TEXT, 
        numeric_value REAL)''')
    connection.commit()
    
    #cursor.execute('CREATE TABLE IF NOT EXISTS metrics (entry TEXT, data_type TEXT, value TEXT, timestamp REAL, match_time REAL, subsystem TEXT, component TEXT, part TEXT, type TEXT, metric TEXT , boolean_value TEXT, numeric_value REAL, filename TEXT, event TEXT, match_id REAL, replay_num REAL)')



    #cursor.execute('CREATE INDEX IF NOT EXISTS metrics_idx_event on metrics (event)')
    #cursor.execute('CREATE INDEX IF NOT EXISTS metrics_idx_filename on metrics (filename)')
    #cursor.execute('CREATE INDEX IF NOT EXISTS metrics_idx_event_match on metrics (event, match_id)')
    #cursor.execute('CREATE INDEX IF NOT EXISTS metrics_idx_component on metrics (component)')
    #cursor.execute('CREATE INDEX IF NOT EXISTS metrics_idx_component on metrics (component, metric)')
    #connection.commit()

    # cursor.execute('CREATE TABLE IF NOT EXISTS metrics_summary (component TEXT, metric TEXT ,minimum REAL ,maximun REAL ,average REAL ,filename TEXT, event TEXT, match_id REAL, replay_num REAL)')

    # cursor.execute('CREATE INDEX IF NOT EXISTS metrics_summary_idx_event on metrics_summary (event)')
    # cursor.execute('CREATE INDEX IF NOT EXISTS metrics_summary_idx_filename on metrics_summary (filename)')
    # cursor.execute('CREATE INDEX IF NOT EXISTS metrics_summary_idx_event_match on metrics_summary (event, match_id)')
    # cursor.execute('CREATE INDEX IF NOT EXISTS metrics_summary_idx_component on metrics_summary (component)')
    # cursor.execute('CREATE INDEX IF NOT EXISTS metrics_summary_idx_component_metric on metrics_summary (component, metric)')
    # connection.commit()
    
    #cursor.execute('CREATE TABLE IF NOT EXISTS preferences ( entry TEXT, data_type TEXT , value TEXT, timestamp REAL , boolean_value TEXT, numeric_value REAL, filename TEXT, event TEXT, match_id REAL, replay_num REAL)')
    #cursor.execute('CREATE INDEX IF NOT EXISTS preference_idx_filename on preferences (filename)')
    #connection.commit()

    #cursor.execute('CREATE TABLE IF NOT EXISTS vision ( entry TEXT, data_type TEXT , value TEXT, timestamp REAL , boolean_value TEXT, numeric_value REAL, filename TEXT, event TEXT, match_id REAL, replay_num REAL)')
    #cursor.execute('CREATE INDEX IF NOT EXISTS vision_idx_filename on vision (filename)')
    #connection.commit()

    return connection

def is_file_already_imported(connection, filepath):
    """
    Check if a file has already been successfully imported by comparing its hash
    against existing entries in file_metadata table.
    
    Args:
        connection: SQLite database connection
        filepath: Path to the file to check
        
    Returns:
        tuple: (bool, str) - (is_duplicate, existing_filename)
        where is_duplicate indicates if file was previously imported
        and existing_filename contains the name of the duplicate file (if found)
    """
    try:
        cursor = connection.cursor()
        file_hash = calculate_file_hash(filepath)
        
        # Query for successful imports with matching hash
        cursor.execute('''
            SELECT filename 
            FROM file_metadata 
            WHERE file_hash = ? AND success = 1
        ''', (file_hash,))
        
        result = cursor.fetchone()
        
        if result:
            return True, result[0]
        return False, None
        
    except Exception as e:
        print(f"Error checking file hash: {e}")
        return False, None

#fills the metadata table and indices
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

#clears data base for new imports
def flush_tables(connection, filename):
    cursor = connection.cursor()

    cursor.execute(f'SELECT * FROM log_metadata where filename = "{filename}"')

    results = cursor.fetchall()

    if len(results) > 0:
        #TODO add in summary?
        for t in ['log_metadata', 'metrics', 'preferences', 'vision']:
            cursor.execute(f'DELETE FROM {t} WHERE filename = "{filename}"')

        connection.commit()

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

#converts logfile into a .csv file and reads a dataframe from it
def read_logfile(filename):
    print(f'Converting {filename}')
    #convert wpilog to a csv file
    csv_converter.csv_convert(filename)
    input_file = sys.argv[1]
    pos = input_file.rfind(".")
    output_csv = input_file[:pos] + ".gz" 
    log_filename = os.path.basename(sys.argv[1])

    print(f'Reading into dataframe')
    #read csv into dataframe
    colnames=['entry', 'data_type', 'value', 'timestamp']
    df = pd.read_csv(output_csv, quotechar='|', header=None, names=colnames)

    return (log_filename, df)

#splits output dataframe from log file into dataframes to be utilized individually
#utlizies prefixes from homemade config
def split_dataframe(df, cfg):
    meta_df = df.loc[df['entry'].str.startswith(cfg['metadata_prefix'])]
    fms_df = df.loc[df['entry'].str.startswith(cfg['fms_prefix'])]
    metrics_df = df.loc[df['entry'].str.startswith(cfg['metrics_prefix'])]
    vision_df = df.loc[df['entry'].str.startswith(cfg['photon_prefix'], cfg['camerapub_prefix'])]
    preferences_df = df.loc[df['entry'].str.startswith(cfg['preferences_prefix'])]

    return (meta_df, fms_df, metrics_df, vision_df, preferences_df)

#creates a new metadata frame from log metadata and fms data
#also gets the year from the build date
def parse_metadata(meta_df, fms_df, filename):
    print('Parsing Metadata')
    #get metadata
    metadata = {}
    #event year
    year = ""
    #iterate and split records because it's logged funky
    for index, row in meta_df.iterrows():
        #split on ': '
        (key, value) = row['value'].split(': ')
        metadata[key] = value
        if key == 'Build Date':
            year = value.split("-")[0]
            
    
    metadata['filename'] = filename


    fms_items =['EventName','MatchNumber','ReplayNumber','MatchType','IsRedAlliance','StationNumber']
    #iterate and map values.  there are dupes in the data and we take the last.
    for index, row in fms_df.iterrows():
        key = row['entry'].split('/')[-1]
        if key in fms_items:
            metadata[key] = row['value']
            print(metadata[key])

    
    #dataframe from dict
    meta_df = pd.DataFrame.from_dict([metadata])
    #fix column names
    meta_df.rename(columns={
        'Project Name': 'project_name',
        'Build Date': 'build_date',
        'Commit Hash': 'commit_hash',
        'Git Date': 'git_date',
        'Git Branch': 'git_branch',
        'GitDirty': 'git_dirty',
        'filename': 'filename',
        'EventName': 'event',
        'MatchNumber': 'match_id',
        'ReplayNumber': 'replay_num',
        'MatchType': 'match_type',
        'IsRedAlliance': 'is_red_alliance',
        'StationNumber': 'station_num'}, inplace=True)
    
    return (meta_df, year)

#essentially find match start
def calculate_match_start(df):
    print('Finding Match Start')
    # filter dataframe to after the match starts to get rid of unneeded data
    #everything is a string right now.  kinda dumb.
    enabled_df = df.loc[(df['entry'] == 'DS:enabled') & (df['value'] == "True")]
    enable_ts = enabled_df['timestamp'].min()
    print(f'enabled at: {enable_ts}')
    return enable_ts

    

def trim_df_by_timestamp(df, ts):
    print('dropping rows before start')
    # drop rows before the enable timestamp
    df = df[df['timestamp'] > ts]

    #now calcuate match_time.
    print('adding match time')
    df['match_time'] = df.timestamp - ts

    return df

#fix datatypes
#things have been a string up until now
def fix_datatypes(df):
    df_boolean_log_data = df[df['data_type'] == 'boolean']
    df_boolean_log_data['boolean_value'] = df_boolean_log_data['value'].astype(bool)
    df_numerical_log_data = df[(df['data_type'] == 'int64') | (df['data_type'] == 'double') | (df['data_type'] == 'float')]
    df_numerical_log_data['numeric_value'] = pd.to_numeric(df_numerical_log_data['value'])
    df_other_log_data = df[(df['data_type'] != 'boolean') & (df['data_type'] != 'int64') & (df['data_type'] != 'double') & (df['data_type'] != 'float')]

    df_log_data = pd.concat([df_boolean_log_data, df_numerical_log_data, df_other_log_data])
    df_log_data['timestamp'] = df_log_data['timestamp'].astype('int64')

    return df_log_data

    
def parse_metrics(df, logfile, comp_name, match_id, replay_num):
    print(f'Parsing Metrics')
    #gets rid of the '/Robot/m_robotContainer' in front of the entries in the metrics_df
    metrics_df['entry'] = metrics_df['entry'].str.replace('/Robot/m_robotContainer/','')

    # metrics_df[['component', 'metric']] = metrics_df['entry'].str.rsplit('/',n=1, expand=True)
  
    #fix datatypes
    df_log_data = fix_datatypes(metrics_df)

    
    # #calculate metrics stats (min, max, median)
    # match_metrics = df_numerical_log_data.groupby(['component', 'metric']).agg(
    #     minimum=pd.NamedAgg(column='numeric_value', aggfunc='min'),
    #     maximun=pd.NamedAgg(column='numeric_value', aggfunc='max'),
    #     average=pd.NamedAgg(column='numeric_value', aggfunc=np.mean)
    # ).reset_index()


    return df_log_data


def add_keys(df, filename, event, match_id, replay_num):
    df['filename'] = filename
    df['event'] = event
    df['match_id'] = pd.to_numeric(match_id)
    df['replay_num'] = pd.to_numeric(replay_num)



if __name__ == "__main__":

    #Debug statement
    print('Starting....')
    
    #checks that the "system arguments" array's length is two
    #note that argv[0] is always the script name
    if len(sys.argv) != 2:
        print(f"Usage: {sys.argv[0]} <file>", file=sys.stderr)
        sys.exit(1)

    #the argument passed in by the user is the filename
    filepath = sys.argv[1]

    #only use the filename for the key, but use the path to calculate the hash
    filename = os.path.basename(filepath)
    hash = calculate_file_hash(filepath)

    #load config from `config.json`
    #this config lists "prefixes" from log entries that can be used to organize the data
    config = json.load(open('config.json'))
    
    #reads in home-made dataframe
    map_df = pd.read_csv('map.csv', header=0)

    #open the db connection:
    conn = setup_db("db/metrics.db")


    #start the import by checking if the file has already been imported.
    (is_duplicate, existing_filename) = is_file_already_imported(conn, filepath)

    if is_duplicate:
        print(f"File {filename} has already been imported as {existing_filename}. Skipping.")
        sys.exit(0)
    else:
        print(f'Starting import of {filename} from {filepath}')

    #create entry, but mark it as False
    update_file_metadata(conn, filename, hash, 0)

    #read the input
    #also creates another variable for the filename for some reason?
    (logfile, df) = read_logfile(filepath)

    #flush_tables(conn, logfile)

    #splits the output dataframe from the "read_logfile" function into dataframes to be utilized seperately
    #meta_df is log metadata, not file metadata
    (meta_df, fms_df, metrics_df, vision_df, preferences_df) = split_dataframe(df, config)
    
    #effictively merges the log metadata and fms data into a single clean metadata dataframe
    #still note this metadata dataframe does not contain file metadata
    (meta_df, year) = parse_metadata(meta_df, fms_df, logfile)
    
    if year == "":
        print("Failed to get year from build date")
    else: 
        meta_df["event_year"] = year
        metrics_df["event_year"] = year
        print("Successfully got year from build date")
    
    #finds match time
    enabled_ts = calculate_match_start(df)

    #trims metrics dataframe to after the match starts
    #also adds match time to data_frame
    metrics_df = trim_df_by_timestamp(metrics_df, enabled_ts)

    #mergs map_df and metrics_df
    metrics_df = metrics_df.merge(right=map_df, how='left', on='entry')

    metrics_df = parse_metrics(metrics_df, logfile, meta_df.at[0,'event'],meta_df.at[0,'match_id'],meta_df.at[0,'replay_num'])
    
    #preferences_df = fix_datatypes(preferences_df)
    #vision_df = fix_datatypes(vision_df)

    #adds keys from meta_df to dataframes to show obvious connection between dataframes
    add_keys(metrics_df, logfile, meta_df.at[0,'event'],meta_df.at[0,'match_id'],meta_df.at[0,'replay_num'])
    # add_keys(summary_df, logfile, meta_df.at[0,'event'],meta_df.at[0,'match_id'],meta_df.at[0,'replay_num'])
    #add_keys(preferences_df,logfile, meta_df.at[0,'event'],meta_df.at[0,'match_id'],meta_df.at[0,'replay_num'])
    #add_keys(vision_df, logfile, meta_df.at[0,'event'],meta_df.at[0,'match_id'],meta_df.at[0,'replay_num'])
    
    #########################  DB LOADING #########################
    print('loading into DB')

    write_dataframe(meta_df, 'log_metadata',conn)

    # write_dataframe(summary_df, 'metrics_summary',conn)

    #write_dataframe(metrics_df, 'metrics',conn)

    #write_dataframe(preferences_df, 'preferences',conn)

    #write_dataframe(vision_df, 'vision', conn)

    #come back and make it true
    update_file_metadata(conn, filename, hash, 0)

    close_db(conn)

    print('Done.')