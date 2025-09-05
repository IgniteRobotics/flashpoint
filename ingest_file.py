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

    #creates log metadata table
    cursor.execute('''CREATE TABLE IF NOT EXISTS log_metadata (
        filename TEXT PRIMARY KEY, 
        build_date TEXT, 
        commit_hash TEXT, 
        git_date TEXT, 
        git_branch TEXT, 
        project_name TEXT, 
        git_dirty TEXT, 
        event TEXT, 
        match_id TEXT, 
        replay_num TEXT, 
        match_type TEXT, 
        is_red_alliance TEXT, 
        station_num TEXT)''')
    connection.commit()
    
    #creates raw device data table
    cursor.execute('''CREATE TABLE IF NOT EXISTS device_data_raw (
        filename TEXT,
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
        metric TEXT, 
        boolean_value TEXT, 
        numeric_value REAL)''')
    connection.commit()
    
    #creates device telemetry table
    cursor.execute('''CREATE TABLE IF NOT EXISTS device_telemetry (
    event_year TEXT,
    event TEXT,
    match_id REAL,
    replay_num REAL,
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
    event_year REAL,
    event TEXT,
    match_id REAL,
    replay_num REAL,
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
    
    #creates vision_data_raw table
    cursor.execute('''CREATE TABLE IF NOT EXISTS vision_data_raw (
        filename TEXT,
        event_year TEXT, 
        event TEXT, 
        match_id REAL, 
        replay_num REAL, 
        entry TEXT, 
        data_type TEXT, 
        value TEXT, 
        timestamp REAL, 
        match_time REAL, 
        camera TEXT,
        metric TEXT , 
        boolean_value TEXT, 
        numeric_value REAL)''')
    connection.commit()
    
    #creates vision telemetry table
    cursor.execute('''CREATE TABLE IF NOT EXISTS vision_telemetry (
    event_year TEXT,
    event TEXT,
    match_id REAL,
    replay_num REAL,
    match_time REAL,
    camera TEXT,
    latency REAL,
    hasTarget TEXT)''')
    connection.commit()
    
    #creates vision stats table
    cursor.execute('''CREATE TABLE IF NOT EXISTS vision_stats (
    event_year TEXT,
    event TEXT,
    match_id REAL,
    replay_num REAL,
    camera TEXT,
    avg_latency REAL,
    min_latency REAL,
    max_latency REAL,
    stddev_latency REAL)''')
    connection.commit()
    
    #creates raw device data table
    cursor.execute('''CREATE TABLE IF NOT EXISTS preferences (
        event_year TEXT, 
        event TEXT, 
        match_id REAL, 
        replay_num REAL, 
        entry TEXT, 
        data_type TEXT, 
        value TEXT)''')
    connection.commit()
    
    
    
    #cursor.execute('CREATE INDEX IF NOT EXISTS telemetry_idx_match on device_telemetry (event_year, event, match_id)')
    #cursor.execute('CREATE INDEX IF NOT EXISTS telemetry_idx_component on device_telemetry (subsystem, assembly, subassembly, component)')
    #connection.commit()
    
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

    print(f'Reading into dataframe')
    #read csv into dataframe
    colnames=['entry', 'data_type', 'value', 'timestamp']
    df = pd.read_csv(output_csv, quotechar='|', header=None, names=colnames)

    return (df)

#splits output dataframe from log file into dataframes to be utilized individually
#utlizies prefixes from homemade configs
def split_dataframe(df, cfg):
    meta_df = df.loc[df['entry'].str.startswith(cfg['metadata_prefix'])]
    preferences_df = df.loc[df['entry'].str.startswith(cfg['preferences_prefix'])]
    fms_df = df.loc[df['entry'].str.startswith(cfg['fms_prefix'])]
    metrics_df = df.loc[df['entry'].str.startswith(cfg['metrics_prefix'])]
    vision_df = df.loc[df['entry'].str.startswith(cfg['photon_prefix'], cfg['camerapub_prefix'])]
    
    #readability
    metrics_df['entry'] = metrics_df['entry'].str.replace(cfg['metrics_prefix'],'')
    vision_df['entry'] = vision_df['entry'].str.replace(cfg['photon_prefix'], '').str.replace(cfg['camerapub_prefix'], '')

    return (meta_df, fms_df, metrics_df, vision_df, preferences_df)

#creates a new metadata frame from log metadata and fms data
#also gets the year from the build date
def parse_metadata(meta_df, fms_df, filename):
    print('Parsing Metadata')
    #get metadata
    metadata = {}
    #iterate and split records because it's logged funky
    for index, row in meta_df.iterrows():
        #split on ': '
        (key, value) = row['value'].split(': ')
        metadata[key] = value
            
    metadata['filename'] = filename


    fms_items =['EventName','MatchNumber','ReplayNumber','MatchType','IsRedAlliance','StationNumber']
    #iterate and map values.  there are dupes in the data and we take the last.
    for index, row in fms_df.iterrows():
        key = row['entry'].split('/')[-1]
        if key in fms_items:
            metadata[key] = row['value']

    
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
    
    return (meta_df)

#essentially find match start
def calculate_match_period(df):
    print('Finding Match Start')
    # filter dataframe to after the match starts to get rid of unneeded data
    #everything is a string right now.  kinda dumb.
    enabled_df = df.loc[(df['entry'] == 'DS:enabled') & (df['value'] == "True")]
    enable_ts = enabled_df['timestamp'].astype('int64').min()
    print(f'enabled at: {enable_ts}')
    
    terminated_ts = -1
    try:
        terminated_df = df.loc[(df['entry'] == 'DS:enabled') & (df['value'] == "False") & (df['timestamp'] > enable_ts)]
        terminated_ts = terminated_df['timestamp'].astype('int64').max()
        print(f'disabled at: {terminated_ts}')
    except KeyError as e:
        print('Failed to find termination timestamp')
    
    return (enable_ts, terminated_ts)

    

def trim_df_by_timestamp(df, ts):
    # drop rows before the enable timestamp
    df = df[df['timestamp'].astype('int64') > ts]

    #now calcuate match_time.
    print('adding match time')
    df['match_time'] = df.timestamp - ts

    return df

def trim_tail(df, ts):
    #drop rows after disabled timestamp
    df = df[df['timestamp'].astype('int64') < ts]
    
    return df
    
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

def add_keys(df, event_year, event, match_id, replay_num):
    df['event_year'] = event_year
    df['event'] = event
    df['match_id'] = pd.to_numeric(match_id)
    df['replay_num'] = pd.to_numeric(replay_num)

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

def read_vision_data_raw (df):
    #creates a deep copy of the raw dataframe
    telemetry_df = df[df['metric'].notnull()]
    
    #drops unnecessary columns
    telemetry_df.drop(columns = ['entry', 'value', 'timestamp', 'data_type'], inplace = True)
    
    #removes data that is not necessary for this dataframe
    #also splits up data temporarily as needed
    has_target_data = True
    has_latency_data = True
    
    try:
        target_df = telemetry_df.loc[(telemetry_df['metric'] == 'HAS_TARGET')]
    except KeyError:
        has_target_data = False
    
    try:
        latency_df = telemetry_df.loc[(telemetry_df['metric'] == 'LATENCY')]
    except KeyError:
        has_latency_data = False
    
    if not has_target_data and not has_latency_data:
        return (None, None)
           
    telemetry_df.drop(columns = ['metric', 'numeric_value', 'boolean_value'], inplace = True)
    telemetry_df.drop_duplicates(inplace = True)  
      
    if has_target_data:
        target_df.drop(columns = ['metric', 'numeric_value'], inplace = True)    
        target_df.rename(columns = {'boolean_value' : 'hasTarget'}, inplace = True)   
        telemetry_df = pd.merge(telemetry_df, target_df, on = ['match_time', 'camera'], how = 'outer')
    else:
        telemetry_df['hasTarget'] = np.nan
        
    if has_latency_data:
        latency_df.drop(columns = ['metric', 'boolean_value'], inplace = True)
        latency_df.rename(columns = {'numeric_value' : 'latency'}, inplace = True)
        telemetry_df = pd.merge(telemetry_df, latency_df, on = ['match_time', 'camera'], how = 'outer')
        stats_df = latency_df.drop(columns = 'match_time').groupby('camera', as_index = False).agg(
            avg_latency = ('latency', 'mean'),
            min_latency = ('latency', 'min'),
            max_latency = ('latency', 'max'),
            stddev_latency = ('latency', 'std'))
    else:
        telemetry_df['latency'] = np.nan
        stats_df = None
        

    return(telemetry_df, stats_df)

if __name__ == "__main__":
    
    #Debug statement
    print('Starting....')
    
    #checks that the "system arguments" array's length is two
    #note that argv[0] is always the script name
    if len(sys.argv) != 3:
        print(f"Usage: {sys.argv[0]} <file>", file=sys.stderr)
        sys.exit(1)

    #the argument passed in by the user is the filename
    filepath = sys.argv[1]

    #only use the filename for the key, but use the path to calculate the hash
    filename = os.path.basename(filepath)
    hash = calculate_file_hash(filepath)
    
    #open the db connection:
    #"db/robot.db"
    conn = setup_db(sys.argv[2])

    #start the import by checking if the file has already been imported.
    (is_duplicate, existing_filename) = is_file_already_imported(conn, filepath)

    if is_duplicate:
        print(f"File {filename} has already been imported as {existing_filename}. Skipping.")
        sys.exit(0)
    else:
        print(f'Starting import of {filename} from {filepath}')

    #create entry, but mark it as False
    update_file_metadata(conn, filename, hash, 0)

    #reads the input
    df = read_logfile(filepath)

    #flush_tables(conn, logfile)

    #splits the output dataframe from the "read_logfile" function into dataframes to be utilized seperately
    #meta_df is log metadata, not file metadata
    cfg = json.load(open('log_configs/config' + filename[4:8] + '.json'))
    (meta_df, fms_df, metrics_df, vision_df, preferences_df) = split_dataframe(df, cfg)
    
    #effictively merges the log metadata and fms data into a single clean metadata dataframe
    #still note this metadata dataframe does not contain file metadata
    (meta_df) = parse_metadata(meta_df, fms_df, filename)
    
    #finds match time
    (enabled_ts, disabled_ts) = calculate_match_period(df)

    #trims metrics dataframe to after the match starts
    #also adds match time to data_frame
    print('dropping rows before start')
    metrics_df = trim_df_by_timestamp(metrics_df, enabled_ts)
    vision_df = trim_df_by_timestamp(vision_df, enabled_ts)
    
    if(disabled_ts != -1):
        print('dropping rows after end')
        metrics_df = trim_tail(metrics_df, disabled_ts)
        vision_df = trim_tail(vision_df, disabled_ts)

    #reads in home-made dataframes
    metrics_map_df = pd.read_csv('datamaps/' + meta_df.at[0, 'build_date'].split('-')[0] + '/metrics_map.csv', header=0)
    vision_map_df = pd.read_csv('datamaps/' + meta_df.at[0, 'build_date'].split('-')[0] + '/vision_map.csv', header=0)

    #merges dataframes with maps
    metrics_df = metrics_df.merge(right=metrics_map_df, how='left', on='entry')
    vision_df = vision_df.merge(right=vision_map_df, how ='left', on='entry')

    print(f'Parsing')
    metrics_df = fix_datatypes(metrics_df)
    vision_df = fix_datatypes(vision_df)
    
    #creates parsed dataframes
    (device_telemetry_df, device_stats_df) = read_device_data_raw(metrics_df)
    (vision_telemetry_df, vision_stats_df) = read_vision_data_raw(vision_df)
    
    #adds additional keys
    add_keys(metrics_df, meta_df.at[0, 'build_date'].split('-')[0], meta_df.at[0,'event'], meta_df.at[0,'match_id'], meta_df.at[0,'replay_num'])
    metrics_df['filename'] = filename
    
    add_keys(vision_df, meta_df.at[0, 'build_date'].split('-')[0], meta_df.at[0,'event'], meta_df.at[0,'match_id'], meta_df.at[0,'replay_num'])
    vision_df['filename'] = filename
    
    add_keys(preferences_df, meta_df.at[0, 'build_date'].split('-')[0], meta_df.at[0,'event'], meta_df.at[0,'match_id'], meta_df.at[0,'replay_num'])
    preferences_df.drop(columns = 'timestamp', inplace = True)

    write_dataframe(meta_df, 'log_metadata', conn)
    
    write_dataframe(metrics_df, 'device_data_raw', conn)
    
    if device_telemetry_df is not None:
        add_keys(device_telemetry_df, meta_df.at[0, 'build_date'].split('-')[0], meta_df.at[0,'event'], meta_df.at[0,'match_id'], meta_df.at[0,'replay_num'])
        write_dataframe(device_telemetry_df, 'device_telemetry', conn)
        
    if device_stats_df is not None:
        add_keys(device_stats_df, meta_df.at[0, 'build_date'].split('-')[0], meta_df.at[0,'event'], meta_df.at[0,'match_id'], meta_df.at[0,'replay_num'])
        write_dataframe(device_stats_df, 'device_stats', conn)
    
    write_dataframe(vision_df, 'vision_data_raw', conn)
    
    if vision_telemetry_df is not None:
        add_keys(vision_telemetry_df, meta_df.at[0, 'build_date'].split('-')[0], meta_df.at[0,'event'], meta_df.at[0,'match_id'], meta_df.at[0,'replay_num'])
        write_dataframe(vision_telemetry_df, 'vision_telemetry', conn)
    
    if vision_stats_df is not None:
        add_keys(vision_stats_df, meta_df.at[0, 'build_date'].split('-')[0], meta_df.at[0,'event'], meta_df.at[0,'match_id'], meta_df.at[0,'replay_num'])
        write_dataframe(vision_stats_df, 'vision_stats', conn)
        
    write_dataframe(preferences_df, 'preferences', conn)
    
    #write_dataframe(device_stats_df, 'device_stats', conn)
    
    # write_dataframe(summary_df, 'metrics_summary',conn)

    #write_dataframe(metrics_df, 'metrics',conn)

    #write_dataframe(preferences_df, 'preferences',conn)

    #write_dataframe(vision_df, 'vision', conn)

    #come back and make it true
    update_file_metadata(conn, filename, hash, 1)

    close_db(conn)

    print('Done.')