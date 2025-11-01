import os
import pandas as pd
import sys
from sqlite3 import connect 
import json
from ingest_library import *

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

if __name__ == "__main__":
    
    #Debug statement
    print('Starting....')
    
    #checks that the "system arguments" array's length is two
    #note that argv[0] is always the script name
    if len(sys.argv) != 4:
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
    (is_duplicate, existing_filename) = is_file_already_imported(conn, hash)

    if is_duplicate:
        print(f"File {filename} has already been imported as {existing_filename}. Skipping.")
        sys.exit(0)

    #create entry, but mark it as False
    update_file_metadata(conn, filename, hash, 0)

    #reads the inputs
    df = read_system_logfile(filepath)

    #flush_tables(conn, logfile)

    #splits the output dataframe from the "read_logfile" function into dataframes to be utilized seperately
    #meta_df is log metadata, not file metadata
    cfg = json.load(open('log_configs/config' + sys.argv[3] + '.json'))
    (meta_df, fms_df, metrics_df, vision_df, preferences_df) = split_system_dataframe(df, cfg)
    
    #effictively merges the log metadata and fms data into a single clean metadata dataframe
    #still note this metadata dataframe does not contain file metadata
    (meta_df) = parse_metadata_from_system(meta_df, fms_df)
    meta_df['filename'] = filename
    
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
    metrics_map_df = pd.read_csv('datamaps/' + sys.argv[3] + '/metrics_map.csv', header=0)
    vision_map_df = pd.read_csv('datamaps/' + sys.argv[3] + '/vision_map.csv', header=0)

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
    add_keys(metrics_df, sys.argv[3], meta_df.at[0,'event'], meta_df.at[0,'match_id'], meta_df.at[0,'match_type'], meta_df.at[0,'replay_num'])
    metrics_df['filename'] = filename
    
    add_keys(vision_df, sys.argv[3], meta_df.at[0,'event'], meta_df.at[0,'match_id'], meta_df.at[0,'match_type'], meta_df.at[0,'replay_num'])
    vision_df['filename'] = filename
    
    add_keys(preferences_df, sys.argv[3], meta_df.at[0,'event'], meta_df.at[0,'match_id'], meta_df.at[0,'match_type'], meta_df.at[0,'replay_num'])
    preferences_df.drop(columns = 'timestamp', inplace = True)

    write_dataframe(meta_df, 'log_metadata', conn)
    
    write_dataframe(metrics_df, 'device_data_raw', conn)
    
    if device_telemetry_df is not None:
        add_keys(device_telemetry_df, sys.argv[3], meta_df.at[0,'event'], meta_df.at[0,'match_id'], meta_df.at[0,'match_type'], meta_df.at[0,'replay_num'])
        write_dataframe(device_telemetry_df, 'device_telemetry', conn)
        
    if device_stats_df is not None:
        add_keys(device_stats_df, sys.argv[3], meta_df.at[0,'event'], meta_df.at[0,'match_id'], meta_df.at[0,'match_type'], meta_df.at[0,'replay_num'])
        write_dataframe(device_stats_df, 'device_stats', conn)
    
    write_dataframe(vision_df, 'vision_data_raw', conn)
    
    if vision_telemetry_df is not None:
        add_keys(vision_telemetry_df, sys.argv[3], meta_df.at[0,'event'], meta_df.at[0,'match_id'], meta_df.at[0,'match_type'], meta_df.at[0,'replay_num'])
        write_dataframe(vision_telemetry_df, 'vision_telemetry', conn)
    
    if vision_stats_df is not None:
        add_keys(vision_stats_df, sys.argv[3], meta_df.at[0,'event'], meta_df.at[0,'match_id'], meta_df.at[0,'match_type'], meta_df.at[0,'replay_num'])
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