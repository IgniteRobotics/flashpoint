import json
import os
from ingest_library import *
import sys

def ingest_match_logs(system_log_filepath, drivetrain_devices_filepath, rio_devices_filepath, db_filepath):
    print("Starting")
    
    #only use the filename for the key, but use the path to calculate the hash
    system_log_filename = os.path.basename(system_log_filepath)
    drivetrain_devices_filename = os.path.basename(drivetrain_devices_filepath)
    rio_devices_filename = os.path.basename(rio_devices_filepath)
    
    system_log_hash = calculate_file_hash(system_log_filepath)
    drivetrain_devices_hash = calculate_file_hash(drivetrain_devices_filepath)
    rio_devices_hash = calculate_file_hash(rio_devices_filepath)
    
    #open the db/connection
    #db/robot.db
    conn = setup_db(db_filepath)
    
    update_file_metadata(conn, system_log_filename, system_log_hash, 0)
    update_file_metadata(conn, drivetrain_devices_filename, drivetrain_devices_hash, 0)
    update_file_metadata(conn, rio_devices_filename, rio_devices_hash, 0)
    
    print("Reading in system log file")
    system_df = read_system_logfile(system_log_filepath)
    print("Reading in drivetrain log file")
    drive_df = read_device_logfile(drivetrain_devices_filepath)
    print("reading in rio log file")
    rio_df = read_device_logfile(rio_devices_filepath)
    
    #splits the output dataframe from the "read_logfile" function into dataframes to be utilized seperately
    #meta_df is log metadata, not file metadata
    cfg = json.load(open('log_configs/config2025.json'))
    (meta_df, fms_df, system_metrics_df, vision_df, preferences_df) = split_system_dataframe(system_df, cfg)
    
    meta_df = parse_metadata_from_system(meta_df, fms_df)
    
    meta_system_df = meta_df.copy(True)
    meta_drive_df = meta_df.copy(True)
    meta_rio_df = meta_df.copy(True)
    
    meta_system_df['filename'] = system_log_filename
    meta_drive_df['filename'] = drivetrain_devices_filename
    meta_rio_df['filename'] = rio_devices_filename
    
    write_dataframe(meta_system_df, 'log_metadata', conn)
    write_dataframe(meta_drive_df, 'log_metadata', conn) 
    write_dataframe(meta_rio_df, 'log_metadata', conn)
    
    (enabled_ts, disabled_ts) = calculate_match_period(system_df)
    
    print('dropping rows before start')
    drive_df = trim_df_by_timestamp(drive_df, enabled_ts)
    rio_df = trim_df_by_timestamp(rio_df, enabled_ts)
    vision_df = trim_df_by_timestamp(vision_df, enabled_ts)
    
    if(disabled_ts != -1):
        print('dropping rows after end')
        drive_df = trim_tail(drive_df, disabled_ts)
        rio_df = trim_tail(rio_df, disabled_ts)
        vision_df = trim_tail(vision_df, disabled_ts)
    
    print('reading maps')
    drive_map_df = pd.read_csv('datamaps/drivetrain_devices_map.csv')
    rio_map_df = pd.read_csv('datamaps/rio_devices_map.csv')
    vision_map_df = pd.read_csv('datamaps/2025/vision_map.csv')
    
    print('mapping entries to identifiers')
    drive_df = drive_df.merge(right=drive_map_df, how='left', on='entry')
    rio_df = rio_df.merge(right=rio_map_df,how='left',on='entry')
    vision_df = vision_df.merge(right=vision_map_df,how='left',on='entry')
    
    print('Fixing datatypes')
    drive_df = fix_datatypes(drive_df)
    rio_df = fix_datatypes(rio_df)
    vision_df = fix_datatypes(vision_df)
    
    print('Parsing data')
    (drive_telemetry_df,drive_stats_df) = read_device_data_raw(drive_df)
    (rio_telemetry_df,rio_stats_df) = read_device_data_raw(rio_df)
    (vision_telemetry_df,vision_stats_df) = read_vision_data_raw(vision_df)
    
    print('uploading to database')
    add_keys(drive_df, '2025', meta_df.at[0,'event'], meta_df.at[0,'match_id'], meta_df.at[0,'replay_num'])
    drive_df['filename'] = drivetrain_devices_filename
    write_dataframe(drive_df, 'device_data_raw', conn)
    
    add_keys(rio_df, '2025', meta_df.at[0,'event'], meta_df.at[0,'match_id'], meta_df.at[0,'replay_num'])
    rio_df['filename'] = rio_devices_filename
    write_dataframe(rio_df, 'device_data_raw', conn)
    
    add_keys(vision_df, '2025', meta_df.at[0,'event'], meta_df.at[0,'match_id'], meta_df.at[0,'replay_num'])
    vision_df['filename'] = system_log_filename
    write_dataframe(vision_df, 'vision_data_raw', conn)
    
    add_keys(preferences_df, '2025', meta_df.at[0,'event'], meta_df.at[0,'match_id'], meta_df.at[0,'replay_num'])
    preferences_df.drop(columns = 'timestamp', inplace = True)
    write_dataframe(preferences_df, 'preferences', conn)
    
    if drive_telemetry_df is not None:
        add_keys(drive_telemetry_df, '2025', meta_df.at[0,'event'], meta_df.at[0,'match_id'], meta_df.at[0,'replay_num'])
        write_dataframe(drive_telemetry_df, 'device_telemetry', conn)
        
    if drive_stats_df is not None:
        add_keys(drive_stats_df, '2025', meta_df.at[0,'event'], meta_df.at[0,'match_id'], meta_df.at[0,'replay_num'])
        write_dataframe(drive_stats_df, 'device_stats', conn)
    
    if rio_telemetry_df is not None:
        add_keys(rio_telemetry_df, '2025', meta_df.at[0,'event'], meta_df.at[0,'match_id'], meta_df.at[0,'replay_num'])
        write_dataframe(rio_telemetry_df, 'device_telemetry', conn)
        
    if rio_stats_df is not None:
        add_keys(rio_stats_df, '2025', meta_df.at[0,'event'], meta_df.at[0,'match_id'], meta_df.at[0,'replay_num'])
        write_dataframe(rio_stats_df, 'device_stats', conn)
    
    if vision_telemetry_df is not None:
        add_keys(vision_telemetry_df, '2025', meta_df.at[0,'event'], meta_df.at[0,'match_id'], meta_df.at[0,'replay_num'])
        write_dataframe(vision_telemetry_df, 'vision_telemetry', conn)
        
    if vision_stats_df is not None:
        add_keys(vision_stats_df, '2025', meta_df.at[0,'event'], meta_df.at[0,'match_id'], meta_df.at[0,'replay_num'])
        write_dataframe(vision_stats_df, 'vision_stats', conn)
    
    print('Updating file metadata')
    update_file_metadata(conn, system_log_filename, system_log_hash, 1)
    update_file_metadata(conn, drivetrain_devices_filename, drivetrain_devices_hash, 1)
    update_file_metadata(conn, rio_devices_filename, rio_devices_hash, 1)
    
    close_db(conn)
    print('Done')

if __name__ == "__main__":
    if len(sys.argv) != 5:
        print(f"Usage: {sys.argv[0]} <file>", file=sys.stderr)
        sys.exit(1)
    ingest_match_logs(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4])
    
    