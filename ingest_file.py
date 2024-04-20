import os
import pandas as pd
import sys
from sqlite3 import connect 
import csv_converter 
import json
import numpy as np




def setup_db(db_name):

    connection = connect(db_name)

    cursor = connection.cursor()

    cursor.execute('CREATE TABLE IF NOT EXISTS log_metadata (filename TEXT PRIMARY KEY, build_date TEXT, commit_hash TEXT, git_date TEXT, git_branch TEXT, project_name TEXT, git_dirty TEXT, event TEXT, match_id TEXT, replay_num TEXT, match_type TEXT, is_red_alliance TEXT, station_num TEXT)')
    connection.commit()

    cursor.execute('CREATE TABLE IF NOT EXISTS metrics (entry TEXT, data_type TEXT, value TEXT, timestamp REAL, match_time REAL, subsystem TEXT, component TEXT, part TEXT, type TEXT, metric TEXT , boolean_value TEXT, numeric_value REAL, filename TEXT, event TEXT, match_id REAL, replay_num REAL)')

    cursor.execute('CREATE INDEX IF NOT EXISTS metrics_idx_event on metrics (event)')
    cursor.execute('CREATE INDEX IF NOT EXISTS metrics_idx_filename on metrics (filename)')
    cursor.execute('CREATE INDEX IF NOT EXISTS metrics_idx_event_match on metrics (event, match_id)')
    cursor.execute('CREATE INDEX IF NOT EXISTS metrics_idx_component on metrics (component)')
    cursor.execute('CREATE INDEX IF NOT EXISTS metrics_idx_component on metrics (component, metric)')
    connection.commit()

    # cursor.execute('CREATE TABLE IF NOT EXISTS metrics_summary (component TEXT, metric TEXT ,minimum REAL ,maximun REAL ,average REAL ,filename TEXT, event TEXT, match_id REAL, replay_num REAL)')

    # cursor.execute('CREATE INDEX IF NOT EXISTS metrics_summary_idx_event on metrics_summary (event)')
    # cursor.execute('CREATE INDEX IF NOT EXISTS metrics_summary_idx_filename on metrics_summary (filename)')
    # cursor.execute('CREATE INDEX IF NOT EXISTS metrics_summary_idx_event_match on metrics_summary (event, match_id)')
    # cursor.execute('CREATE INDEX IF NOT EXISTS metrics_summary_idx_component on metrics_summary (component)')
    # cursor.execute('CREATE INDEX IF NOT EXISTS metrics_summary_idx_component_metric on metrics_summary (component, metric)')
    # connection.commit()

    cursor.execute('CREATE TABLE IF NOT EXISTS preferences ( entry TEXT, data_type TEXT , value TEXT, timestamp REAL , boolean_value TEXT, numeric_value REAL, filename TEXT, event TEXT, match_id REAL, replay_num REAL)')
    cursor.execute('CREATE INDEX IF NOT EXISTS preference_idx_filename on preferences (filename)')
    connection.commit()

    cursor.execute('CREATE TABLE IF NOT EXISTS vision ( entry TEXT, data_type TEXT , value TEXT, timestamp REAL , boolean_value TEXT, numeric_value REAL, filename TEXT, event TEXT, match_id REAL, replay_num REAL)')
    cursor.execute('CREATE INDEX IF NOT EXISTS vision_idx_filename on vision (filename)')
    connection.commit()

    return connection


def flush_tables(connection, filename):
    cursor = connection.cursor()

    cursor.execute(f'SELECT * FROM log_metadata where filename = "{filename}"')

    results = cursor.fetchall()

    if len(results) > 0:
        #TODO add in summary?
        for t in ['log_metadata', 'metrics', 'preferences', 'vision']:
            cursor.execute(f'DELETE FROM {t} WHERE filename = "{filename}"')

        connection.commit()



def write_dataframe(df, tablename, connection, filename = None):

    df.to_sql(tablename, connection, if_exists='append', index=False)
    connection.commit()

    if filename is not None:
        df.to_csv(filename, index=False)

def close_db(connection):
    connection.commit()
    connection.close()

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

def split_dataframe(df, cfg):
    meta_df = df.loc[df['entry'].str.startswith(cfg['metadata_prefix'])]
    fms_df = df.loc[df['entry'].str.startswith(cfg['fms_prefix'])]
    metrics_df = df.loc[df['entry'].str.startswith(cfg['metrics_prefix'])]
    vision_df = df.loc[df['entry'].str.startswith(cfg['photon_prefix'], cfg['camerapub_prefix'])]
    preferences_df = df.loc[df['entry'].str.startswith(cfg['preferences_prefix'])]

    return (meta_df, fms_df, metrics_df, vision_df, preferences_df)


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
    
    return meta_df

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

    print('Starting....')
    if len(sys.argv) != 2:
        print(f"Usage: {sys.argv[0]} <file>", file=sys.stderr)
        sys.exit(1)


    #load config from `config.json`
    config = json.load(open('config.json'))

    map_df = pd.read_csv('map.csv', header=0)

    #open the db connection:
    conn = setup_db("db/metrics.db")

    #read the input
    (logfile, df) = read_logfile(sys.argv[1])

    flush_tables(conn, logfile)

    
    (meta_df, fms_df, metrics_df, vision_df, preferences_df) = split_dataframe(df, config)

    meta_df = parse_metadata(meta_df, fms_df, logfile)
    #add match time and filter early data.

    enabled_ts = calculate_match_start(df)

    metrics_df = trim_df_by_timestamp(metrics_df, enabled_ts)

    metrics_df = metrics_df.merge(right=map_df, how='left', on='entry')

    metrics_df = parse_metrics(metrics_df, logfile, meta_df.at[0,'event'],meta_df.at[0,'match_id'],meta_df.at[0,'replay_num'])
    
    preferences_df = fix_datatypes(preferences_df)
    vision_df = fix_datatypes(vision_df)

    add_keys(metrics_df, logfile, meta_df.at[0,'event'],meta_df.at[0,'match_id'],meta_df.at[0,'replay_num'])
    # add_keys(summary_df, logfile, meta_df.at[0,'event'],meta_df.at[0,'match_id'],meta_df.at[0,'replay_num'])
    add_keys(preferences_df,logfile, meta_df.at[0,'event'],meta_df.at[0,'match_id'],meta_df.at[0,'replay_num'])
    add_keys(vision_df, logfile, meta_df.at[0,'event'],meta_df.at[0,'match_id'],meta_df.at[0,'replay_num'])
    
    #########################  DB LOADING #########################
    print('loading into DB')

    write_dataframe(meta_df, 'log_metadata',conn)

    # write_dataframe(summary_df, 'metrics_summary',conn)

    write_dataframe(metrics_df, 'metrics',conn)

    write_dataframe(preferences_df, 'preferences',conn)

    write_dataframe(vision_df, 'vision', conn)
    
    close_db(conn)