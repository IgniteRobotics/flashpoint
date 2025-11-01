from pygwalker.api.streamlit import StreamlitRenderer
import pandas as pd
import streamlit as st
from sqlite3 import connect 

# You should cache your pygwalker renderer, if you don't want your memory to explode
#@st.cache_resource
#def get_vision_stats_renderer(conn) -> "StreamlitRenderer":
#    df = pd.read_sql_query("SELECT * FROM vision_stats", conn)
    # If you want to use feature of saving chart config, set `spec_io_mode="rw"`
#    return StreamlitRenderer(df, spec="./gw_config.json", spec_io_mode="rw")

def viz_vision_stats(conn):
    title = 'Vision Statistics Table'
    st.title(title)
    df = pd.read_sql_query("SELECT * FROM vision_stats", conn)
    df.drop(columns=['event_year','event','match_id','match_type','replay_num'], inplace=True)
    df.set_index('camera',inplace=True)
    df['avg_latency'].astype(int)
    df.style.format(precision=0)
    st.table(df)

if __name__ == '__main__':
    conn = connect("db/robot.db")
    viz_vision_stats(conn)