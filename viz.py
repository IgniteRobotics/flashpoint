from pygwalker.api.streamlit import StreamlitRenderer
import pandas as pd
import streamlit as st
from sqlite3 import connect 
title = 'Firematics Robot Telemetry'
conn = connect("db/robot.db")

# Adjust the width of the Streamlit page
st.set_page_config(
    page_title=title,
    layout="wide"
)

# Add Title
st.title(title)

# You should cache your pygwalker renderer, if you don't want your memory to explode
#@st.cache_resource
def get_pyg_renderer() -> "StreamlitRenderer":
    st.set_page_config(layout="wide")
    # df = pd.read_csv("out.csv", header=None, skipinitialspace = True, quotechar = '|', names=['NTName', 'type','value','ts'])
    df = pd.read_sql_query("SELECT * FROM device_stats", conn)
    df = filter(df)
    # If you want to use feature of saving chart config, set `spec_io_mode="rw"`
    return StreamlitRenderer(df, spec="./gw_config.json", spec_io_mode="rw")

def filter(df):
    table = df.copy()

    if "event_year" in table.keys():
        yearlist = df['event_year'].unique().tolist()
        yearselector = st.segmented_control("**Year**", yearlist)

    if "event" in table.keys():
        eventlist = df['event'].unique().tolist()
        eventselector = st.segmented_control("**Event**", eventlist)

    if "match_id" in table.keys():
        match_id_list = df['match_id'].unique().tolist()
        match_id_selector = st.segmented_control("**Match**", match_id_list)

    if type(yearselector) == str:
        table.set_index('event_year', inplace=True)
        table = table.filter(like=f'{yearselector}', axis=0)

    if type(eventselector) == str:
        table.set_index('event', inplace=True)
        table = table.filter(like=f'{eventselector}', axis=0)

    if type(match_id_selector) == str:
        table.set_index('match_id', inplace=True)
        table = table.filter(like=f'{match_id_selector}', axis=0)

    if match_id_list: table.set_index('match_id', inplace=True)
    return table

renderer = get_pyg_renderer()
renderer.explorer()