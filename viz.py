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
@st.cache_resource
def get_pyg_renderer() -> "StreamlitRenderer":
    # df = pd.read_csv("out.csv", header=None, skipinitialspace = True, quotechar = '|', names=['NTName', 'type','value','ts'])
    df = pd.read_sql_query("SELECT * FROM device_stats", conn)
    # If you want to use feature of saving chart config, set `spec_io_mode="rw"`
    return StreamlitRenderer(df, spec="./gw_config.json", spec_io_mode="rw")
renderer = get_pyg_renderer()
renderer.explorer()