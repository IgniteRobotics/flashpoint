from pygwalker.api.streamlit import StreamlitRenderer
import pandas as pd
import streamlit as st
import os
import platform
from sqlite3 import connect 
title = 'Firematics Robot Telemetry'
conn = connect("db/robot.db")

# Adjust the width of the Streamlit page
st.set_page_config(
    page_title=title,
    layout="wide",
    page_icon = ":material/edit:"
)

# Add Title
st.title(title)

df = None

def init_dataframe():
    global df
    df = pd.read_sql_query("SELECT * FROM device_stats", conn)

# You should cache your pygwalker renderer, if you don't want your memory to explode
@st.cache_resource
def get_pyg_renderer() -> "StreamlitRenderer":
    #df = filter(df)
    # If you want to use feature of saving chart config, set `spec_io_mode="rw"`
    return StreamlitRenderer(df, spec="./gw_config.json", spec_io_mode="rw")

init_dataframe()

table = df
keys = table.keys()

st.sidebar.header("Filters")
global yearselector, eventselector, match_id_selector
yearselector, eventselector, match_id_selector= None, None, None
years, events, matches = df['event_year'], df['event'], df['match_id']

if "event_year" in keys:
    yearlist = df['event_year'].unique().tolist()
    yearselector = st.sidebar.segmented_control("**Year**", yearlist)
    if isinstance(yearselector, str):
        table = table[table['event_year'].isin([yearselector])]

if "event" in keys:
    eventlist = table['event'].unique().tolist() if yearselector is None else table[table['event_year'] == yearselector]['event'].unique().tolist()
    eventselector = st.sidebar.segmented_control("**Event**", eventlist)
    if isinstance(eventselector, str):
        table = table[table['event'].isin([eventselector])]

if "match_id" in keys:
    match_id_list = table['match_id'].unique().tolist() if eventselector is None else table[table['event'] == eventselector]['match_id'].unique().tolist()
    match_id_selector = st.sidebar.segmented_control("**Match**", match_id_list)
    if isinstance(match_id_selector, str):
        table = table[table['match_id'].isin([match_id_selector])]    

df = table
renderer = get_pyg_renderer()

st.sidebar.header("AdvantageScope")

allfiles = os.listdir(".")
telemetryfiles = {}
for root, dirs, files in os.walk("."):
    for filename in files:
            if not eventselector == None:
                if "wpilog" in filename and eventselector in filename:
                    telemetryfiles[filename] = root
            else:
                if "wpilog" in filename:
                    telemetryfiles[filename] = root

openfile = st.sidebar.selectbox("File to open", telemetryfiles)
filepath = os.getcwd()+"/"+telemetryfiles[openfile]+"/"+openfile
filepath = filepath.replace("!", "\\!").replace("./", "")

if st.sidebar.button("Open in AdvantageScope", type="primary"):
    if platform.system() == "Windows":
        os.system("open \"~/wpilib/2025/advantagescope/AdvantageScope (WPILib).exe\"")
    elif platform.system() == "Linux":
        os.system("open \"~/wpilib/2025/advantagescope/AdvantageScope (WPILib).AppImage\"")
    elif platform.system() == "Darwin":
        print(filepath)
        if os.path.exists(filepath.replace("\\!", "!")):
            os.system("open \"/Users/$USER/wpilib/2025/advantagescope/AdvantageScope (WPILib).app\" --args "+filepath)

if st.button("Refresh"):
    renderer = get_pyg_renderer()

renderer.explorer()

page1 = st.Page("display_scripts/display_vision_stats.py", title="Table 1")
page2 = st.Page("display_scripts/display_nothing.py", title="Table 2")
pg = st.navigation([page1, page2])
pg.run()
