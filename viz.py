from pygwalker.api.streamlit import StreamlitRenderer
import pandas as pd
import streamlit as st
import os
import platform
from sqlite3 import connect 

# Constants
TITLE = 'Firematics Robot Telemetry'
DB_PATH = "db/robot.db"
GW_CONFIG_PATH = "./gw_config.json"
selected_table = "device_stats"

# Streamlit page configuration
st.set_page_config(
    page_title=TITLE,
    layout="wide",
    page_icon=":material/edit:"
)

# Add Title
st.title(TITLE)

# Global variables
yearselector, eventselector, match_id_selector = None, None, None

def init_dataframe():
    """Initialize the dataframe from the database."""
    # Database connection
    conn = connect(DB_PATH)
    df = pd.read_sql_query("SELECT * FROM "+selected_table, conn)
    st.session_state.init_df = df.astype({'match_id': int, 'replay_num': int})
    conn.close()

@st.cache_resource
def get_pyg_renderer() -> "StreamlitRenderer":
    """Cache the pygwalker renderer."""
    if st.session_state.filtered_df.empty:
        st.error("No data to display.")
        st.stop()
    return StreamlitRenderer(st.session_state.filtered_df, spec=GW_CONFIG_PATH, spec_io_mode="rw")

def filter_dataframe():
    """Setup sidebar filters for year, event, and match."""
    global selected_table
    df = st.session_state.init_df.copy()
    keys = df.keys()

    st.sidebar.header("Filters")

    if "event_year" in keys:
        yearlist = df['event_year'].unique().tolist()
        yearselector = st.sidebar.segmented_control("Year", yearlist, key = "sidebar_year_selector")
        if(yearselector is not None):
            df = df[df['event_year'].isin([yearselector])]

    if "event" in keys:
        eventlist = df['event'].unique().tolist()
        eventselector = st.sidebar.segmented_control("Event", eventlist, key="sidebar_event_selector")
        if(eventselector is not None):
            df = df[df['event'].isin([eventselector])]

    if "match_id" in keys:
        match_id_list = df['match_id'].unique().tolist()
        match_id_selector = st.sidebar.segmented_control("Match", match_id_list, key="sidebar_match_selector")
        if(match_id_selector is not None):  
            df = df[df['match_id'].isin([match_id_selector])]
    
    st.session_state.filtered_df = df

def setup_advantagescope():
    """Setup AdvantageScope file selection and opening."""
    st.sidebar.header("AdvantageScope")
    telemetryfiles = {
        filename: root
        for root, _, files in os.walk(".")
        for filename in files
        if "wpilog" in filename and (eventselector in filename if eventselector else True)
    }

    openfile = st.sidebar.selectbox("File to open", telemetryfiles)
    filepath = os.path.join(os.getcwd(), telemetryfiles[openfile], openfile).replace("!", "\\!").replace("./", "").replace(".\\", "")
    
    windows_exe_filepath = "\"C:/Users/Public/wpilib/2025/advantagescope/AdvantageScope (WPILib).exe\""
    linux_AppImage_filepath = "\"~/wpilib/2025/advantagescope/advantagescope-wpilib\""
    darwin_app_filepath = "\"/Users/$USER/wpilib/2025/advantagescope/AdvantageScope (WPILib).app\""
    
    if st.sidebar.button("Open in AdvantageScope", type="primary"):
        if platform.system() == "Windows":
            os.system(f"start \"\" {windows_exe_filepath} \"{filepath}\"")
        elif platform.system() == "Linux":
            os.system(f"exec {linux_AppImage_filepath} {filepath}")#placeholder for linux 
        elif platform.system() == "Darwin" and os.path.exists(filepath.replace("\\!", "!")):
            os.system(f"open {darwin_app_filepath} --args {filepath}")

global renderer

refresh_button = st.button("Refresh")
if refresh_button:
    init_dataframe()
    get_pyg_renderer.clear()

if('init_df' not in st.session_state):
    init_dataframe()

filter_dataframe()

setup_advantagescope()
renderer = get_pyg_renderer()
renderer.explorer()

