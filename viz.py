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

page1 = st.Page("display_scripts/display_vision_stats.py", title="Table 1")
page2 = st.Page("display_scripts/display_nothing.py", title="Table 2")
pg = st.navigation([page1, page2])
st.set_page_config(page_title = "Data Manager", page_icon = ":material/edit:")
pg.run()