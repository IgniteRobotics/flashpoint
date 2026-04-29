from pygwalker.api.streamlit import StreamlitRenderer
import pandas as pd
import streamlit as st
from sqlite3 import connect 

st.title("Vision Statistics Table")
conn = connect("db/robot.db")
df = pd.read_sql_query("SELECT * FROM vision_stats", conn)
df.drop(columns=['event_year','event','match_id','match_type','replay_num'], inplace=True)
df.set_index('camera',inplace=True)
df = df.astype(int)
st.table(df)
conn.close()
