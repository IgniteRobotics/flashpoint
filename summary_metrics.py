import os
import sys
from sqlite3 import connect 
from datetime import datetime



def setup_db(connection):

    cursor = connection.cursor()

     # Add the new electrical_summaries table
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS electrical_summaries (
        event TEXT,
        match_id REAL,
        subsystem TEXT,
        component TEXT,
        part TEXT,
        type TEXT,
        metric TEXT,
        max_abs_value REAL,
        min_abs_value REAL,
        avg_abs_value REAL,
        UNIQUE(event, match_id, subsystem, component, part, type, metric)
    )
    ''')

    # Add useful indexes for the new table
    cursor.execute('CREATE INDEX IF NOT EXISTS electrical_summaries_idx_event ON electrical_summaries (event)')
    cursor.execute('CREATE INDEX IF NOT EXISTS electrical_summaries_idx_match ON electrical_summaries (event, match_id)')
    cursor.execute('CREATE INDEX IF NOT EXISTS electrical_summaries_idx_component ON electrical_summaries (component)')
    cursor.execute('CREATE INDEX IF NOT EXISTS electrical_summaries_idx_metric ON electrical_summaries (component, metric)')
    
    connection.commit()



def summarize_electrical_metrics(connection):
    cursor = connection.cursor()

    # Query to get the electrical metrics
    cursor.execute('''
    INSERT OR REPLACE INTO electrical_summaries 
    SELECT 
        event,
        match_id,
        subsystem,
        component,
        part,
        type,
        metric,
        MAX(ABS(CAST(numeric_value AS REAL))) as max_abs_value,
        MIN(ABS(CAST(numeric_value AS REAL))) as min_abs_value,
        AVG(ABS(CAST(numeric_value AS REAL))) as avg_abs_value
    FROM metrics
    WHERE data_type = 'double' 
    AND abs(value) >  0.05
    AND metric in ('CURRENT','TEMP','VOLTAGE')
    GROUP BY event, match_id, subsystem, component, part, type, metric
    ''')

    connection.commit()


if __name__ == "__main__":
    connection = connect("db/metrics.db")
    setup_db(connection)
    summarize_electrical_metrics(connection)
