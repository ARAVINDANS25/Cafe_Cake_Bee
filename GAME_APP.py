import streamlit as st
import pandas as pd
from snowflake.connector import connect

# Snowflake connection parameters
snowflake_conn_params = {
    'user': 'ARAVINDANS',
    'password': 'thedubbelH@25',
    'account': 'ZHUQIOD.BD61920',
    'warehouse': 'COMPUTE_WH',
    'database': 'AGS_GAME_AUDIENCE',
    'schema': 'ENHANCED'
}

# Function to run query and return results as a DataFrame
def run_query(query):
    conn = connect(**snowflake_conn_params)
    cursor = conn.cursor()
    cursor.execute(query)
    result = cursor.fetch_pandas_all()
    cursor.close()
    conn.close()
    return result

# Streamlit layout
st.title('AGS Game Audience Dashboard')

# Sidebar for navigation
st.sidebar.title('Navigation')
pages = ['Overview', 'Game Events', 'User Analysis']
selected_page = st.sidebar.selectbox('Select a page:', pages)

# Page: Overview
if selected_page == 'Overview':
    st.header('Overview')
    st.write('This dashboard provides insights into the game audience logs.')
    
    # Display current time and timezone information
    timezone_query = "SHOW PARAMETERS LIKE 'timezone'"
    timezone_info = run_query(timezone_query)
    st.write('Current Timezone Information:')
    st.dataframe(timezone_info)
    
    current_time_query = "SELECT CURRENT_TIMESTAMP()"
    current_time = run_query(current_time_query)
    st.write('Current Timestamp:')
    st.dataframe(current_time)

# Page: Game Events
elif selected_page == 'Game Events':
    st.header('Game Events')
    
    # Fetch game logs
    game_logs_query = "SELECT * FROM AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED"
    game_logs = run_query(game_logs_query)
    
    st.write('Game Logs:')
    st.dataframe(game_logs)

    # Plotting game events over time
    st.subheader('Game Events Over Time')
    game_events_over_time = game_logs[['GAME_EVENT_UTC', 'GAME_EVENT_NAME']].groupby('GAME_EVENT_UTC').count().reset_index()
    fig = px.line(game_events_over_time, x='GAME_EVENT_UTC', y='GAME_EVENT_NAME', title='Game Events Over Time')
    st.plotly_chart(fig)

# Page: User Analysis
elif selected_page == 'User Analysis':
    st.header('User Analysis')
    
    # Fetch user details
    user_details_query = """
    WITH log_details AS (
        SELECT 
            logs.RAW_LOG:ip_address::TEXT AS ip_address,
            logs.RAW_LOG:user_login::STRING AS GAMER_NAME,
            logs.RAW_LOG:user_event::STRING AS GAME_EVENT_NAME,
            logs.RAW_LOG:datetime_iso8601::TIMESTAMP_NTZ AS GAME_EVENT_UTC,
            loc.city,
            loc.region,
            loc.country,
            loc.timezone AS GAMER_LTZ_NAME,
            CONVERT_TIMEZONE('UTC', loc.timezone, logs.RAW_LOG:datetime_iso8601::TIMESTAMP_NTZ) AS GAME_EVENT_LTZ,
            DAYNAME(CONVERT_TIMEZONE('UTC', loc.timezone, logs.RAW_LOG:datetime_iso8601::TIMESTAMP_NTZ)) AS DOW_NAME,
            EXTRACT(HOUR FROM CONVERT_TIMEZONE('UTC', loc.timezone, logs.RAW_LOG:datetime_iso8601::TIMESTAMP_NTZ)) AS event_hour
        FROM 
            AGS_GAME_AUDIENCE.RAW.GAME_LOGS logs
        JOIN 
            IPINFO_GEOLOC.demo.location loc 
        ON 
            IPINFO_GEOLOC.public.TO_JOIN_KEY(logs.RAW_LOG:ip_address::TEXT) = loc.join_key
            AND IPINFO_GEOLOC.public.TO_INT(logs.RAW_LOG:ip_address::TEXT) 
            BETWEEN loc.start_ip_int AND loc.end_ip_int
    )
    SELECT
        ld.ip_address,
        ld.GAMER_NAME,
        ld.GAME_EVENT_NAME,
        ld.GAME_EVENT_UTC,
        ld.city,
        ld.region,
        ld.country,
        ld.GAMER_LTZ_NAME,
        ld.GAME_EVENT_LTZ,
        ld.DOW_NAME,
        tod.tod_name
    FROM
        log_details ld
    JOIN
        ags_game_audience.raw.time_of_day_lu tod
    ON
        ld.event_hour = tod.hour;
    """
    user_details = run_query(user_details_query)
    
    st.write('User Details:')
    st.dataframe(user_details)

    # Plotting user activity by region
    st.subheader('User Activity by Region')
    user_activity_by_region = user_details[['region', 'GAME_EVENT_NAME']].groupby('region').count().reset_index()
    fig = px.bar(user_activity_by_region, x='region', y='GAME_EVENT_NAME', title='User Activity by Region')
    st.plotly_chart(fig)

# Run the Streamlit app
if __name__ == '__main__':
    st.run()
