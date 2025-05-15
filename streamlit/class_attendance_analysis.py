import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import boto3
import time
import io
import json
import os
from pathlib import Path
import aws_connection

# Create data cache directory if it doesn't exist
CACHE_DIR = Path("./data_cache")
CACHE_DIR.mkdir(exist_ok=True)

# Function to execute Athena query with caching
@st.cache_data(ttl=3600, show_spinner=False)
def query_athena_with_cache(query, query_name):
    """
    Execute Athena query with caching to avoid re-running queries
    
    Parameters:
    - query: Athena SQL query
    - query_name: Name of the query for caching purposes
    
    Returns:
    - DataFrame: Query results
    """
    # Define cache file path
    cache_file = CACHE_DIR / f"{query_name}.csv"
    
    # Check if cache exists and is recent
    if cache_file.exists():
        try:
            return pd.read_csv(cache_file)
        except Exception as e:
            st.warning(f"Failed to load cached data: {str(e)}. Running query...")
    
    # Run the query if cache doesn't exist or is invalid
    try:
        # Create Athena client using AWS connection module
        athena_client = aws_connection.create_aws_client('athena')
        if not athena_client:
            st.error("Failed to create Athena client. Please check your AWS credentials.")
            return None
        
        # Get Athena configuration
        database, s3_output = aws_connection.get_athena_config()
        
        # Start query execution
        response = athena_client.start_query_execution(
            QueryString=query,
            QueryExecutionContext={
                'Database': database
            },
            ResultConfiguration={
                'OutputLocation': s3_output,
            }
        )
        
        # Get query execution ID
        query_execution_id = response['QueryExecutionId']
        
        with st.spinner(f"Running query: {query_name}..."):
            # Wait for query completion
            while True:
                query_status = athena_client.get_query_execution(QueryExecutionId=query_execution_id)
                status = query_status['QueryExecution']['Status']['State']
                
                if status in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
                    break
                    
                time.sleep(1)  # Wait 1 second before checking again
            
            # Check if query was successful
            if status == 'SUCCEEDED':
                # Get results
                result = athena_client.get_query_results(QueryExecutionId=query_execution_id)
                
                # Extract column names from results
                columns = [col['Label'] for col in result['ResultSet']['ResultSetMetadata']['ColumnInfo']]
                
                # Extract data rows from results
                data_rows = []
                for row in result['ResultSet']['Rows'][1:]:  # Skip header row
                    data_row = []
                    for datum in row['Data']:
                        if 'VarCharValue' in datum:
                            data_row.append(datum['VarCharValue'])
                        else:
                            data_row.append(None)
                    data_rows.append(data_row)
                
                # Create DataFrame
                df = pd.DataFrame(data_rows, columns=columns)
                
                # Save to cache
                df.to_csv(cache_file, index=False)
                
                return df
            else:
                error_message = query_status['QueryExecution']['Status'].get('StateChangeReason', 'Unknown error')
                st.error(f"Query failed: {error_message}")
                return None
    except Exception as e:
        st.error(f"Error executing query: {str(e)}")
        return None

# Function to load dummy data when Athena is not available
def load_dummy_data(query_name):
    """
    Load dummy data for development and testing.
    This version is modified to return None, effectively disabling dummy data.
    """
    # Return None for any query_name to disable dummy data
    return None

# Function to visualize top popular classes
def visualize_popular_classes(df):
    """
    Visualize top 10 most popular classes
    """
    st.subheader("Top 10 Most Popular Classes (2025)")
    
    if df is None or df.empty:
        st.info("No data available for Top 10 Popular Classes.")
        return
    
    # Ensure data types
    df['attendance_count'] = pd.to_numeric(df['attendance_count'], errors='coerce')
    
    # Sort by attendance
    df = df.sort_values('attendance_count', ascending=False).head(10)
    
    # Horizontal bar chart for better visualization of class names
    fig, ax = plt.subplots(figsize=(10, 8))
    bars = sns.barplot(y='class_name', x='attendance_count', data=df, ax=ax)
    
    # Add value labels
    for i, bar in enumerate(bars.patches):
        bars.text(
            bar.get_width() + 5,
            bar.get_y() + bar.get_height()/2,
            f'{int(df.iloc[i]["attendance_count"])}',
            ha='left', va='center', fontsize=10
        )
    
    plt.title('Top 10 Most Popular Classes (2025)')
    plt.xlabel('Attendance Count')
    plt.ylabel('Class Name')
    plt.tight_layout()
    st.pyplot(fig)
    
    # Raw data
    with st.expander("View Raw Data"):
        st.dataframe(df)
        
        # Download option
        csv = df.to_csv(index=False).encode('utf-8')
        st.download_button(
            "Download CSV",
            data=csv,
            file_name='popular_classes.csv',
            mime='text/csv',
            key="download_popular_classes"
        )

# Function to visualize popular class times
def visualize_popular_class_times(df):
    """
    Visualize most popular class times
    """
    st.subheader("Most Popular Class Times (2025)")
    
    if df is None or df.empty:
        st.info("No data available for Popular Class Times.")
        return
    
    # Ensure data types
    df['class_hour'] = pd.to_numeric(df['class_hour'], errors='coerce')
    df['total_sessions'] = pd.to_numeric(df['total_sessions'], errors='coerce')
    
    # Sort by hour for better visualization
    df = df.sort_values('class_hour')
    
    # Column chart
    fig, ax = plt.subplots(figsize=(12, 6))
    bars = sns.barplot(x='class_hour', y='total_sessions', data=df, ax=ax)
    
    # Add value labels on top of bars
    for i, bar in enumerate(bars.patches):
        bars.text(
            bar.get_x() + bar.get_width()/2.,
            bar.get_height() + 5,
            f'{int(df.iloc[i]["total_sessions"])}',
            ha='center', va='bottom', fontsize=9
        )
    
    plt.title('Most Popular Class Times (2025)')
    plt.xlabel('Hour of Day (24h format)')
    plt.ylabel('Total Sessions')
    plt.xticks(range(len(df)), [f"{int(hour)}:00" for hour in df['class_hour']])
    plt.tight_layout()
    st.pyplot(fig)
    
    # Time distribution - morning, afternoon, evening
    st.subheader("Class Time Distribution")
    
    # Define time periods
    time_periods = {
        'Early Morning (5-8)': (5, 8),
        'Morning (9-11)': (9, 11),
        'Noon (12-13)': (12, 13),
        'Afternoon (14-16)': (14, 16),
        'Evening (17-20)': (17, 20),
        'Night (21-23)': (21, 23)
    }
    
    period_data = []
    for period, (start, end) in time_periods.items():
        filtered_df = df[(df['class_hour'] >= start) & (df['class_hour'] <= end)]
        if not filtered_df.empty:
            period_sessions = filtered_df['total_sessions'].sum()
            period_data.append({'period': period, 'total_sessions': period_sessions})
    
    period_df = pd.DataFrame(period_data)
    
    fig2, ax2 = plt.subplots(figsize=(10, 6))
    plt.pie(
        period_df['total_sessions'],
        labels=period_df['period'],
        autopct='%1.1f%%',
        startangle=90,
        shadow=True
    )
    plt.title('Class Time Distribution by Period')
    plt.axis('equal')
    st.pyplot(fig2)
    
    # Raw data
    with st.expander("View Raw Data"):
        st.dataframe(df)
        
        # Download option
        csv = df.to_csv(index=False).encode('utf-8')
        st.download_button(
            "Download CSV",
            data=csv,
            file_name='popular_class_times.csv',
            mime='text/csv',
            key="download_popular_class_times"
        )

# Function to visualize popular class days
def visualize_popular_class_days(df):
    """
    Visualize most popular class days
    """
    st.subheader("Most Popular Class Days (2025)")
    
    if df is None or df.empty:
        st.info("No data available for Popular Class Days.")
        return
    
    # Ensure data types
    df['weekday'] = pd.to_numeric(df['weekday'], errors='coerce')
    df['total_sessions'] = pd.to_numeric(df['total_sessions'], errors='coerce')
    
    # Sort by weekday for better visualization
    df = df.sort_values('weekday')
    
    # Column chart
    fig, ax = plt.subplots(figsize=(12, 6))
    bars = sns.barplot(x='weekday_name', y='total_sessions', data=df, ax=ax)
    
    # Add value labels on top of bars
    for i, bar in enumerate(bars.patches):
        bars.text(
            bar.get_x() + bar.get_width()/2.,
            bar.get_height() + 5,
            f'{int(df.iloc[i]["total_sessions"])}',
            ha='center', va='bottom', fontsize=9
        )
    
    plt.title('Most Popular Class Days (2025)')
    plt.xlabel('Day of Week')
    plt.ylabel('Total Sessions')
    plt.xticks(rotation=45)
    plt.tight_layout()
    st.pyplot(fig)
    
    # Weekday vs Weekend analysis
    weekday_data = df[df['weekday'].isin([2, 3, 4, 5, 6])]
    weekend_data = df[df['weekday'].isin([1, 7])]
    
    weekday_total = weekday_data['total_sessions'].sum()
    weekend_total = weekend_data['total_sessions'].sum()
    
    period_df = pd.DataFrame({
        'period': ['Weekday (Mon-Fri)', 'Weekend (Sat-Sun)'],
        'total_sessions': [weekday_total, weekend_total]
    })
    
    fig2, ax2 = plt.subplots(figsize=(8, 8))
    plt.pie(
        period_df['total_sessions'],
        labels=period_df['period'],
        autopct='%1.1f%%',
        startangle=90,
        shadow=True,
        colors=['#3498db', '#e74c3c']
    )
    plt.title('Weekday vs Weekend Distribution')
    plt.axis('equal')
    st.pyplot(fig2)
    
    # Raw data
    with st.expander("View Raw Data"):
        st.dataframe(df)
        
        # Download option
        csv = df.to_csv(index=False).encode('utf-8')
        st.download_button(
            "Download CSV",
            data=csv,
            file_name='popular_class_days.csv',
            mime='text/csv',
            key="download_popular_class_days"
        )

# Function to visualize location attendance trends
def visualize_location_attendance(df):
    """
    Visualize location attendance trends
    """
    st.subheader("Location Attendance Trends (2025)")
    
    if df is None or df.empty:
        st.info("No data available for Location Attendance Trends.")
        return
    
    # Ensure data types
    df['month'] = pd.to_numeric(df['month'], errors='coerce')
    df['total_attendance'] = pd.to_numeric(df['total_attendance'], errors='coerce')
    df['year'] = pd.to_numeric(df['year'], errors='coerce')
    
    # Filter for year 2025
    df = df[df['year'] == 2025]
    
    # Create a pivot table for better visualization
    pivot_df = df.pivot_table(
        index='month',
        columns='location_name',
        values='total_attendance',
        aggfunc='sum'
    ).fillna(0)
    
    # Bar chart
    fig, ax = plt.subplots(figsize=(12, 8))
    pivot_df.plot(kind='bar', ax=ax)
    plt.title('Monthly Attendance by Location (2025)')
    plt.xlabel('Month')
    plt.ylabel('Total Attendance')
    plt.xticks(rotation=45)
    plt.legend(title='Location', bbox_to_anchor=(1.05, 1), loc='upper left')
    plt.tight_layout()
    st.pyplot(fig)
    
    # Location summary
    location_df = df.groupby('location_name')['total_attendance'].sum().reset_index()
    location_df = location_df.sort_values('total_attendance', ascending=False)
    
    fig3, ax3 = plt.subplots(figsize=(10, 6))
    bars = sns.barplot(x='total_attendance', y='location_name', data=location_df, ax=ax3)
    
    # Add value labels
    for i, bar in enumerate(bars.patches):
        bars.text(
            bar.get_width() + 5,
            bar.get_y() + bar.get_height()/2,
            f'{int(location_df.iloc[i]["total_attendance"])}',
            ha='left', va='center', fontsize=10
        )
    
    plt.title('Total Attendance by Location (2025)')
    plt.xlabel('Total Attendance')
    plt.ylabel('Location')
    plt.tight_layout()
    st.pyplot(fig3)
    
    # Raw data
    with st.expander("View Raw Data"):
        st.dataframe(df)
        
        # Download option
        csv = df.to_csv(index=False).encode('utf-8')
        st.download_button(
            "Download CSV",
            data=csv,
            file_name='location_attendance.csv',
            mime='text/csv',
            key="download_location_attendance"
        )

# Define SQL queries
popular_classes_query = """
-- Step 1: Historical attendance records from fact_class_attendance (confirmed sessions)
WITH attendance_old AS (
  SELECT 
    s.class_name,
    a.user_id,
    a.session_id
  FROM fact_class_attendance a
  JOIN dim_class_session s ON a.session_id = s.session_id
  WHERE s.year = 2025
),
-- Step 2: Extract latest event per user-session from fact_event (streaming data)
latest_event AS (
  SELECT 
    user_id,
    session_id,
    event_type,
    ROW_NUMBER() OVER (PARTITION BY user_id, session_id ORDER BY processed_time DESC) AS rn
  FROM fact_event
  WHERE year = '2025'
),
-- Step 3: Filter only the final status = 'signup'
valid_signup AS (
  SELECT user_id, session_id
  FROM latest_event
  WHERE rn = 1 AND event_type = 'signup'
),
-- Step 4: Match valid signups with class information
attendance_new AS (
  SELECT 
    s.class_name,
    v.user_id,
    v.session_id
  FROM valid_signup v
  JOIN dim_class_session s ON v.session_id = s.session_id
  WHERE s.year = 2025
)
-- Step 5: Merge historical and recent signups, then count attendance per class
SELECT 
  class_name,
  COUNT(DISTINCT user_id || session_id) AS attendance_count
FROM (
  SELECT * FROM attendance_old
  UNION ALL
  SELECT * FROM attendance_new
) AS combined_attendance
GROUP BY class_name
ORDER BY attendance_count DESC
LIMIT 10;
"""

popular_class_times_query = """
-- Step 1: Historical attendance from fact_class_attendance (before May 2025)
WITH attendance_old AS (
  SELECT 
    HOUR(class_datetime) AS class_hour
  FROM fact_class_attendance
  WHERE year(class_datetime) = 2025
    AND class_datetime < DATE '2025-05-01'
),

-- Step 2: Streaming event data from fact_event (April and May 2025 only)
-- Get the final event for each user-session combination, using event_ts
latest_event AS (
  SELECT 
    user_id,
    session_id,
    class_datetime,
    event_type,
    ROW_NUMBER() OVER (
      PARTITION BY user_id, session_id 
      ORDER BY event_ts DESC
    ) AS rn
  FROM fact_event
  WHERE year = '2025'
    AND month IN ('04', '05')
),

-- Step 3: Keep only users whose latest status is 'signup'
valid_event AS (
  SELECT 
    HOUR(class_datetime) AS class_hour
  FROM latest_event
  WHERE rn = 1 AND event_type = 'signup'
)

-- Step 4: Combine both datasets and count sessions by class hour
SELECT 
  class_hour,
  COUNT(*) AS total_sessions
FROM (
  SELECT * FROM attendance_old
  UNION ALL
  SELECT * FROM valid_event
) AS combined
GROUP BY class_hour
ORDER BY total_sessions DESC;
"""

popular_class_days_query = """
-- Step 1: Historical attendance from fact_class_attendance (before May 2025)
WITH attendance_old AS (
  SELECT 
    DAY_OF_WEEK(class_datetime) AS weekday
  FROM fact_class_attendance
  WHERE year(class_datetime) = 2025
    AND class_datetime < DATE '2025-05-01'
),

-- Step 2: All events from fact_event during Apr-May 2025
event_all AS (
  SELECT 
    user_id,
    session_id,
    class_datetime,
    event_type,
    event_ts,
    ROW_NUMBER() OVER (PARTITION BY user_id, session_id ORDER BY event_ts DESC) AS rn
  FROM fact_event
  WHERE year = '2025'
    AND month IN ('04', '05')
),

-- Step 3: Get latest event per user-session and filter to only 'signup'
latest_signup AS (
  SELECT 
    user_id,
    session_id,
    class_datetime,
    DAY_OF_WEEK(class_datetime) AS weekday
  FROM event_all
  WHERE rn = 1 AND event_type = 'signup'
),

-- Step 4: Filter out sessions where user had any checkin or cancel
sessions_with_other_events AS (
  SELECT DISTINCT user_id, session_id
  FROM fact_event
  WHERE year = '2025'
    AND month IN ('04', '05')
    AND event_type IN ('checkin', 'cancel')
),

-- Step 5: Valid signup sessions with no later checkin or cancel
valid_event AS (
  SELECT weekday
  FROM latest_signup ls
  LEFT JOIN sessions_with_other_events s
    ON ls.user_id = s.user_id AND ls.session_id = s.session_id
  WHERE s.user_id IS NULL
),

-- Step 6: Combine and count sessions by weekday
combined_data AS (
  SELECT * FROM attendance_old
  UNION ALL
  SELECT * FROM valid_event
)

-- Step 7: Final result with weekday name
SELECT 
  weekday,
  CASE weekday
    WHEN 1 THEN 'Sunday'
    WHEN 2 THEN 'Monday'
    WHEN 3 THEN 'Tuesday'
    WHEN 4 THEN 'Wednesday'
    WHEN 5 THEN 'Thursday'
    WHEN 6 THEN 'Friday'
    WHEN 7 THEN 'Saturday'
  END AS weekday_name,
  COUNT(*) AS total_sessions
FROM combined_data
GROUP BY weekday
ORDER BY total_sessions DESC;
"""

location_attendance_query = """
WITH attendance_old AS (
  SELECT 
    s.location_id,
    LOWER(s.city) AS city,
    LOWER(s.location_name) AS location_name,
    cs.year,
    cs.month,
    COUNT(DISTINCT a.user_id || a.session_id) AS total_attendance
  FROM fact_class_attendance a
  JOIN dim_class_session cs ON a.session_id = cs.session_id
  JOIN dim_location s ON cs.location_id = s.location_id
  WHERE cs.year = 2025
  GROUP BY s.location_id, LOWER(s.city), LOWER(s.location_name), cs.year, cs.month
),

latest_event AS (
  SELECT 
    user_id,
    session_id,
    event_type,
    ROW_NUMBER() OVER (
      PARTITION BY user_id, session_id 
      ORDER BY event_ts DESC
    ) AS rn
  FROM fact_event
  WHERE event_ts >= DATE '2025-01-01' AND event_ts < DATE '2026-01-01'
),

valid_attendance_event AS (
  SELECT user_id, session_id
  FROM latest_event
  WHERE rn = 1 AND event_type = 'signup'
),

attendance_new AS (
  SELECT 
    s.location_id,
    LOWER(s.city) AS city,
    LOWER(s.location_name) AS location_name,
    cs.year,
    cs.month,
    COUNT(DISTINCT v.user_id || v.session_id) AS total_attendance
  FROM valid_attendance_event v
  JOIN dim_class_session cs ON v.session_id = cs.session_id
  JOIN dim_location s ON cs.location_id = s.location_id
  WHERE cs.year = 2025
  GROUP BY s.location_id, LOWER(s.city), LOWER(s.location_name), cs.year, cs.month
),

all_attendance AS (
  SELECT * FROM attendance_old
  UNION ALL
  SELECT * FROM attendance_new
)

-- Final aggregation: sum total_attendance for each group
SELECT
  location_id,
  city,
  location_name,
  year,
  month,
  SUM(total_attendance) AS total_attendance
FROM all_attendance
GROUP BY location_id, city, location_name, year, month
ORDER BY year, month, total_attendance DESC;
"""

def display_attendance_dashboard():
    """
    Display the complete class attendance dashboard
    """
    st.title("Class Attendance Analysis")
    st.markdown("### Class Attendance Analysis Dashboard")
    st.write("This dashboard provides insights into class attendance patterns, popular timings, and location trends.")
    
    # Sidebar configuration
    st.sidebar.title("Dashboard Settings")
    
    # Data source is now controlled by main_dashboard.py via st.session_state['force_data_source']
    data_source = st.session_state.get('force_data_source', "AWS Athena") # Default to AWS Athena if not set

    if data_source != "AWS Athena":
        st.sidebar.error("Invalid data source configuration from main dashboard. Forcing AWS Athena.")
        data_source = "AWS Athena"
        
    st.sidebar.info(f"Data Source: {data_source}") # Display the determined data source

    # Add AWS connection status indicator
    aws_connected = aws_connection.test_aws_connection()
    if aws_connected:
        st.sidebar.success("AWS Connection: Connected ✅")
    else:
        st.sidebar.error("AWS Connection: Not Connected ❌")
        if data_source == "AWS Athena":
            st.sidebar.warning("Falling back to dummy data since AWS is not connected.")
            data_source = "Dummy Data (Development)"
    
    # Get data based on selected source
    if data_source == "AWS Athena":
        # Only attempt AWS connection if explicitly selected and available
        if aws_connected:
            # st.sidebar.success("Connected to AWS") # Already shown in main_dashboard
            
            # Get data from Athena with caching
            with st.spinner("Querying Athena for attendance data..."):
                popular_classes = query_athena_with_cache(popular_classes_query, "popular_classes")
                popular_class_times = query_athena_with_cache(popular_class_times_query, "popular_class_times")
                popular_class_days = query_athena_with_cache(popular_class_days_query, "popular_class_days")
                location_attendance = query_athena_with_cache(location_attendance_query, "location_attendance")
                
                # Display data source indicator
                st.sidebar.success("Using real data from AWS Athena")
        else:
            st.sidebar.error("AWS connection failed. No data will be loaded for Class Attendance.")
            popular_classes = None # Ensure data is None if AWS connection fails
            popular_class_times = None
            popular_class_days = None
            location_attendance = None
            
            # Display data source indicator
            # st.sidebar.warning("Using dummy data due to AWS connection failure") # No dummy data fallback
    else:
        # This block should ideally not be reached if main_dashboard correctly sets force_data_source
        st.sidebar.error(f"Unexpected data source: {data_source}. Defaulting to no data.")
        popular_classes = None
        popular_class_times = None
        popular_class_days = None
        location_attendance = None
        
        # Display data source indicator
        # st.sidebar.info("Using dummy development data") # No dummy data
    
    # Create tabs for different visualizations
    tab1, tab2, tab3, tab4 = st.tabs([
        "Popular Classes", 
        "Class Times", 
        "Class Days",
        "Location Trends"
    ])
    
    with tab1:
        if popular_classes is not None:
            visualize_popular_classes(popular_classes)
        else:
            visualize_popular_classes(None)

    with tab2:
        if popular_class_times is not None:
            visualize_popular_class_times(popular_class_times)
        else:
            visualize_popular_class_times(None)

    with tab3:
        if popular_class_days is not None:
            visualize_popular_class_days(popular_class_days)
        else:
            visualize_popular_class_days(None)

    with tab4:
        if location_attendance is not None:
            visualize_location_attendance(location_attendance)
        else:
            visualize_location_attendance(None)
    
    # Refresh button
    if st.sidebar.button("Refresh Data", key="refresh_data_attendance"):
        # Clear cache and reload
        st.cache_data.clear()
        st.rerun()
    
    # Cache information
    st.sidebar.info(
        "Data is cached for 1 hour to improve performance. "
        "Click 'Refresh Data' to fetch the latest data from the source."
    )

# Run the app if this file is executed directly
if __name__ == "__main__":
    display_attendance_dashboard() 