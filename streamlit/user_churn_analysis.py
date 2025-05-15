import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import aws_connection # For AWS client creation and config
import time # For query polling
from pathlib import Path # For cache directory

# Create data cache directory if it doesn't exist
CACHE_DIR = Path("./data_cache")
CACHE_DIR.mkdir(exist_ok=True)

# Function to execute Athena query with caching (specific to this module)
@st.cache_data(ttl=3600, show_spinner=False)
def query_athena_with_cache(query, query_name):
    """
    Execute Athena query with caching to avoid re-running queries.
    Results are cached in a local CSV file.
    """
    cache_file = CACHE_DIR / f"{query_name}.csv"
    
    if cache_file.exists():
        try:
            return pd.read_csv(cache_file)
        except Exception as e:
            st.warning(f"Failed to load cached data for {query_name}: {str(e)}. Re-querying...")

    try:
        athena_client = aws_connection.create_aws_client('athena')
        if not athena_client:
            st.error("Failed to create Athena client. Check AWS credentials.")
            return None
        
        database, s3_output = aws_connection.get_athena_config()
        if not database or not s3_output:
            st.error("Athena database or S3 output location is not configured.")
            return None

        response = athena_client.start_query_execution(
            QueryString=query,
            QueryExecutionContext={'Database': database},
            ResultConfiguration={'OutputLocation': s3_output}
        )
        query_execution_id = response['QueryExecutionId']
        
        # No st.spinner here as @st.cache_data handles it with show_spinner=True by default if not specified
        # Or if we want custom spinner text, it should be outside this cached function.
        while True:
            query_status = athena_client.get_query_execution(QueryExecutionId=query_execution_id)
            status = query_status['QueryExecution']['Status']['State']
            if status in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
                break
            time.sleep(1)

        if status == 'SUCCEEDED':
            result = athena_client.get_query_results(QueryExecutionId=query_execution_id)
            columns = [col['Label'] for col in result['ResultSet']['ResultSetMetadata']['ColumnInfo']]
            data_rows = []
            for row in result['ResultSet']['Rows'][1:]: # Skip header
                data_rows.append([datum.get('VarCharValue') for datum in row['Data']])
            
            df = pd.DataFrame(data_rows, columns=columns)
            df.to_csv(cache_file, index=False)
            return df
        else:
            error_message = query_status['QueryExecution']['Status'].get('StateChangeReason', 'Unknown error')
            st.error(f"Athena query ({query_name}) failed: {error_message}")
            return None
    except Exception as e:
        st.error(f"Error executing Athena query ({query_name}): {str(e)}")
        return None

# SQL Query for Churn Analysis
churn_analysis_query = """
-- Step 1: Get the latest event per user-session pair
WITH latest_event AS (
  SELECT 
    user_id,
    session_id,
    event_ts,
    event_type,
    ROW_NUMBER() OVER (PARTITION BY user_id, session_id ORDER BY event_ts DESC) AS rn
  FROM peak_fitness_analytics.fact_event
  WHERE event_type IN ('signup', 'checkin', 'cancel')
),
-- Step 2: Only keep sessions where the last event is 'signup'
valid_signup_session AS (
  SELECT 
    user_id,
    session_id,
    event_ts AS signup_date
  FROM latest_event
  WHERE rn = 1 AND event_type = 'signup'
),
-- Step 3: Combine with confirmed class attendance records
all_attendance_union AS (
  SELECT user_id, session_id, class_datetime AS activity_time
  FROM peak_fitness_analytics.fact_class_attendance
  UNION
  SELECT user_id, session_id, CAST(signup_date AS timestamp) AS activity_time
  FROM valid_signup_session
),
-- Step 4: Find latest activity per user
last_activity_per_user AS (
  SELECT user_id, MAX(activity_time) AS last_activity_time
  FROM all_attendance_union
  GROUP BY user_id
),
-- Step 5: Identify users inactive for more than 90 days
churned_users AS (
  SELECT 
    user_id,
    last_activity_time,
    DATE_DIFF('day', last_activity_time, CURRENT_DATE) AS days_inactive
  FROM last_activity_per_user
  WHERE DATE_DIFF('day', last_activity_time, CURRENT_DATE) > 90
)
-- Step 6: Output with optional user info from dim_user_v3
SELECT 
  c.user_id,
  d.email,
  c.last_activity_time,
  c.days_inactive
FROM churned_users c
LEFT JOIN peak_fitness_analytics.dim_user_v3 d
  ON c.user_id = d.user_id
ORDER BY c.days_inactive DESC;
"""

def display_churn_dashboard():
    """
    Display the User Churn Analysis dashboard.
    """
    st.title("User Activity & Churn Analysis")
    st.markdown("### Identifying Users Inactive for Over 90 Days")

    data_source = st.session_state.get('force_data_source', "AWS Athena") 
    st.sidebar.info(f"Data Source: {data_source}")

    aws_connected = aws_connection.test_aws_connection()
    churn_data = None

    if data_source == "AWS Athena":
        if aws_connected:
            # Use the local query_athena_with_cache function
            with st.spinner("Querying Athena for churn data..."): # Spinner outside cached function
                 churn_data = query_athena_with_cache(churn_analysis_query, "churn_analysis_data")
        else:
            st.error("AWS connection failed. Cannot load churn data.")
    else:
        st.error(f"Unsupported data source: {data_source} for churn analysis.")

    if churn_data is not None and not churn_data.empty:
        st.subheader("Churned User Report")
        
        total_churned_users = len(churn_data)
        st.metric(label="Total Churned Users (>90 days inactive)", value=total_churned_users)

        st.dataframe(churn_data)

        csv = churn_data.to_csv(index=False).encode('utf-8')
        st.download_button(
            label="Download Churned Users List (CSV)",
            data=csv,
            file_name="churned_users_report.csv",
            mime="text/csv",
            key="download_churn_users"
        )
        
        if 'days_inactive' in churn_data.columns:
            try:
                churn_data['days_inactive'] = pd.to_numeric(churn_data['days_inactive'])
                st.subheader("Distribution of Inactive Days")
                fig, ax = plt.subplots()
                sns.histplot(churn_data['days_inactive'], bins=10, kde=False, ax=ax)
                ax.set_xlabel("Days Inactive")
                ax.set_ylabel("Number of Users")
                ax.set_title("Distribution of Inactivity Periods for Churned Users")
                st.pyplot(fig)
            except Exception as e:
                st.warning(f"Could not generate inactivity distribution plot: {e}")

    elif churn_data is not None and churn_data.empty:
        st.info("No users found matching the churn criteria (inactive > 90 days).")
    else:
        st.info("Churn data could not be loaded.") # Covers various failure reasons

    # Refresh button logic might need adjustment based on how @st.cache_data clears.
    # For @st.cache_data, clearing the specific function's cache is done by query_athena_with_cache.clear()
    if st.sidebar.button("Refresh Churn Data", key="refresh_churn_data_module"): # Changed key for uniqueness
        try:
            query_athena_with_cache.clear() # Clear cache for this module's function
        except Exception as e:
            st.warning(f"Could not clear churn cache: {e}. Try a full page refresh or clearing all app cache.")
            # st.cache_data.clear() # This would clear cache for all modules
        st.rerun()

if __name__ == "__main__":
    if 'force_data_source' not in st.session_state:
        st.session_state['force_data_source'] = "AWS Athena"
    display_churn_dashboard() 