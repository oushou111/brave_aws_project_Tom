import streamlit as st
import pandas as pd
import aws_connection # For create_aws_client and get_athena_config
import athena_queries # For get_query
from pathlib import Path
import time # Needed for query_athena_with_cache

# Create data cache directory if it doesn't exist (COPIED FROM user_demographics.py)
CACHE_DIR = Path("./data_cache")
CACHE_DIR.mkdir(exist_ok=True)

# Function to execute Athena query with caching (COPIED FROM user_demographics.py)
# This function is now local to leaderboard_feature.py to resolve the AttributeError
@st.cache_data(ttl=3600, show_spinner=False)
def query_athena_with_cache(query, query_name):
    """
    Execute Athena query with caching to avoid re-running queries
    
    Parameters:
    - query: Athena SQL query
    - query_name: Name of the query for caching purposes
    
    Returns:
    - DataFrame: Query results, or an empty DataFrame on error
    """
    cache_file = CACHE_DIR / f"{query_name.replace(' ', '_').lower()}.csv"
    
    if cache_file.exists():
        try:
            return pd.read_csv(cache_file)
        except Exception as e:
            st.warning(f"Failed to load cached data for '{query_name}' from {cache_file}: {str(e)}. Running query...")
    
    try:
        athena_client = aws_connection.create_aws_client('athena')
        if not athena_client:
            # st.error is called within create_aws_client if it fails
            return pd.DataFrame() 
        
        database, s3_output = aws_connection.get_athena_config()
        if not (database and s3_output and s3_output.startswith("s3://")):
            st.error(f"Athena database ('{database}') or S3 output ('{s3_output}') is misconfigured. Cannot run '{query_name}'.")
            return pd.DataFrame()

        response = athena_client.start_query_execution(
            QueryString=query,
            QueryExecutionContext={'Database': database},
            ResultConfiguration={'OutputLocation': s3_output}
        )
        query_execution_id = response['QueryExecutionId']
        
        with st.spinner(f"⏳ Running Athena query for '{query_name}'..."):
            while True:
                query_status_data = athena_client.get_query_execution(QueryExecutionId=query_execution_id)
                status = query_status_data['QueryExecution']['Status']['State']
                if status in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
                    break
                time.sleep(1)
            
            if status == 'SUCCEEDED':
                results_paginator = athena_client.get_paginator('get_query_results')
                query_results_iter = results_paginator.paginate(QueryExecutionId=query_execution_id)
                all_rows = []
                columns = None
                for page in query_results_iter:
                    if columns is None and page['ResultSet']['ResultSetMetadata']['ColumnInfo']:
                        columns = [col_info['Label'] for col_info in page['ResultSet']['ResultSetMetadata']['ColumnInfo']]
                    page_rows_data = page['ResultSet']['Rows']
                    if columns and page_rows_data and page_rows_data[0]['Data'] and all(page_rows_data[0]['Data'][i].get('VarCharValue') == columns[i] for i in range(len(columns))):
                        page_rows_data = page_rows_data[1:] # Skip header row if present
                    for row_data in page_rows_data:
                        all_rows.append([item.get('VarCharValue') for item in row_data['Data']])
                
                df = pd.DataFrame(all_rows, columns=columns if columns else [])
                if not df.empty:
                    df.to_csv(cache_file, index=False)
                return df
            else:
                error_message = query_status_data['QueryExecution']['Status'].get('StateChangeReason', 'Unknown error')
                st.error(f"Athena query '{query_name}' {status}. Reason: {error_message}")
                return pd.DataFrame()
    except Exception as e:
        st.error(f"Exception during Athena query '{query_name}': {str(e)}")
        return pd.DataFrame()

# Cache for data fetching (using the local query_athena_with_cache)
@st.cache_data(ttl=3600) # Streamlit cache for fetch_leaderboard_data itself
def fetch_leaderboard_data():
    """
    Fetches leaderboard data from Athena using the local query_athena_with_cache function.
    """
    query = athena_queries.get_query("Leaderboard Queries", "User Class Engagement Leaderboard (April 2025)")
    if not query:
        st.error("Leaderboard query 'User Class Engagement Leaderboard (April 2025)' not found. Check athena_queries.py.")
        return pd.DataFrame()

    # Call the local query_athena_with_cache function
    # Updated query_name for caching to be more specific
    df = query_athena_with_cache(query, "leaderboard_april_2025_data") 
    
    if df.empty:
        # query_athena_with_cache already shows errors/info if data is empty or query fails
        # st.info("No leaderboard data available currently or query failed.")
        pass # Let query_athena_with_cache handle messages for empty/failed states
            
    return df

def display_leaderboard():
    """
    Displays the user engagement leaderboard.
    """
    st.subheader("🏆 User Class Engagement Leaderboard (April 2025)")

    leaderboard_df = fetch_leaderboard_data()

    if not leaderboard_df.empty:
        leaderboard_df_display = leaderboard_df.copy()
        leaderboard_df_display.insert(0, 'Rank', range(1, len(leaderboard_df_display) + 1))
        leaderboard_df_display = leaderboard_df_display.rename(columns={
            'first_name': 'First Name',
            'last_name': 'Last Name',
            'total_classes_attended': 'Classes Attended'
        })
        columns_to_display = ['Rank', 'First Name', 'Last Name', 'Classes Attended']
        st.dataframe(
            leaderboard_df_display[columns_to_display],
            hide_index=True,
            use_container_width=True
        )
    else:
        st.info("Leaderboard is currently empty or data could not be fetched.") # Fallback message if df is still empty

if __name__ == '__main__':
    st.set_page_config(layout="wide")
    st.title("Leaderboard Test Page")
    display_leaderboard() 