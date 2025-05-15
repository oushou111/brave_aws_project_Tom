"""
AWS Connection Configuration Module

Provides configuration and functionality for connecting to AWS services
"""

import boto3
import os
import streamlit as st
from dotenv import load_dotenv
from pathlib import Path
import pandas as pd
import time

# Try to load environment variables from .env file
load_dotenv()
print(f"DEBUG: .env loaded. AWS_ACCESS_KEY_ID from env: {os.environ.get('AWS_ACCESS_KEY_ID')}")

def get_aws_credentials():
    """
    Get AWS credentials with the following priority:
    1. Streamlit secrets
    2. Environment variables
    3. User input
    """
    # Added for debugging
    print(f"DEBUG: get_aws_credentials called. AWS_ACCESS_KEY_ID from env: {os.environ.get('AWS_ACCESS_KEY_ID')}, Secret Key from env: {'******' if os.environ.get('AWS_SECRET_ACCESS_KEY') else None}")

    # Check if credentials are stored in session state
    if 'aws_credentials' in st.session_state:
        return st.session_state['aws_credentials']
    
    # Try to get credentials from various sources
    access_key = None
    secret_key = None
    region = None
    
    # Get from environment variables
    access_key = os.environ.get('AWS_ACCESS_KEY_ID')
    secret_key = os.environ.get('AWS_SECRET_ACCESS_KEY')
    region = os.environ.get('AWS_REGION', 'us-east-1')
        
    # If not found in environment variables, prompt user for input
    if not (access_key and secret_key):
        st.sidebar.warning("AWS credentials not found. Please provide the following information:")
        with st.sidebar.expander("AWS Credentials Configuration", expanded=True):
            access_key = st.text_input("AWS Access Key ID", 
                                      value=access_key if access_key else "", 
                                      type="password")
            secret_key = st.text_input("AWS Secret Access Key", 
                                      value=secret_key if secret_key else "", 
                                      type="password")
            region = st.text_input("AWS Region", 
                                  value=region if region else "us-east-1")
            
            if st.button("Save Credentials"):
                if access_key and secret_key:
                    st.session_state['aws_credentials'] = {
                        'access_key': access_key,
                        'secret_key': secret_key,
                        'region': region
                    }
                    st.success("AWS credentials saved!")
                    # Page needs to be refreshed to use the newly saved credentials
                    st.rerun()
                else:
                    st.error("Please provide Access Key and Secret Key")
    
    # Return credentials
    return {
        'access_key': access_key,
        'secret_key': secret_key,
        'region': region
    }

def create_aws_client(service_name):
    """
    Create AWS service client
    
    Parameters:
    - service_name: AWS service name, such as 's3', 'athena', etc.
    
    Returns:
    - Created AWS client, or None if credentials are invalid
    """
    # Get AWS credentials
    credentials = get_aws_credentials()
    
    # Check if credentials are complete
    if not (credentials.get('access_key') and credentials.get('secret_key')):
        st.warning("AWS credentials are incomplete. Please configure in the sidebar.")
        return None
    
    try:
        # Create client
        client = boto3.client(
            service_name,
            aws_access_key_id=credentials['access_key'],
            aws_secret_access_key=credentials['secret_key'],
            region_name=credentials['region']
        )
        return client
    except Exception as e:
        st.error(f"Failed to create AWS client: {str(e)}")
        return None

def test_aws_connection():
    """
    Test if AWS connection is available
    
    Returns:
    - bool: Whether the connection is successful
    """
    s3_client = create_aws_client('s3')
    if not s3_client:
        return False
    
    try:
        # Try to list S3 buckets
        s3_client.list_buckets()
        return True
    except Exception as e:
        st.error(f"AWS connection test failed: {str(e)}")
        return False

def get_athena_config():
    """
    Get Athena database and S3 output location configuration
    
    Returns:
    - tuple: (database_name, s3_output_location)
    """
    # Try to get from session state first
    if 'athena_config' in st.session_state:
        return st.session_state['athena_config']
        
    # Default values - these should be updated with your actual Athena configuration
    database = 'peak_fitness_analytics'
    s3_output = 's3://peak-fitness-results-bucket/athena-query-results/'
    
    # Get from environment variables if available
    env_database = os.environ.get('ATHENA_DATABASE')
    env_s3_output = os.environ.get('ATHENA_S3_OUTPUT')
    
    if env_database:
        database = env_database
    if env_s3_output:
        s3_output = env_s3_output
    
    # If not found, prompt user for configuration
    if not st.session_state.get('athena_config_prompted', False):
        with st.sidebar.expander("Athena Configuration", expanded=False):
            st.session_state['athena_config_prompted'] = True
            user_database = st.text_input("Athena Database Name", value=database)
            user_s3_output = st.text_input("S3 Output Location", value=s3_output)
            
            if st.button("Save Athena Configuration"):
                database = user_database
                s3_output = user_s3_output
                st.session_state['athena_config'] = (database, s3_output)
                st.success("Athena configuration saved!")
                st.rerun()
    
    # Store in session state
    st.session_state['athena_config'] = (database, s3_output)
    
    return (database, s3_output)

# Usage example
if __name__ == "__main__":
    if test_aws_connection():
        print("AWS connection test successful!")
    else:
        print("AWS connection test failed!")

# Create data cache directory if it doesn't exist
CACHE_DIR = Path("./data_cache")
CACHE_DIR.mkdir(exist_ok=True)

# Function to execute Athena query with caching
@st.cache_data(ttl=3600, show_spinner=False)
def query_athena_with_cache(query: str, query_name: str):
    """
    Execute Athena query with both Streamlit's in-memory caching (@st.cache_data)
    and a manual file-based cache.

    Parameters:
    - query: Athena SQL query string.
    - query_name: A unique name for the query, used for naming the cache file.

    Returns:
    - pd.DataFrame: Query results as a Pandas DataFrame, or None if an error occurs.
    """
    # Define cache file path using the provided query_name
    cache_file = CACHE_DIR / f"{query_name.replace(' ', '_').lower()}.csv"

    # Check if file cache exists and is recent (Streamlit's @st.cache_data handles TTL for in-memory)
    # For file cache, we'll rely on Streamlit's caching for re-computation avoidance mostly.
    # The file cache here acts more as a persistent store across app restarts if @st.cache_data doesn't cover that.
    if cache_file.exists():
        try:
            # st.info(f"Loading data for '{query_name}' from file cache: {cache_file}")
            return pd.read_csv(cache_file)
        except Exception as e:
            st.warning(f"Failed to load cached data from {cache_file}: {str(e)}. Running query...")

    # Run the query if cache doesn't exist or is invalid
    try:
        athena_client = create_aws_client('athena')
        if not athena_client:
            # Error is already handled by create_aws_client st.error(...)
            return pd.DataFrame()

        database, s3_output = get_athena_config()

        if not database or not s3_output:
            st.error("Athena database or S3 output location is not configured. Cannot run query.")
            return pd.DataFrame()

        # st.info(f"Starting Athena query execution for: {query_name}")
        response = athena_client.start_query_execution(
            QueryString=query,
            QueryExecutionContext={'Database': database},
            ResultConfiguration={'OutputLocation': s3_output}
        )
        query_execution_id = response['QueryExecutionId']

        # Using a spinner for user feedback during query execution
        with st.spinner(f"⏳ Running query for '{query_name}'... please wait."):
            while True:
                query_status = athena_client.get_query_execution(QueryExecutionId=query_execution_id)
                status = query_status['QueryExecution']['Status']['State']
                if status in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
                    break
                time.sleep(1)

            if status == 'SUCCEEDED':
                # st.info(f"Query '{query_name}' SUCCEEDED. Fetching results...")
                # Paginate results if necessary, though get_query_results usually handles reasonable sizes well.
                results_paginator = athena_client.get_paginator('get_query_results')
                query_results_iter = results_paginator.paginate(QueryExecutionId=query_execution_id)
                
                all_rows = []
                columns = []
                first_page = True

                for page in query_results_iter:
                    if first_page:
                        columns = [col_info['Label'] for col_info in page['ResultSet']['ResultSetMetadata']['ColumnInfo']]
                        first_page = False
                    
                    # Skip header row in subsequent pages if it's somehow included by API,
                    # normally only first page's ResultSet['Rows'][0] is header.
                    # The official example for parsing results:
                    # rows = page['ResultSet']['Rows']
                    # if columns and rows[0] and all(rows[0]['Data'][i].get('VarCharValue') == columns[i] for i in range(len(columns))):
                    #    rows = rows[1:] # Skip header row if present
                    
                    page_rows = page['ResultSet']['Rows']
                    # The first row of the first page is the header, skip it.
                    if columns and page_rows and all(page_rows[0]['Data'][i].get('VarCharValue') == columns[i] for i in range(len(columns))):
                         page_rows = page_rows[1:]


                    for row_data in page_rows:
                        all_rows.append([item.get('VarCharValue') for item in row_data['Data']])
                
                df = pd.DataFrame(all_rows, columns=columns)
                
                # Save to file cache
                try:
                    df.to_csv(cache_file, index=False)
                    # st.info(f"Saved query results for '{query_name}' to file cache: {cache_file}")
                except Exception as e:
                    st.warning(f"Failed to save data for '{query_name}' to cache file {cache_file}: {str(e)}")
                
                return df
            else:
                error_message = query_status['QueryExecution']['Status'].get('StateChangeReason', 'Unknown error')
                st.error(f"Athena query '{query_name}' FAILED. Reason: {error_message}")
                return pd.DataFrame()

    except Exception as e:
        st.error(f"An error occurred while executing Athena query '{query_name}': {str(e)}")
        # Optionally, log the full traceback here for debugging
        # import traceback
        # st.error(traceback.format_exc())
        return pd.DataFrame()

# Added from user_demographics.py
import pandas as pd
import time
from pathlib import Path

# Create data cache directory if it doesn't exist
CACHE_DIR = Path("./data_cache")
CACHE_DIR.mkdir(exist_ok=True)

# Function to execute Athena query with caching
@st.cache_data(ttl=3600, show_spinner=False)
def query_athena_with_cache(query: str, query_name: str):
    """
    Execute Athena query with both Streamlit's in-memory caching (@st.cache_data)
    and a manual file-based cache.

    Parameters:
    - query: Athena SQL query string.
    - query_name: A unique name for the query, used for naming the cache file.

    Returns:
    - pd.DataFrame: Query results as a Pandas DataFrame, or None if an error occurs.
    """
    # Define cache file path using the provided query_name
    cache_file = CACHE_DIR / f"{query_name.replace(' ', '_').lower()}.csv"

    # Check if file cache exists and is recent (Streamlit's @st.cache_data handles TTL for in-memory)
    # For file cache, we'll rely on Streamlit's caching for re-computation avoidance mostly.
    # The file cache here acts more as a persistent store across app restarts if @st.cache_data doesn't cover that.
    if cache_file.exists():
        try:
            # st.info(f"Loading data for '{query_name}' from file cache: {cache_file}")
            return pd.read_csv(cache_file)
        except Exception as e:
            st.warning(f"Failed to load cached data from {cache_file}: {str(e)}. Running query...")

    # Run the query if cache doesn't exist or is invalid
    try:
        athena_client = create_aws_client('athena')
        if not athena_client:
            # Error is already handled by create_aws_client st.error(...)
            return pd.DataFrame()

        database, s3_output = get_athena_config()

        if not database or not s3_output:
            st.error("Athena database or S3 output location is not configured. Cannot run query.")
            return pd.DataFrame()

        # st.info(f"Starting Athena query execution for: {query_name}")
        response = athena_client.start_query_execution(
            QueryString=query,
            QueryExecutionContext={'Database': database},
            ResultConfiguration={'OutputLocation': s3_output}
        )
        query_execution_id = response['QueryExecutionId']

        # Using a spinner for user feedback during query execution
        with st.spinner(f"⏳ Running query for '{query_name}'... please wait."):
            while True:
                query_status = athena_client.get_query_execution(QueryExecutionId=query_execution_id)
                status = query_status['QueryExecution']['Status']['State']
                if status in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
                    break
                time.sleep(1)

            if status == 'SUCCEEDED':
                # st.info(f"Query '{query_name}' SUCCEEDED. Fetching results...")
                # Paginate results if necessary, though get_query_results usually handles reasonable sizes well.
                results_paginator = athena_client.get_paginator('get_query_results')
                query_results_iter = results_paginator.paginate(QueryExecutionId=query_execution_id)
                
                all_rows = []
                columns = []
                first_page = True

                for page in query_results_iter:
                    if first_page:
                        columns = [col_info['Label'] for col_info in page['ResultSet']['ResultSetMetadata']['ColumnInfo']]
                        first_page = False
                    
                    # Skip header row in subsequent pages if it's somehow included by API,
                    # normally only first page's ResultSet['Rows'][0] is header.
                    # The official example for parsing results:
                    # rows = page['ResultSet']['Rows']
                    # if columns and rows[0] and all(rows[0]['Data'][i].get('VarCharValue') == columns[i] for i in range(len(columns))):
                    #    rows = rows[1:] # Skip header row if present
                    
                    page_rows = page['ResultSet']['Rows']
                    # The first row of the first page is the header, skip it.
                    if columns and page_rows and all(page_rows[0]['Data'][i].get('VarCharValue') == columns[i] for i in range(len(columns))):
                         page_rows = page_rows[1:]


                    for row_data in page_rows:
                        all_rows.append([item.get('VarCharValue') for item in row_data['Data']])
                
                df = pd.DataFrame(all_rows, columns=columns)
                
                # Save to file cache
                try:
                    df.to_csv(cache_file, index=False)
                    # st.info(f"Saved query results for '{query_name}' to file cache: {cache_file}")
                except Exception as e:
                    st.warning(f"Failed to save data for '{query_name}' to cache file {cache_file}: {str(e)}")
                
                return df
            else:
                error_message = query_status['QueryExecution']['Status'].get('StateChangeReason', 'Unknown error')
                st.error(f"Athena query '{query_name}' FAILED. Reason: {error_message}")
                return pd.DataFrame()

    except Exception as e:
        st.error(f"An error occurred while executing Athena query '{query_name}': {str(e)}")
        # Optionally, log the full traceback here for debugging
        # import traceback
        # st.error(traceback.format_exc())
        return pd.DataFrame() 