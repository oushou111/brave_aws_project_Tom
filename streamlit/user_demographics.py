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

# Function to visualize age distribution
def visualize_age_distribution(df):
    """
    Visualize user age distribution
    """
    st.subheader("User Age Distribution")

    if df is None or df.empty:
        st.info("No data available for User Age Distribution.")
        return

    # Ensure data types
    df['total'] = pd.to_numeric(df['total'], errors='coerce')
    
    # Sort by age group properly
    age_order = ['<25', '25-34', '35-44', '45+']
    df['age_group'] = pd.Categorical(df['age_group'], categories=age_order, ordered=True)
    df = df.sort_values('age_group')
    
    # Column chart
    fig, ax = plt.subplots(figsize=(10, 6))
    bars = sns.barplot(x='age_group', y='total', data=df, ax=ax)
    
    # Add value labels on top of bars
    for i, bar in enumerate(bars.patches):
        bars.text(
            bar.get_x() + bar.get_width()/2.,
            bar.get_height() + 5,
            f'{int(df.iloc[i]["total"])}',
            ha='center', va='bottom', fontsize=10
        )
    
    plt.title('User Age Distribution')
    plt.xlabel('Age Group')
    plt.ylabel('Number of Users')
    plt.tight_layout()
    st.pyplot(fig)
    
    # Pie chart for age distribution
    # Ensure there's data to plot for the pie chart
    df_pie = df.dropna(subset=['total']) # Drop rows where 'total' is NaN after numeric conversion
    if not df_pie.empty and df_pie['total'].sum() > 0:
        fig2, ax2 = plt.subplots(figsize=(8, 8))
        # Dynamically create explode based on number of data points
        num_slices = len(df_pie)
        explode_list = [0.05] + [0] * (num_slices - 1) if num_slices > 0 else []
        
        # Ensure explode_list is not longer than the number of slices
        if num_slices > 0 :
             explode_list = explode_list[:num_slices]
        else: # if num_slices is 0, df_pie is empty, pie chart should not be drawn.
             st.info("Not enough data to display Age Distribution pie chart.")
             # Intentionally not returning here, to allow raw data display if desired.
             # The outer 'if not df_pie.empty ...' handles the no-data case for pie.
        
        # Only attempt to plot if there are slices and explode_list is correctly sized
        if num_slices > 0 and len(explode_list) == num_slices:
            plt.pie(
                df_pie['total'], 
                labels=df_pie['age_group'], 
                autopct='%1.1f%%',
                startangle=90,
                shadow=True,
                explode=explode_list
            )
            plt.title('Age Distribution (Percentage)')
            plt.axis('equal')
            st.pyplot(fig2)
        elif num_slices > 0 : # num_slices > 0 but explode list is not right (should not happen with current logic)
            st.warning("Could not generate pie chart due to data inconsistency.")

    else:
        st.info("No valid data to display Age Distribution pie chart.")
    
    # Raw data
    with st.expander("View Raw Data"):
        st.dataframe(df)
        
        # Download option
        csv = df.to_csv(index=False).encode('utf-8')
        st.download_button(
            "Download CSV",
            data=csv,
            file_name='age_distribution.csv',
            mime='text/csv',
        )

# Function to visualize gender distribution
def visualize_gender_distribution(df):
    """
    Visualize user gender distribution
    """
    st.subheader("User Gender Distribution")

    if df is None or df.empty:
        st.info("No data available for User Gender Distribution.")
        return

    # Ensure data types
    df['total'] = pd.to_numeric(df['total'], errors='coerce')
    
    # Column chart
    fig, ax = plt.subplots(figsize=(8, 5))
    bars = sns.barplot(x='standardized_gender', y='total', data=df, ax=ax)
    
    # Add value labels on top of bars
    for i, bar in enumerate(bars.patches):
        bars.text(
            bar.get_x() + bar.get_width()/2.,
            bar.get_height() + 5,
            f'{int(df.iloc[i]["total"])}',
            ha='center', va='bottom', fontsize=10
        )
    
    plt.title('User Gender Distribution')
    plt.xlabel('Gender')
    plt.ylabel('Number of Users')
    plt.tight_layout()
    st.pyplot(fig)
    
    # Pie chart for gender distribution
    fig2, ax2 = plt.subplots(figsize=(8, 8))
    colors = ['#3498db', '#e74c3c']  # Blue for Male, Red for Female
    plt.pie(
        df['total'], 
        labels=df['standardized_gender'], 
        autopct='%1.1f%%',
        startangle=90,
        shadow=True,
        colors=colors
    )
    plt.title('Gender Distribution (Percentage)')
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
            file_name='gender_distribution.csv',
            mime='text/csv',
        )

# Define SQL queries
age_distribution_query = """
SELECT 
  CASE 
    WHEN final_age < 25 THEN '<25'
    WHEN final_age BETWEEN 25 AND 34 THEN '25-34'
    WHEN final_age BETWEEN 35 AND 44 THEN '35-44'
    ELSE '45+'
  END AS age_group,
  COUNT(*) AS total
FROM (
  SELECT DISTINCT user_id,
    COALESCE(age, CAST(FLOOR(DATE_DIFF('year', date_of_birth, current_date)) AS int)) AS final_age
  FROM dim_user_v3
  WHERE age IS NOT NULL OR date_of_birth IS NOT NULL
) AS age_data
GROUP BY 
  CASE 
    WHEN final_age < 25 THEN '<25'
    WHEN final_age BETWEEN 25 AND 34 THEN '25-34'
    WHEN final_age BETWEEN 35 AND 44 THEN '35-44'
    ELSE '45+'
  END
ORDER BY age_group;
"""

gender_distribution_query = """
SELECT
  CASE 
    WHEN LOWER(gender) = 'male' THEN 'Male'
    WHEN LOWER(gender) = 'female' THEN 'Female'
  END AS standardized_gender,
  COUNT(*) AS total
FROM dim_user_v3
WHERE LOWER(gender) IN ('male', 'female')
GROUP BY 
  CASE 
    WHEN LOWER(gender) = 'male' THEN 'Male'
    WHEN LOWER(gender) = 'female' THEN 'Female'
  END
ORDER BY total DESC;
"""

def display_demographics_dashboard():
    """
    Display the complete user demographics dashboard
    """
    st.title("User Demographics Dashboard")
    st.markdown("### User Demographics Analysis")
    st.write("This dashboard provides insights into user demographics based on age and gender distribution.")
    
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
            with st.spinner("Querying Athena for demographics data..."):
                age_data = query_athena_with_cache(age_distribution_query, "age_distribution")
                gender_data = query_athena_with_cache(gender_distribution_query, "gender_distribution")
                
                # Display data source indicator
                st.sidebar.success("Using real data from AWS Athena")
        else:
            st.sidebar.error("AWS connection failed. No data will be loaded for User Demographics.")
            age_data = None # Ensure data is None if AWS connection fails
            gender_data = None # Ensure data is None if AWS connection fails
            
            # Display data source indicator
            # st.sidebar.warning("Using dummy data due to AWS connection failure") # No dummy data fallback
    else:
        # This block should ideally not be reached if main_dashboard correctly sets force_data_source
        st.sidebar.error(f"Unexpected data source: {data_source}. Defaulting to no data.")
        age_data = None
        gender_data = None
        
        # Display data source indicator
        # st.sidebar.info("Using dummy development data") # No dummy data
    
    # Create tabs for different visualizations
    tab1, tab2 = st.tabs(["Age Distribution", "Gender Distribution"])
    
    with tab1:
        if age_data is not None:
            visualize_age_distribution(age_data)
        else:
            st.error("Failed to load age distribution data.")
    
    with tab2:
        if gender_data is not None:
            visualize_gender_distribution(gender_data)
        else:
            st.error("Failed to load gender distribution data.")
    
    # Refresh button
    if st.sidebar.button("Refresh Data", key="refresh_data_demographics"):
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
    display_demographics_dashboard() 