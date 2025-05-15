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
import sys
import importlib.util

# Import custom modules
import user_demographics
import class_attendance_analysis
import aws_connection
import user_churn_analysis
import leaderboard_feature

# Ensure data cache directory exists
CACHE_DIR = Path("./data_cache")
CACHE_DIR.mkdir(exist_ok=True)

# Set page title and configuration - this should be the FIRST Streamlit command
st.set_page_config(
    page_title="Peak Fitness Analytics",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Add module paths to sys.path if needed
current_dir = os.path.dirname(os.path.abspath(__file__))
if current_dir not in sys.path:
    sys.path.append(current_dir)

# Import module functionality dynamically
def import_module_from_file(module_name, file_path):
    """
    Dynamically import a module from file path
    """
    try:
        spec = importlib.util.spec_from_file_location(module_name, file_path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module
    except Exception as e:
        st.error(f"Error importing module {module_name}: {str(e)}")
        return None

# Debug information
DEBUG = False

# Function to load fallback dummy data when modules are not available
def load_fallback_dummy_data(query_name):
    """
    Load fallback dummy data when modules cannot be imported.
    This version is modified to return None, effectively disabling dummy data.
    """
    # Return None for any query_name to disable dummy data
    return None

# Basic visualization functions in case modules are not available
def visualize_age_distribution_fallback(df):
    """
    Fallback visualization for age distribution
    """
    if df is None or df.empty:
        st.info("No data available for User Age Distribution (Fallback).")
        return
    # Ensure data types
    df['total'] = pd.to_numeric(df['total'], errors='coerce')
    
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

def visualize_gender_distribution_fallback(df):
    """
    Fallback visualization for gender distribution
    """
    if df is None or df.empty:
        st.info("No data available for User Gender Distribution (Fallback).")
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

def visualize_popular_classes_fallback(df):
    """
    Fallback visualization for popular classes
    """
    if df is None or df.empty:
        st.info("No data available for Popular Classes (Fallback).")
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

def main():
    """
    Main function for the Streamlit dashboard app
    """
    # Page title and header
    st.title("Peak Fitness Analytics Dashboard")
    
    # Sidebar navigation
    st.sidebar.title("Navigation")
    
    # AWS connection status
    aws_status = aws_connection.test_aws_connection()
    data_source = "AWS Athena" # Initialize data_source

    if aws_status:
        st.sidebar.success("AWS Connection: Connected ✅")
        
        # Display AWS configuration information
        with st.sidebar.expander("AWS Configuration Details", expanded=False):
            credentials = aws_connection.get_aws_credentials()
            athena_db, athena_s3_output = aws_connection.get_athena_config()

            # Robust check for credentials before trying to access its elements
            if credentials and isinstance(credentials, dict):
                access_key_val = credentials.get('access_key')
                region_val = credentials.get('region')

                if access_key_val and isinstance(access_key_val, str) and region_val and isinstance(region_val, str):
                    masked_access_key = access_key_val[:4] + "****" if len(access_key_val) >= 4 else "****"
                    st.write(f"AWS Region: {region_val}")
                    st.write(f"Access Key: {masked_access_key}")
                else:
                    st.warning("AWS access key or region is missing or not a string. Please configure.")
            else:
                st.warning("AWS credentials dictionary is not available. Please configure.")

            if athena_db and athena_s3_output:
                st.write(f"Athena Database: {athena_db}")
                st.write(f"S3 Output Location: {athena_s3_output}")
            else:
                st.warning("Athena database or S3 output location is not configured.")
    else:
        st.sidebar.error("AWS Connection: Not Connected ❌")
        st.sidebar.warning("AWS connection failed. Data queries may not succeed.")
        # data_source is already set to "AWS Athena" by default initialization
    
    # Select dashboard view
    dashboard_view = st.sidebar.radio(
        "Select Dashboard",
        ["User Demographics", "Class Attendance Analysis", "User Activity & Churn", "App Leaderboard"],
        index=0
    )
    
    # The data source is now fixed to AWS Athena.
    # The radio button for selecting "Dummy Data (Development)" is removed.
    # data_source is determined above based on aws_status, but always targets Athena.
    # We just need to display the info/warning based on aws_status here.
    if aws_status:
        # data_source = "AWS Athena" # Already set
        st.sidebar.info("Data Source: AWS Athena (Connected)")
    else:
        # data_source = "AWS Athena" # Already set
        st.sidebar.warning("Data Source: AWS Athena (Connection Failed)")
    
    # Force refresh cache when switching data sources (though switching is no longer manual)
    if 'last_data_source' in st.session_state and st.session_state['last_data_source'] != data_source:
        st.cache_data.clear()
    
    # Store current data source in session
    st.session_state['last_data_source'] = data_source
    
    # Refresh button
    if st.sidebar.button("Refresh Data"):
        # Clear cache and reload
        try:
            st.cache_data.clear()
        except:
            st.sidebar.warning("Cache clear failed. Try refreshing the page.")
        st.rerun()
    
    # Cache information
    st.sidebar.info(
        "Data is cached for 1 hour to improve performance. "
        "Click 'Refresh Data' to fetch the latest data from the source."
    )
    
    # Display appropriate dashboard based on selection
    if dashboard_view == "User Demographics":
        # Execute the function from the imported module with data source parameter
        try:
            # Override module's internal data source selection
            st.session_state['force_data_source'] = data_source
            user_demographics.display_demographics_dashboard()
        except Exception as e:
            st.error(f"Error displaying demographics dashboard: {str(e)}")
            # Display fallback content
            st.subheader("User Demographics (Fallback View)")
            col1, col2 = st.columns(2)
            with col1:
                visualize_age_distribution_fallback(user_demographics.fetch_age_distribution_data_fallback())
            with col2:
                visualize_gender_distribution_fallback(user_demographics.fetch_gender_distribution_data_fallback())
    elif dashboard_view == "Class Attendance Analysis":
        # Execute the function from the imported module with data source parameter
        try:
            # Override module's internal data source selection
            st.session_state['force_data_source'] = data_source
            class_attendance_analysis.display_attendance_dashboard()
        except Exception as e:
            st.error(f"Error displaying class attendance dashboard: {str(e)}")
            # Display fallback content
            st.subheader("Class Attendance Analysis (Fallback View)")
            # Reverted to use load_fallback_dummy_data directly as it's defined in this file
            # and designed to return None, effectively showing "No data available"
            popular_classes_fallback_data = load_fallback_dummy_data("popular_classes")
            visualize_popular_classes_fallback(popular_classes_fallback_data)
    elif dashboard_view == "User Activity & Churn":
        try:
            st.session_state['force_data_source'] = data_source
            user_churn_analysis.display_churn_dashboard()
        except Exception as e:
            st.error(f"Error displaying user churn dashboard: {str(e)}")
            st.info("User activity & churn data currently unavailable (Fallback).")
    elif dashboard_view == "App Leaderboard":
        try:
            # Ensure data_source context if leaderboard_feature needs it, though it currently doesn't explicitly use it in display_leaderboard
            # st.session_state['force_data_source'] = data_source 
            leaderboard_feature.display_leaderboard()
        except Exception as e:
            st.error(f"Error displaying App Leaderboard: {str(e)}")
            st.info("Leaderboard data is currently unavailable.")

def display_additional_analysis(data_source):
    """
    Display additional analyses, such as churn prediction
    """
    st.header("Additional Analytics")
    
    # Placeholder for future analyses
    st.info("This section is reserved for future analytics capabilities.")
    
    # Example churn analysis
    st.subheader("User Churn Analysis")
    st.markdown("""
    This section will include:
    - 90-day churn detection
    - Churn prediction models
    - Retention strategies
    - User lifecycle analysis
    """)
    
    # Example engagement analysis
    st.subheader("User Engagement Analysis")
    st.markdown("""
    This section will include:
    - Average sessions per user
    - Engagement by class type
    - Engagement by time of day/week
    - User journey mapping
    """)

if __name__ == "__main__":
    main() 