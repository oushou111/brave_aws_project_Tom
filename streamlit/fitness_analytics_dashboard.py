import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import boto3
import time
import io
import aws_connection
import athena_queries

# Set page title and configuration
st.set_page_config(page_title="Peak Fitness Analytics Dashboard", layout="wide")
st.title("Peak Fitness Analytics Dashboard")

# Function to execute Athena query
def query_athena(query, database, s3_output):
    """
    Execute Athena query and return results
    
    Parameters:
    - query: Athena SQL query
    - database: Athena database name
    - s3_output: S3 output location for query results
    
    Returns:
    - DataFrame: Query results
    """
    try:
        # Create Athena client using AWS connection module
        athena_client = aws_connection.create_aws_client('athena')
        if not athena_client:
            st.error("Failed to create Athena client. Please check your AWS credentials.")
            return None
        
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
        st.write(f"Query ID: {query_execution_id}")
        
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
            return df
        else:
            error_message = query_status['QueryExecution']['Status'].get('StateChangeReason', 'Unknown error')
            st.error(f"Query failed: {error_message}")
            return None
    except Exception as e:
        st.error(f"Error executing query: {str(e)}")
        return None

# Function to visualize store attendance trends
def visualize_store_attendance(df):
    """
    Visualize store attendance trends by month and location
    """
    # Ensure data types
    df['month'] = df['month'].astype(int)
    df['total_attendance'] = pd.to_numeric(df['total_attendance'], errors='coerce')
    
    # Create a pivot table for better visualization
    pivot_df = df.pivot_table(
        index='month', 
        columns='location_name', 
        values='total_attendance', 
        aggfunc='sum'
    ).fillna(0)
    
    # Plotting
    st.subheader("Store Attendance Trends (2025)")
    
    fig, ax = plt.subplots(figsize=(12, 8))
    pivot_df.plot(kind='bar', ax=ax)
    plt.title('Monthly Attendance by Location (2025)')
    plt.xlabel('Month')
    plt.ylabel('Total Attendance')
    plt.xticks(rotation=45)
    plt.legend(title='Location', bbox_to_anchor=(1.05, 1), loc='upper left')
    plt.tight_layout()
    st.pyplot(fig)
    
    # Line chart for trends
    fig2, ax2 = plt.subplots(figsize=(12, 8))
    pivot_df.plot(kind='line', marker='o', ax=ax2)
    plt.title('Monthly Attendance Trends by Location (2025)')
    plt.xlabel('Month')
    plt.ylabel('Total Attendance')
    plt.grid(True, linestyle='--', alpha=0.7)
    plt.legend(title='Location', bbox_to_anchor=(1.05, 1), loc='upper left')
    plt.tight_layout()
    st.pyplot(fig2)
    
    # City summary
    st.subheader("Attendance by City")
    city_df = df.groupby('city')['total_attendance'].sum().reset_index()
    city_df = city_df.sort_values('total_attendance', ascending=False)
    
    fig3, ax3 = plt.subplots(figsize=(10, 6))
    sns.barplot(x='total_attendance', y='city', data=city_df, ax=ax3)
    plt.title('Total Attendance by City (2025)')
    plt.xlabel('Total Attendance')
    plt.ylabel('City')
    plt.tight_layout()
    st.pyplot(fig3)
    
    # Raw data
    with st.expander("View Raw Data"):
        st.dataframe(df)

# Function to visualize popular class times
def visualize_popular_class_times(df):
    """
    Visualize most popular class times
    """
    # Ensure data types
    df['class_hour'] = pd.to_numeric(df['class_hour'], errors='coerce')
    df['total_sessions'] = pd.to_numeric(df['total_sessions'], errors='coerce')
    
    # Sort by hour for better visualization
    df = df.sort_values('class_hour')
    
    # Plotting
    st.subheader("Most Popular Class Times (2025)")
    
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
        period_sessions = df[(df['class_hour'] >= start) & (df['class_hour'] <= end)]['total_sessions'].sum()
        period_data.append({'period': period, 'total_sessions': period_sessions})
    
    period_df = pd.DataFrame(period_data)
    
    fig2, ax2 = plt.subplots(figsize=(10, 6))
    pie = ax2.pie(
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

# Function to visualize popular class days
def visualize_popular_class_days(df):
    """
    Visualize most popular class days
    """
    # Ensure data types
    if 'weekday' in df.columns:
        df['weekday'] = pd.to_numeric(df['weekday'], errors='coerce')
    df['total_sessions'] = pd.to_numeric(df['total_sessions'], errors='coerce')
    
    # Sort by weekday for better visualization
    if 'weekday' in df.columns:
        df = df.sort_values('weekday')
    
    # Plotting
    st.subheader("Most Popular Class Days (2025)")
    
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
    
    # Weekday vs Weekend comparison
    st.subheader("Weekday vs Weekend Comparison")
    
    # Combine weekday and weekend sessions
    weekday_sessions = df[df['weekday'].isin([2, 3, 4, 5, 6])]['total_sessions'].sum()
    weekend_sessions = df[df['weekday'].isin([1, 7])]['total_sessions'].sum()
    
    comparison_df = pd.DataFrame({
        'period': ['Weekdays', 'Weekend'],
        'total_sessions': [weekday_sessions, weekend_sessions]
    })
    
    fig2, ax2 = plt.subplots(figsize=(8, 6))
    pie = ax2.pie(
        comparison_df['total_sessions'],
        labels=comparison_df['period'],
        autopct='%1.1f%%',
        startangle=90,
        explode=(0, 0.1),
        shadow=True
    )
    plt.title('Weekday vs Weekend Class Distribution')
    plt.axis('equal')
    st.pyplot(fig2)
    
    # Raw data
    with st.expander("View Raw Data"):
        st.dataframe(df)

# Main app
def main():
    # Sidebar configuration
    st.sidebar.title("Analytics Options")
    
    # AWS Connection setup
    if aws_connection.test_aws_connection():
        st.sidebar.success("AWS connection successful!")
    else:
        st.sidebar.error("AWS connection failed. Please configure credentials.")
    
    # Athena configuration
    database = st.sidebar.text_input("Athena database name", "peak_fitness")
    s3_output = st.sidebar.text_input("S3 output location", "s3://peak-fitness-processed-data-0503/athena-results/")
    
    # Analysis selection
    analysis_type = st.sidebar.selectbox(
        "Select Analysis",
        ["Store Attendance Trends", "Popular Class Times", "Popular Class Days", "All Analyses"]
    )
    
    # Run analysis button
    run_analysis = st.sidebar.button("Run Analysis")
    
    if run_analysis:
        if analysis_type == "Store Attendance Trends" or analysis_type == "All Analyses":
            with st.spinner("Running store attendance analysis..."):
                query = athena_queries.LOCATION_ANALYSIS_QUERIES["门店出勤趋势"]
                df_store = query_athena(query, database, s3_output)
                if df_store is not None:
                    visualize_store_attendance(df_store)
        
        if analysis_type == "Popular Class Times" or analysis_type == "All Analyses":
            with st.spinner("Running popular class times analysis..."):
                query = athena_queries.TIME_ANALYSIS_QUERIES["最受欢迎的课程时间"]
                df_times = query_athena(query, database, s3_output)
                if df_times is not None:
                    visualize_popular_class_times(df_times)
        
        if analysis_type == "Popular Class Days" or analysis_type == "All Analyses":
            with st.spinner("Running popular class days analysis..."):
                query = athena_queries.TIME_ANALYSIS_QUERIES["最受欢迎的课程日"]
                df_days = query_athena(query, database, s3_output)
                if df_days is not None:
                    visualize_popular_class_days(df_days)
    
    # Display instructions if no analysis is running
    if not run_analysis:
        st.info("Select an analysis type and click 'Run Analysis' to start visualization")
        
        # Show sample visualizations with mock data
        st.subheader("Sample Visualizations (Mock Data)")
        
        # Sample store data
        if analysis_type == "Store Attendance Trends" or analysis_type == "All Analyses":
            sample_store_data = {
                'location_id': ['L001', 'L002', 'L003'] * 4,
                'city': ['new york', 'los angeles', 'chicago'] * 4,
                'location_name': ['downtown', 'west side', 'lakefront'] * 4,
                'year': [2025] * 12,
                'month': [1, 1, 1, 2, 2, 2, 3, 3, 3, 4, 4, 4],
                'total_attendance': [120, 95, 80, 135, 100, 85, 150, 110, 90, 145, 115, 95]
            }
            sample_df_store = pd.DataFrame(sample_store_data)
            visualize_store_attendance(sample_df_store)
        
        # Sample class times data
        if analysis_type == "Popular Class Times" or analysis_type == "All Analyses":
            sample_times_data = {
                'class_hour': [6, 7, 8, 9, 10, 12, 15, 17, 18, 19, 20],
                'total_sessions': [50, 75, 120, 95, 80, 110, 85, 150, 200, 180, 90]
            }
            sample_df_times = pd.DataFrame(sample_times_data)
            visualize_popular_class_times(sample_df_times)
        
        # Sample class days data
        if analysis_type == "Popular Class Days" or analysis_type == "All Analyses":
            sample_days_data = {
                'weekday': [1, 2, 3, 4, 5, 6, 7],
                'weekday_name': ['Sunday', 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday'],
                'total_sessions': [150, 85, 90, 110, 120, 160, 180]
            }
            sample_df_days = pd.DataFrame(sample_days_data)
            visualize_popular_class_days(sample_df_days)

if __name__ == "__main__":
    main() 