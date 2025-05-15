import streamlit as st
import pandas as pd
import boto3
import time
import matplotlib
matplotlib.use('Agg')  # Non-interactive backend
import matplotlib.pyplot as plt
import seaborn as sns
import pyarrow.parquet as pq
import io
from io import StringIO
import athena_queries
import aws_connection
import os

# Set page title
st.set_page_config(page_title="Peak Fitness Analytics Dashboard", layout="wide")
st.title("Peak Fitness Analytics Dashboard")

# Function to connect to Athena and execute queries
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
        with st.spinner("Executing Athena query..."):
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

# Function to load data from local Parquet file
def load_parquet_data(file_path):
    """
    Load data from local Parquet file
    
    Parameters:
    - file_path: Path to Parquet file
    
    Returns:
    - DataFrame: Loaded data
    """
    try:
        table = pq.read_table(file_path)
        df = table.to_pandas()
        return df
    except Exception as e:
        st.error(f"Error loading Parquet file: {str(e)}")
        return None

# Function to load data from CSV file
def load_csv_data(file_path):
    """
    Load data from CSV file
    
    Parameters:
    - file_path: Path to CSV file
    
    Returns:
    - DataFrame: Loaded data
    """
    try:
        df = pd.read_csv(file_path)
        return df
    except Exception as e:
        st.error(f"Error loading CSV file: {str(e)}")
        return None

# File upload options
st.sidebar.header("Data Source Selection")

# Check AWS connection status first
aws_connected = aws_connection.test_aws_connection()

# Display AWS connection status
if aws_connected:
    st.sidebar.success("AWS Connection: Connected ✅")
    # Display AWS configuration information
    with st.sidebar.expander("AWS Configuration Details", expanded=False):
        credentials = aws_connection.get_aws_credentials()
        masked_access_key = credentials['access_key'][:4] + "****" if credentials['access_key'] else "Not configured"
        st.write(f"AWS Region: {credentials['region']}")
        st.write(f"Access Key: {masked_access_key}")
        
        # Show Athena configuration
        database, s3_output = aws_connection.get_athena_config()
        st.write(f"Athena Database: {database}")
        st.write(f"S3 Output Location: {s3_output}")
    
    # Data source options
    data_source = st.sidebar.radio(
        "Select data source",
        ["Query from Athena", "Upload local file", "Use sample data"]
    )
else:
    st.sidebar.error("AWS Connection: Not Connected ❌")
    st.sidebar.warning("AWS connection is not available. Some features may be limited.")
    # Limit data source options when AWS is not available
    data_source = st.sidebar.radio(
        "Select data source",
        ["Upload local file", "Use sample data"]
    )

# Data source handling
if data_source == "Query from Athena":
    st.sidebar.subheader("Athena Query Configuration")
    
    # Get values from AWS connection module first
    default_database, default_s3_output = aws_connection.get_athena_config()
    
    database = st.sidebar.text_input("Athena database name", default_database)
    s3_output = st.sidebar.text_input("S3 output location", default_s3_output)
    
    # Add predefined query options
    query_method = st.sidebar.radio(
        "Query method",
        ["Use predefined query", "Custom SQL query"]
    )
    
    if query_method == "Use predefined query":
        # Get query categories
        categories = athena_queries.get_query_categories()
        selected_category = st.sidebar.selectbox("Select query category", categories)
        
        # Get queries in selected category
        if selected_category:
            queries_in_category = athena_queries.get_queries_in_category(selected_category)
            query_names = list(queries_in_category.keys())
            selected_query_name = st.sidebar.selectbox("Select query", query_names)
            
            # Display selected query SQL
            if selected_query_name:
                query = athena_queries.get_query(selected_category, selected_query_name)
                st.sidebar.code(query, language="sql")
    else:
        # Custom SQL query
        query = st.sidebar.text_area("Enter Athena SQL query", 
                                   "SELECT * FROM events LIMIT 100")
    
    if st.sidebar.button("Execute Query"):
        with st.spinner("Executing query..."):
            if query_method == "Use predefined query" and selected_query_name:
                exec_query = athena_queries.get_query(selected_category, selected_query_name)
            else:
                exec_query = query
                
            df = query_athena(exec_query, database, s3_output)
            if df is not None:
                st.session_state["df"] = df
                st.success("Query successful! Data loaded.")
                
                # Optionally save to cache
                if st.sidebar.checkbox("Save to cache for future use"):
                    cache_name = st.sidebar.text_input("Cache name", "custom_query_result")
                    if cache_name:
                        cache_path = f"./data_cache/{cache_name}.csv"
                        df.to_csv(cache_path, index=False)
                        st.sidebar.success(f"Data saved to {cache_path}")

elif data_source == "Upload local file":
    st.sidebar.subheader("Upload File")
    file_type = st.sidebar.radio("Select file type", ["CSV", "Parquet"])
    uploaded_file = st.sidebar.file_uploader(f"Upload {file_type} file", type=["csv", "parquet"])
    
    if uploaded_file is not None:
        with st.spinner("Loading file..."):
            if file_type == "CSV":
                df = pd.read_csv(uploaded_file)
            else:  # Parquet
                # Save temporary file
                with open("temp.parquet", "wb") as f:
                    f.write(uploaded_file.getbuffer())
                df = load_parquet_data("temp.parquet")
                
            if df is not None:
                st.session_state["df"] = df
                st.success("File successfully loaded!")

else:  # Use sample data
    try:
        # Try to load from cache directory first
        sample_files = ["age_distribution.csv", "gender_distribution.csv", "popular_classes.csv", "popular_class_times.csv"]
        available_samples = []
        
        # Check which sample files are available
        for sample in sample_files:
            sample_path = f"./data_cache/{sample}"
            try:
                if os.path.exists(sample_path):
                    available_samples.append(sample)
            except:
                pass
        
        if available_samples:
            selected_sample = st.sidebar.selectbox("Select a sample dataset", available_samples)
            df = load_csv_data(f"./data_cache/{selected_sample}")
            if df is not None:
                st.session_state["df"] = df
                st.sidebar.success(f"Sample data loaded from cache: {selected_sample}")
        else:
            # If no cached samples, use default sample data
            df = pd.DataFrame({
                'class_name': ['Yoga Flow', 'HIIT Cardio', 'Strength Training', 'Pilates', 'Cycling'],
                'attendance_count': [2450, 2100, 1850, 1720, 1600]
            })
            st.session_state["df"] = df
            st.sidebar.success("Default sample data loaded (no cached samples found)")
    except Exception as e:
        st.sidebar.error(f"Error loading sample data: {str(e)}")

# Check if there is data to visualize
if "df" in st.session_state:
    df = st.session_state["df"]
    
    # Display data preview
    st.subheader("Data Preview")
    st.dataframe(df.head())
    
    # Display data statistics
    st.subheader("Data Statistics")
    st.write(f"Rows: {df.shape[0]}, Columns: {df.shape[1]}")
    
    # Select analysis options
    analysis_tabs = st.tabs(["Data Visualization", "Data Analysis", "Raw Data"])
    
    with analysis_tabs[0]:  # Data Visualization
        st.subheader("Data Visualization")
        
        # Select visualization type
        viz_type = st.selectbox(
            "Select visualization type",
            ["Bar Chart", "Line Chart", "Scatter Plot", "Pie Chart", "Heatmap", "Time Series"]
        )
        
        # Filter columns by data type
        numeric_cols = df.select_dtypes(include=['int64', 'float64']).columns.tolist()
        categorical_cols = df.select_dtypes(include=['object', 'category']).columns.tolist()
        date_cols = []
        
        # Try to identify date columns
        for col in df.columns:
            try:
                if pd.to_datetime(df[col], errors='coerce').notna().any():
                    date_cols.append(col)
            except:
                pass
        
        # Create visualization based on selected type
        if viz_type == "Bar Chart":
            if len(categorical_cols) > 0 and len(numeric_cols) > 0:
                x_col = st.selectbox("Select X-axis (categorical)", categorical_cols)
                y_col = st.selectbox("Select Y-axis (numeric)", numeric_cols)
                orientation = st.radio("Orientation", ["Vertical", "Horizontal"])
                
                fig, ax = plt.subplots(figsize=(10, 6))
                if orientation == "Vertical":
                    sns.barplot(x=x_col, y=y_col, data=df, ax=ax)
                else:
                    sns.barplot(y=x_col, x=y_col, data=df, ax=ax)
                    
                plt.title(f"{y_col} by {x_col}")
                plt.tight_layout()
                st.pyplot(fig)
            else:
                st.warning("This visualization requires at least one categorical and one numeric column.")
        
        elif viz_type == "Line Chart":
            if len(numeric_cols) >= 2 or (len(date_cols) > 0 and len(numeric_cols) > 0):
                if len(date_cols) > 0:
                    x_cols = date_cols + numeric_cols
                else:
                    x_cols = numeric_cols
                    
                x_col = st.selectbox("Select X-axis", x_cols)
                y_col = st.selectbox("Select Y-axis", numeric_cols)
                
                # Sort by x-col if it's a date
                if x_col in date_cols:
                    df_sorted = df.sort_values(by=x_col)
                    df_sorted[x_col] = pd.to_datetime(df_sorted[x_col])
                else:
                    df_sorted = df.sort_values(by=x_col)
                
                fig, ax = plt.subplots(figsize=(10, 6))
                plt.plot(df_sorted[x_col], df_sorted[y_col], marker='o')
                plt.title(f"{y_col} vs {x_col}")
                plt.grid(True, linestyle='--', alpha=0.7)
                plt.tight_layout()
                st.pyplot(fig)
            else:
                st.warning("This visualization requires at least two numeric columns or a date column and a numeric column.")
        
        elif viz_type == "Scatter Plot":
            if len(numeric_cols) >= 2:
                x_col = st.selectbox("Select X-axis", numeric_cols)
                y_col = st.selectbox("Select Y-axis", [col for col in numeric_cols if col != x_col] if len(numeric_cols) > 1 else numeric_cols)
                
                color_col = None
                if len(categorical_cols) > 0:
                    use_color = st.checkbox("Color points by category")
                    if use_color:
                        color_col = st.selectbox("Select color column", categorical_cols)
                
                fig, ax = plt.subplots(figsize=(10, 6))
                if color_col:
                    scatter = sns.scatterplot(x=x_col, y=y_col, hue=color_col, data=df, ax=ax)
                    plt.legend(title=color_col, bbox_to_anchor=(1.05, 1), loc='upper left')
                else:
                    scatter = sns.scatterplot(x=x_col, y=y_col, data=df, ax=ax)
                    
                plt.title(f"{y_col} vs {x_col}")
                plt.tight_layout()
                st.pyplot(fig)
            else:
                st.warning("This visualization requires at least two numeric columns.")
        
        elif viz_type == "Pie Chart":
            if len(categorical_cols) > 0 and len(numeric_cols) > 0:
                label_col = st.selectbox("Select labels", categorical_cols)
                value_col = st.selectbox("Select values", numeric_cols)
                
                # Aggregate data if there are duplicates
                pie_data = df.groupby(label_col)[value_col].sum().reset_index()
                
                # Limit to top 10 categories if there are too many
                if len(pie_data) > 10:
                    pie_data = pie_data.sort_values(by=value_col, ascending=False).head(10)
                    st.info("Showing only top 10 categories due to large number of values.")
                
                fig, ax = plt.subplots(figsize=(10, 10))
                plt.pie(
                    pie_data[value_col],
                    labels=pie_data[label_col],
                    autopct='%1.1f%%',
                    startangle=90,
                    shadow=True
                )
                plt.title(f"{label_col} Distribution by {value_col}")
                plt.axis('equal')
                st.pyplot(fig)
            else:
                st.warning("This visualization requires at least one categorical and one numeric column.")
        
        elif viz_type == "Heatmap":
            if len(numeric_cols) >= 2:
                selected_cols = st.multiselect("Select columns for correlation", numeric_cols, default=numeric_cols[:min(5, len(numeric_cols))])
                
                if len(selected_cols) >= 2:
                    corr_df = df[selected_cols].corr()
                    
                    fig, ax = plt.subplots(figsize=(10, 8))
                    heatmap = sns.heatmap(corr_df, annot=True, cmap='coolwarm', ax=ax)
                    plt.title("Correlation Heatmap")
                    plt.tight_layout()
                    st.pyplot(fig)
                else:
                    st.warning("Please select at least two columns for the heatmap.")
            else:
                st.warning("This visualization requires at least two numeric columns.")
        
        elif viz_type == "Time Series":
            if len(date_cols) > 0 and len(numeric_cols) > 0:
                date_col = st.selectbox("Select date/time column", date_cols)
                value_col = st.selectbox("Select value column", numeric_cols)
                
                # Convert to datetime and sort
                df_ts = df.copy()
                df_ts[date_col] = pd.to_datetime(df_ts[date_col], errors='coerce')
                df_ts = df_ts.dropna(subset=[date_col])
                df_ts = df_ts.sort_values(by=date_col)
                
                # Resample options
                resample = st.checkbox("Resample time series")
                if resample:
                    freq = st.selectbox(
                        "Select frequency", 
                        ["Day", "Week", "Month", "Quarter", "Year"],
                        index=0
                    )
                    agg_method = st.selectbox(
                        "Select aggregation method",
                        ["Mean", "Sum", "Min", "Max", "Count"],
                        index=0
                    )
                    
                    # Map selected options to pandas arguments
                    freq_map = {
                        "Day": "D", "Week": "W", "Month": "M", 
                        "Quarter": "Q", "Year": "Y"
                    }
                    agg_map = {
                        "Mean": "mean", "Sum": "sum", "Min": "min", 
                        "Max": "max", "Count": "count"
                    }
                    
                    # Resample the data
                    df_resampled = df_ts.set_index(date_col)[value_col].resample(freq_map[freq])
                    if agg_map[agg_method] == "mean":
                        df_resampled = df_resampled.mean()
                    elif agg_map[agg_method] == "sum":
                        df_resampled = df_resampled.sum()
                    elif agg_map[agg_method] == "min":
                        df_resampled = df_resampled.min()
                    elif agg_map[agg_method] == "max":
                        df_resampled = df_resampled.max()
                    else:  # count
                        df_resampled = df_resampled.count()
                    
                    # Plot the resampled data
                    fig, ax = plt.subplots(figsize=(12, 6))
                    plt.plot(df_resampled.index, df_resampled.values, marker='o', linestyle='-')
                    plt.title(f"{value_col} by {date_col} ({agg_method} per {freq})")
                    plt.grid(True, linestyle='--', alpha=0.7)
                    plt.tight_layout()
                    st.pyplot(fig)
                else:
                    # Plot the raw time series
                    fig, ax = plt.subplots(figsize=(12, 6))
                    plt.plot(df_ts[date_col], df_ts[value_col], marker='o', linestyle='-')
                    plt.title(f"{value_col} by {date_col}")
                    plt.grid(True, linestyle='--', alpha=0.7)
                    plt.tight_layout()
                    st.pyplot(fig)
            else:
                st.warning("This visualization requires at least one date column and one numeric column.")
    
    with analysis_tabs[1]:  # Data Analysis
        st.subheader("Data Analysis")
        
        analysis_type = st.selectbox(
            "Select analysis type",
            ["Summary Statistics", "Group Analysis", "Distribution Analysis"]
        )
        
        if analysis_type == "Summary Statistics":
            if len(numeric_cols) > 0:
                st.write("Summary statistics for numeric columns:")
                st.dataframe(df[numeric_cols].describe())
            else:
                st.warning("No numeric columns found for summary statistics.")
            
            if len(categorical_cols) > 0:
                st.write("Value counts for categorical columns:")
                for col in categorical_cols:
                    st.write(f"**{col}**")
                    st.dataframe(df[col].value_counts().reset_index().rename(columns={"index": col, col: "Count"}))
            else:
                st.warning("No categorical columns found for value counts.")
        
        elif analysis_type == "Group Analysis":
            if len(categorical_cols) > 0 and len(numeric_cols) > 0:
                group_col = st.selectbox("Select grouping column", categorical_cols)
                agg_cols = st.multiselect("Select columns to aggregate", numeric_cols, default=numeric_cols[:min(3, len(numeric_cols))])
                
                if agg_cols:
                    agg_funcs = st.multiselect(
                        "Select aggregation functions",
                        ["Mean", "Sum", "Min", "Max", "Count", "Std Dev"],
                        default=["Mean", "Sum"]
                    )
                    
                    if agg_funcs:
                        # Map selected options to pandas functions
                        func_map = {
                            "Mean": "mean", "Sum": "sum", "Min": "min",
                            "Max": "max", "Count": "count", "Std Dev": "std"
                        }
                        selected_funcs = [func_map[func] for func in agg_funcs]
                        
                        # Perform groupby analysis
                        grouped = df.groupby(group_col)[agg_cols].agg(selected_funcs)
                        st.dataframe(grouped)
                        
                        # Download option
                        csv = grouped.to_csv().encode('utf-8')
                        st.download_button(
                            "Download grouped data as CSV",
                            data=csv,
                            file_name='grouped_data.csv',
                            mime='text/csv',
                        )
                    else:
                        st.warning("Please select at least one aggregation function.")
                else:
                    st.warning("Please select at least one column to aggregate.")
            else:
                st.warning("Group analysis requires at least one categorical column and one numeric column.")
        
        elif analysis_type == "Distribution Analysis":
            if len(numeric_cols) > 0:
                dist_col = st.selectbox("Select column for distribution analysis", numeric_cols)
                
                fig, ax = plt.subplots(figsize=(10, 6))
                sns.histplot(df[dist_col], kde=True, ax=ax)
                plt.title(f"Distribution of {dist_col}")
                plt.tight_layout()
                st.pyplot(fig)
                
                # Additional distribution statistics
                st.write("**Distribution Statistics:**")
                dist_stats = pd.DataFrame({
                    "Statistic": ["Mean", "Median", "Std Dev", "Min", "Max", "Skewness", "Kurtosis"],
                    "Value": [
                        df[dist_col].mean(),
                        df[dist_col].median(),
                        df[dist_col].std(),
                        df[dist_col].min(),
                        df[dist_col].max(),
                        df[dist_col].skew(),
                        df[dist_col].kurtosis()
                    ]
                })
                st.dataframe(dist_stats)
                
                # Box plot
                fig2, ax2 = plt.subplots(figsize=(10, 4))
                sns.boxplot(x=df[dist_col], ax=ax2)
                plt.title(f"Box Plot of {dist_col}")
                plt.tight_layout()
                st.pyplot(fig2)
                
                # Check for outliers
                q1 = df[dist_col].quantile(0.25)
                q3 = df[dist_col].quantile(0.75)
                iqr = q3 - q1
                lower_bound = q1 - 1.5 * iqr
                upper_bound = q3 + 1.5 * iqr
                outliers = df[(df[dist_col] < lower_bound) | (df[dist_col] > upper_bound)]
                
                if not outliers.empty:
                    st.write(f"**Potential Outliers** (values < {lower_bound:.2f} or > {upper_bound:.2f}):")
                    st.dataframe(outliers)
                else:
                    st.write("No outliers detected based on IQR method.")
            else:
                st.warning("Distribution analysis requires at least one numeric column.")
    
    with analysis_tabs[2]:  # Raw Data
        st.subheader("Raw Data")
        
        # Show full dataset with filters
        filter_option = st.checkbox("Add filters")
        
        if filter_option:
            # Create filter settings for each column
            filters = {}
            for col in df.columns:
                with st.expander(f"Filter by {col}"):
                    if col in numeric_cols:
                        # Numeric filter
                        min_val = float(df[col].min())
                        max_val = float(df[col].max())
                        filters[col] = st.slider(f"Range for {col}", min_val, max_val, (min_val, max_val))
                    elif col in categorical_cols:
                        # Categorical filter
                        unique_vals = df[col].unique()
                        filters[col] = st.multiselect(f"Values for {col}", unique_vals, default=unique_vals)
                    elif col in date_cols:
                        # Date filter
                        df[col] = pd.to_datetime(df[col], errors='coerce')
                        min_date = df[col].min().date() if not pd.isna(df[col].min()) else None
                        max_date = df[col].max().date() if not pd.isna(df[col].max()) else None
                        
                        if min_date and max_date:
                            date_range = st.date_input(
                                f"Date range for {col}",
                                value=(min_date, max_date),
                                min_value=min_date,
                                max_value=max_date
                            )
                            if len(date_range) == 2:
                                filters[col] = date_range
            
            # Apply filters to create filtered dataframe
            filtered_df = df.copy()
            for col, filter_val in filters.items():
                if col in numeric_cols:
                    filtered_df = filtered_df[(filtered_df[col] >= filter_val[0]) & (filtered_df[col] <= filter_val[1])]
                elif col in categorical_cols and filter_val:  # Only apply if some values are selected
                    filtered_df = filtered_df[filtered_df[col].isin(filter_val)]
                elif col in date_cols and len(filter_val) == 2:
                    start_date, end_date = filter_val
                    filtered_df = filtered_df[
                        (filtered_df[col].dt.date >= start_date) & (filtered_df[col].dt.date <= end_date)
                    ]
            
            # Show filtered dataframe
            st.write(f"Showing {len(filtered_df)} of {len(df)} rows after filtering")
            st.dataframe(filtered_df)
            
            # Download filtered data
            csv = filtered_df.to_csv(index=False).encode('utf-8')
            st.download_button(
                "Download filtered data as CSV",
                data=csv,
                file_name='filtered_data.csv',
                mime='text/csv',
            )
        else:
            # Show full dataset
            st.dataframe(df)
            
            # Download full data
            csv = df.to_csv(index=False).encode('utf-8')
            st.download_button(
                "Download full data as CSV",
                data=csv,
                file_name='full_data.csv',
                mime='text/csv',
            )
else:
    st.warning("No data loaded yet. Please select a data source to begin.")
    
    # Show loading instructions
    st.info("""
        ### How to load data:
        1. Select a data source from the sidebar
        2. For AWS Athena: Configure database settings and run a query
        3. For local files: Upload a CSV or Parquet file
        4. For sample data: Click the "Use sample data" option
    """) 