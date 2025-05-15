# Peak Fitness Analytics Dashboard

A comprehensive fitness data analytics dashboard built with Streamlit to visualize user demographics, class attendance analysis, and facility performance.

## Features

- **User Demographics**: Analyze user age distribution and gender distribution
- **Class Attendance Analysis**: Analyze most popular classes, class times, and day preferences
- **Facility Performance Analysis**: Analyze attendance trends across different locations
- **User Churn Analysis**: Identifies users inactive for over 90 days based on last activity (signup or class attendance), provides a downloadable list of these users with inactivity details, and visualizes inactivity periods.
- **Leaderboard Feature**: Displays a user engagement leaderboard, ranking users by total classes attended in a specific period (e.g., April 2025).
- **Data Caching**: Cache query results to avoid running time-consuming queries repeatedly
- **Offline Development Mode**: Provides mock data for development and testing without AWS connection

## Environment Setup

### System Requirements

- Python 3.8+
- AWS account and access credentials (for using AWS Athena)

### Installation Instructions

1. Clone the repository or download the code locally

2. Install dependencies:

```bash
pip install -r requirements.txt
```

3. (Optional) Configure AWS credentials:
   - Create a `.env` file with the following content:

```
AWS_ACCESS_KEY_ID=your_access_key
AWS_SECRET_ACCESS_KEY=your_secret_key
AWS_REGION=your_region (default: us-east-1)
ATHENA_DATABASE=your_athena_database_name
ATHENA_S3_OUTPUT=s3://your-bucket/athena-results/
```

**Important Security Note:** Ensure that files containing sensitive credentials (like `.env` or `# AWS Credentials.txt`) are listed in your `.gitignore` file and are NEVER committed to your Git repository.

## Usage Instructions

### Launching the Dashboard

The primary dashboard can typically be launched using `main_dashboard.py`:

```bash
streamlit run main_dashboard.py
```

Alternatively, `app.py` provides another interface, particularly for direct Athena queries and file uploads. You can run it via:

```bash
streamlit run app.py 
```
(See `streamlit_readme.md` for more details on `app.py`.)

### Application Structure

This application consists of the following main files:

- `main_dashboard.py`: Integrates various analysis modules (User Demographics, Class Attendance, Churn, Leaderboard) into a unified multi-page dashboard. This is likely the primary intended interface for viewing comprehensive analytics.
- `app.py`: Provides an interface for direct Athena querying, local file uploads (CSV/Parquet), and sample data usage. See `streamlit_readme.md` for specific documentation on this script.
- `fitness_analytics_dashboard.py`: (Role to be clarified - may be an older version or a specific-purpose dashboard. Evaluate if needed.)
- `user_demographics.py`: User demographics analysis module.
- `class_attendance_analysis.py`: Class attendance analysis module.
- `user_churn_analysis.py`: User churn analysis module.
- `leaderboard_feature.py`: Leaderboard display module.
- `aws_connection.py`: AWS connection and authentication module.
- `athena_queries.py`: Stores and manages SQL queries for Athena.
- `data_model.md`: Describes the data models and database schema used by the application.
- `streamlit_readme.md`: A specific README providing detailed instructions for `app.py`.

### Dashboard Navigation

The application contains the following main views:

1. **Overview**: Displays key performance indicators and summary of main trends (Verify if Overview still exists or if it starts directly with other views)
2. **User Demographics**: In-depth analysis of user age and gender distribution
3. **Class Attendance Analysis**: Analysis of class preferences, time slots, and days
4. **User Activity & Churn**: Analysis of user inactivity and churn.
5. **App Leaderboard**: Displays user engagement leaderboard.

### Data Source Options

The dashboard supports two data source modes:

- **AWS Athena**: Retrieves real-time data from AWS Athena database (recommended for production)
- **Dummy Data (Development)**: Uses built-in mock data for development and testing

## Using Real Data from AWS Athena

For production use, it's strongly recommended to use real data from AWS Athena:

1. Ensure you have an AWS account with access to Athena service
2. Set up your AWS credentials as described in the installation section
3. In the dashboard, select "AWS Athena" as the data source
4. If your credentials are valid, the dashboard will automatically query real data

### Athena Database Configuration

The application is configured to work with the following Athena tables:

- `dim_user_v3`: User dimension table
- `fact_class_attendance`: Class attendance fact table
- `fact_event`: Event fact table
- `dim_class_session`: Class session dimension table
- `dim_location`: Facility location dimension table

Ensure your Athena database has these tables or update the queries in the code to match your schema.

## Data Caching Mechanism

To improve performance and reduce repeated queries to AWS resources:

- Query results are cached in the `./data_cache/` directory
- Default cache duration is 1 hour
- Users can manually clear the cache using the "Refresh Data" button in the dashboard

## AWS Configuration

When querying AWS Athena, the following configurations are needed:

1. AWS access credentials (via environment variables or in-app input)
2. In `aws_connection.py`, configure:
   - Database name
   - S3 output location
   - Region settings

You can also configure these settings directly in the Streamlit interface under the AWS Configuration panel in the sidebar.

## Development Guide

### Adding New Visualizations

To add new visualizations:

1. Create a new analysis function (refer to existing visualization functions)
2. Add new SQL queries (if needed)
3. Add visualization logic in the appropriate module
4. Integrate the feature in `main_dashboard.py`

### Data Model

The dashboard uses the following main data tables:

- `dim_user_v3`: User dimension table
- `fact_class_attendance`: Class attendance fact table
- `fact_event`: Event fact table
- `dim_class_session`: Class session dimension table
- `dim_location`: Facility location dimension table

## Troubleshooting

### Common Issues

1. **AWS Connection Issues**:
   - Check if AWS credentials are valid
   - Ensure you have sufficient permissions to access Athena and S3
   - Verify that the specified S3 output location exists and is writable

2. **Cache Issues**:
   - Delete files in the cache directory
   - Click the "Refresh Data" button in the dashboard

3. **Module Import Errors**:
   - Make sure all module files are in the same directory

4. **Athena Query Errors**:
   - Verify that your Athena tables match the expected schema
   - Check the query syntax in the code if you've modified the database structure

## Future Development Plans

- Implement user churn analysis module
- Add predictive analytics features
- Add export and reporting functionality
- Expand data source connection options 