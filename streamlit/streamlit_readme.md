# Peak Fitness Analytics Dashboard

This Streamlit application allows you to fetch query results from Athena and visualize them. It also supports uploading local data files for analysis.

## Features

- Connect directly to AWS Athena to execute SQL queries
- Support uploading CSV and Parquet files
- Provide multiple visualization types: bar charts, line charts, scatter plots, pie charts, and heatmaps
- Support exporting analysis results in CSV or Excel format

## Installation

1. Clone the repository
```bash
git clone <repository-url>
cd <repository-directory>
```

2. Install dependencies
```bash
pip install -r requirements.txt
```

## Configure AWS Credentials

Before using the Athena query function, you need to configure AWS credentials. There are several ways:

1. Configure AWS CLI locally:
```bash
aws configure
```

2. Set environment variables:
```bash
export AWS_ACCESS_KEY_ID=<your-access-key>
export AWS_SECRET_ACCESS_KEY=<your-secret-key>
export AWS_REGION=us-east-1
```

3. Configure in an `.env` file (requires installing the dotenv package)

## Run Application

```bash
streamlit run app.py
```

The application will start locally, usually accessible at http://localhost:8501.

## Usage Instructions

1. **Query from Athena**:
   - Enter Athena database name
   - Enter S3 output location
   - Write SQL query
   - Click the "Execute Query" button

2. **Upload local file**:
   - Select file type (CSV or Parquet)
   - Upload the file

3. **Use sample data**:
   - Directly use preloaded sample data

4. **Data Visualization**:
   - Select visualization type
   - Configure chart parameters
   - View the generated chart

5. **Export Results**:
   - Select export format
   - Click the "Export Data" button to download the file

## Example SQL Queries

```sql
-- Query the number of events in the past 7 days
SELECT date_trunc('day', event_ts) as day, count(*) as event_count
FROM events
WHERE event_ts >= current_date - interval '7' day
GROUP BY 1
ORDER BY 1

-- Query the distribution of different event types
SELECT event_type, count(*) as count
FROM events
GROUP BY event_type
ORDER BY count DESC
```

## Notes

- Athena queries consume AWS resources and may incur costs
- For large datasets, limit the number of rows returned to avoid memory issues
- Use WHERE clauses and the LIMIT keyword to optimize queries 