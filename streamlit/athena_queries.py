"""
Peak Fitness Athena Query Examples

Contains multiple commonly used SQL queries that can be used directly in Streamlit applications
"""

# Basic queries
BASIC_QUERIES = {
    "Total Event Count": """
        SELECT COUNT(*) AS total_events
        FROM events
        LIMIT 10
    """,
    
    "Event Type Distribution": """
        SELECT event_type, COUNT(*) AS event_count
        FROM events
        GROUP BY event_type
        ORDER BY event_count DESC
        LIMIT 10
    """,
    
    "Recent Event List": """
        SELECT event_id, event_type, event_ts
        FROM events
        ORDER BY event_ts DESC
        LIMIT 100
    """
}

# Time analysis queries
TIME_ANALYSIS_QUERIES = {
    "Daily Event Trends": """
        SELECT date_trunc('day', event_ts) AS day,
               COUNT(*) AS event_count
        FROM events
        WHERE event_ts >= current_date - interval '30' day
        GROUP BY 1
        ORDER BY 1
    """,
    
    "Hourly Event Distribution": """
        SELECT EXTRACT(hour FROM event_ts) AS hour_of_day,
               COUNT(*) AS event_count
        FROM events
        WHERE event_ts >= current_date - interval '7' day
        GROUP BY 1
        ORDER BY 1
    """,
    
    "Weekly Event Trends": """
        SELECT date_trunc('week', event_ts) AS week,
               COUNT(*) AS event_count
        FROM events
        WHERE event_ts >= current_date - interval '90' day
        GROUP BY 1
        ORDER BY 1
    """,
    
    "Most Popular Class Days": """
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
        ORDER BY total_sessions DESC
    """,
    
    "Most Popular Class Times": """
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
            event_ts,
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
        ORDER BY total_sessions DESC
    """
}

# User behavior analysis
USER_BEHAVIOR_QUERIES = {
    "Active User Ranking": """
        SELECT user_id, COUNT(*) AS activity_count
        FROM events
        WHERE event_ts >= current_date - interval '30' day
        GROUP BY user_id
        ORDER BY activity_count DESC
        LIMIT 20
    """,
    
    "User Event Type Preferences": """
        SELECT user_id, event_type, COUNT(*) AS type_count
        FROM events
        WHERE event_ts >= current_date - interval '30' day
        GROUP BY user_id, event_type
        ORDER BY user_id, type_count DESC
        LIMIT 100
    """,
    
    "User Engagement": """
        SELECT user_id,
               COUNT(DISTINCT date_trunc('day', event_ts)) AS active_days,
               COUNT(*) AS total_events,
               COUNT(*) / COUNT(DISTINCT date_trunc('day', event_ts)) AS events_per_day
        FROM events
        WHERE event_ts >= current_date - interval '30' day
        GROUP BY user_id
        HAVING COUNT(DISTINCT date_trunc('day', event_ts)) > 1
        ORDER BY events_per_day DESC
        LIMIT 20
    """,
    
    "90-Day User Churn Analysis": """
        -- Step 1: Get the latest event per user-session pair
        WITH latest_event AS (
          SELECT 
            user_id,
            session_id,
            event_ts,
            event_type,
            ROW_NUMBER() OVER (PARTITION BY user_id, session_id ORDER BY processed_time DESC) AS rn
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
          SELECT 
            user_id,
            MAX(activity_time) AS last_activity_time
          FROM all_attendance_union
          GROUP BY user_id
        ),
        -- Step 5: Determine user churn status (no activity in last 90 days)
        user_churn_status AS (
          SELECT
            user_id,
            last_activity_time,
            CASE 
              WHEN last_activity_time < CURRENT_DATE - INTERVAL '90' DAY THEN 'Churned'
              ELSE 'Active'
            END AS churn_status
          FROM last_activity_per_user
        )
        -- Final result
        SELECT 
          churn_status,
          COUNT(*) AS user_count,
          ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS percentage
        FROM user_churn_status
        GROUP BY churn_status
        ORDER BY user_count DESC
    """
}

# 会员分析
MEMBER_ANALYSIS_QUERIES = {
    "课程出勤率": """
        SELECT class_id, 
               class_name,
               COUNT(DISTINCT user_id) AS attendance_count
        FROM class_attendance
        WHERE class_date >= current_date - interval '30' day
        GROUP BY class_id, class_name
        ORDER BY attendance_count DESC
        LIMIT 20
    """,
    
    "会员续订率": """
        SELECT membership_type,
               COUNT(CASE WHEN renewal_status = 'renewed' THEN 1 END) AS renewed,
               COUNT(*) AS total,
               COUNT(CASE WHEN renewal_status = 'renewed' THEN 1 END) * 100.0 / COUNT(*) AS renewal_rate
        FROM membership_renewals
        WHERE renewal_date >= current_date - interval '90' day
        GROUP BY membership_type
        ORDER BY renewal_rate DESC
    """,
    
    "会员流失预测": """
        SELECT user_id,
               last_activity_date,
               CURRENT_DATE - last_activity_date AS days_inactive,
               CASE 
                  WHEN CURRENT_DATE - last_activity_date > 30 THEN 'High Risk'
                  WHEN CURRENT_DATE - last_activity_date > 14 THEN 'Medium Risk'
                  ELSE 'Low Risk'
               END AS churn_risk
        FROM (
            SELECT user_id, MAX(event_ts) AS last_activity_date
            FROM events
            GROUP BY user_id
        )
        ORDER BY days_inactive DESC
        LIMIT 100
    """
}

# 设施使用分析
FACILITY_USAGE_QUERIES = {
    "设施使用率": """
        SELECT facility_id, 
               facility_name,
               COUNT(*) AS usage_count
        FROM facility_usage
        WHERE usage_date >= current_date - interval '30' day
        GROUP BY facility_id, facility_name
        ORDER BY usage_count DESC
    """,
    
    "设施高峰时段": """
        SELECT EXTRACT(hour FROM usage_time) AS hour_of_day,
               facility_id,
               facility_name,
               COUNT(*) AS usage_count
        FROM facility_usage
        WHERE usage_date >= current_date - interval '14' day
        GROUP BY 1, 2, 3
        ORDER BY facility_id, usage_count DESC
    """,
    
    "设施使用趋势": """
        SELECT date_trunc('day', usage_date) AS day,
               facility_id,
               facility_name,
               COUNT(*) AS usage_count
        FROM facility_usage
        WHERE usage_date >= current_date - interval '30' day
        GROUP BY 1, 2, 3
        ORDER BY facility_id, day
    """
}

# 门店分析
LOCATION_ANALYSIS_QUERIES = {
    "门店出勤趋势": """
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
            event_ts,
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
        ORDER BY year, month, total_attendance DESC
    """
}

# Leaderboard Queries
LEADERBOARD_QUERIES = {
    "User Class Engagement Leaderboard (April 2025)": """
WITH
  historical_attendance AS (
    SELECT
      user_id,
      session_id,
      class_datetime
    FROM fact_class_attendance
    WHERE
      CAST(class_datetime AS DATE) >= DATE '2025-04-01' 
      AND CAST(class_datetime AS DATE) < DATE '2025-05-01'
  ),
  recent_events AS (
    SELECT
      user_id,
      session_id,
      class_datetime,
      event_type,
      event_ts,
      processed_time,
      ROW_NUMBER() OVER (PARTITION BY user_id, session_id ORDER BY event_ts DESC, processed_time DESC) as rn
    FROM fact_event
    WHERE
      event_ts >= TIMESTAMP '2025-04-01 00:00:00' 
      AND event_ts < TIMESTAMP '2025-05-01 00:00:00'
      AND event_type IN ('signup', 'cancel')
  ),
  valid_recent_engagement AS (
    SELECT
      user_id,
      session_id,
      class_datetime
    FROM recent_events
    WHERE
      rn = 1 AND event_type = 'signup'
  ),
  all_engagements AS (
    SELECT user_id, session_id, class_datetime FROM historical_attendance
    UNION ALL 
    SELECT user_id, session_id, class_datetime FROM valid_recent_engagement
  ),
  user_engagement_counts AS (
    SELECT
      user_id,
      COUNT(DISTINCT session_id) AS total_classes_attended 
    FROM all_engagements
    GROUP BY
      user_id
  )
SELECT
  uec.user_id,
  du.first_name,
  du.last_name,
  uec.total_classes_attended
FROM user_engagement_counts uec
JOIN dim_user_v3 du ON uec.user_id = du.user_id
ORDER BY
  uec.total_classes_attended DESC
LIMIT 20;
"""
}

# 所有查询的集合
ALL_QUERIES = {
    "Basic Queries": BASIC_QUERIES,
    "Time Analysis": TIME_ANALYSIS_QUERIES,
    "User Behavior Analysis": USER_BEHAVIOR_QUERIES,
    "Member Analysis": MEMBER_ANALYSIS_QUERIES,
    "Facility Usage Analysis": FACILITY_USAGE_QUERIES,
    "Location Analysis": LOCATION_ANALYSIS_QUERIES,
    "Leaderboard Queries": LEADERBOARD_QUERIES
}

def get_query_categories():
    """
    Get all available query categories
    
    Returns:
    - list: Names of available query categories
    """
    return list(ALL_QUERIES.keys())

def get_queries_in_category(category):
    """
    Get all queries in a specific category
    
    Parameters:
    - category: Name of the query category
    
    Returns:
    - dict: Dictionary of queries in the specified category
    """
    return ALL_QUERIES.get(category, {})

def get_query(category, query_name):
    """
    Get specific query by category and name
    
    Parameters:
    - category: Name of the query category
    - query_name: Name of the query
    
    Returns:
    - str: SQL query string
    """
    category_queries = get_queries_in_category(category)
    return category_queries.get(query_name, "") 