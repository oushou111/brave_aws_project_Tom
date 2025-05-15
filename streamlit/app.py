import streamlit as st
import user_demographics
import user_churn_analysis
import class_attendance_analysis
import leaderboard_feature
import aws_connection

# Set page title and layout
st.set_page_config(page_title="Peak Fitness Analytics Dashboard", layout="wide")

# Main title for the dashboard
st.title("Peak Fitness Analytics Dashboard")
st.markdown("Welcome to the Peak Fitness comprehensive analytics dashboard. Select an analysis from the sidebar to view.")

# --- Sidebar Setup ---
st.sidebar.title("Navigation")

# Global AWS Connection Check (once per session)
if 'aws_connection_checked' not in st.session_state:
    st.session_state.aws_connection_checked = True
    aws_connected = aws_connection.test_aws_connection()
    if aws_connected:
        st.sidebar.success("AWS Connection: Connected ✅")
        st.session_state.force_data_source = "AWS Athena" # Signal to sub-modules
    else:
        st.sidebar.error("AWS Connection: Not Connected ❌")
        st.sidebar.warning("AWS unavailable. Dashboards may use cached/dummy data or fail to load.")
        st.session_state.force_data_source = "Dummy Data (Development)" # Signal to sub-modules
st.sidebar.markdown("---")

# Navigation options
dashboard_options = {
    "📊 User Demographics": user_demographics.display_demographics_dashboard,
    "📉 User Churn Analysis": user_churn_analysis.display_churn_dashboard,
    "📅 Class Attendance": class_attendance_analysis.display_attendance_dashboard,
    "🏆 Leaderboard": leaderboard_feature.display_leaderboard
}

selected_dashboard_label = st.sidebar.radio(
    "Select Dashboard:",
    options=list(dashboard_options.keys()),
    key="selected_dashboard_nav"
)
st.sidebar.markdown("---")

# --- Main Page Content based on Sidebar Selection ---
if selected_dashboard_label:
    # Call the selected dashboard's display function
    dashboard_function = dashboard_options[selected_dashboard_label]
    dashboard_function() # The individual dashboards should set their own st.title or st.header

# --- Global Sidebar Utilities (continued) ---
st.sidebar.markdown("**Global Utilities**")
st.sidebar.info("Each dashboard manages its own data fetching. Refresh options may be available within the specific dashboard's section in the sidebar when active.")

if st.sidebar.button("Clear All App Data Caches", key="clear_all_cache_main_app"):
    st.cache_data.clear()
    st.success("All app data caches have been cleared! Select a dashboard or refresh the page to reload data.")
    st.experimental_rerun() 