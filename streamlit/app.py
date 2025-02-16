import streamlit as st
import pandas as pd
import plotly.express as px
from pathlib import Path
import json
from datetime import datetime

# Page config
st.set_page_config(
    page_title="Marvel Characters Count Dashboard",
    page_icon="🦸‍♂️",
    layout="wide"
)

# Constants
DATA_PATH = Path("/app/data/marvel")
ANALYTICS_PATH = DATA_PATH / "analytics" / "latest"
VALIDATION_PATH = DATA_PATH / "logs" 

def load_data():
    """Load the latest character data and refresh info"""
    try:
        # Read character data
        df = pd.read_csv(ANALYTICS_PATH / "characters.csv")
        
        # Convert character_id to string
        df['character_id'] = df['character_id'].astype(str)
        df['last_updated'] = df['last_updated'].astype(str)

        
        # Read refresh info
        with open(ANALYTICS_PATH / "last_refresh.json", 'r') as f:
            refresh_info = json.load(f)
            
            # Format the last_update date
            if 'last_update' in refresh_info:
                try:
                    # Assuming last_update is in YYYYMMDD format
                    last_update = datetime.strptime(str(refresh_info['last_update']), '%Y%m%d')
                    refresh_info['last_update'] = last_update.strftime('%Y-%m-%d')
                except ValueError:
                    # Keep original if parsing fails
                    pass
            
        return df, refresh_info
    except Exception as e:
        st.error(f"Error loading data: {str(e)}")
        return None, None

# def load_quality_metrics():
#     """Load quality metrics from validation results"""
#     try:
#         # Get all validation issues files
#         issues_files = sorted(VALIDATION_PATH.glob("/*/validation_issues.json"))
        
#         all_issues = []
#         for file_path in issues_files:
#             with open(file_path, 'r') as f:
#                 issues_data = json.load(f)
#                 all_issues.extend(issues_data['issues'])
        
#         # Add date field to each issue
#         for issue in all_issues:
#             issue['date'] = issue['timestamp'][:10]  # Extract date from timestamp
        
#         return all_issues

#     except Exception as e:
#         st.error(f"Error loading quality metrics: {str(e)}")
#         return None
def main():
    # Add tabs for main dashboard and quality metrics
    # tab1, tab2 = st.tabs(["Main Dashboard", "Quality Metrics"])
    # with tab1:
    # Header
    st.title("🦸‍♂️ Marvel Characters Dashboard")
    
    # Load data
    df, refresh_info = load_data()
    
    if df is not None and refresh_info is not None:
        # Show refresh information
        st.sidebar.info(
            f"Last Updated: {refresh_info['last_update']}\n\n"
            f"Total Characters: {refresh_info['record_count']}"
        )
        
        # Overview metrics
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Total Characters", len(df))
        with col2:
            st.metric("Average Comics per Character", round(df['comic_count'].mean(), 2))
        with col3:
            st.metric("Max Comics", df['comic_count'].max())
        
        # Top Characters by Comic Count
        st.subheader("Top Characters by Comic Appearances")
        top_chars = df.nlargest(10, 'comic_count')
        fig = px.bar(
            top_chars,
            x='name',
            y='comic_count',
            title="Top 10 Characters by Comic Appearances"
        )
        fig.update_layout(xaxis_title="Character", yaxis_title="Number of Comics")
        st.plotly_chart(fig, use_container_width=True)
        
        # Comic Count Distribution
        st.subheader("Comic Count Distribution")
        fig = px.histogram(
            df,
            x='comic_count',
            nbins=50,
            title="Distribution of Comic Appearances"
        )
        fig.update_layout(xaxis_title="Number of Comics", yaxis_title="Number of Characters")
        st.plotly_chart(fig, use_container_width=True)
        
        # Character Search
        st.subheader("Character Search")
        search_name = st.text_input("Search for a character")
        if search_name:
            filtered_df = df[df['name'].str.contains(search_name, case=False)]
            if not filtered_df.empty:
                st.dataframe(filtered_df)
            else:
                st.info("No characters found matching your search.")
        
        # Raw Data View and Download Section
        st.subheader("Data Download and View")
        col1, col2 = st.columns(2)
        
        with col1:
            if st.checkbox("Show Raw Data"):
                st.dataframe(df)
                
        with col2:
            # Download full dataset
            st.download_button(
                label="Download Full Dataset",
                data=df.to_csv(index=False),
                file_name=f"marvel_characters_{datetime.now().strftime('%Y%m%d')}.csv",
                mime="text/csv"
            )
            
            # Download filtered dataset if search is active
            if search_name and not filtered_df.empty:
                st.download_button(
                    label="Download Filtered Dataset",
                    data=filtered_df.to_csv(index=False),
                    file_name=f"marvel_characters_filtered_{datetime.now().strftime('%Y%m%d')}.csv",
                    mime="text/csv"
                )
    
    else:
        st.warning("No data available. Please ensure the ETL pipeline has been run.")

    # with tab2:
    #     # In the "Quality Metrics" tab
    #     st.header("Data Quality Issues")

    #     # Load quality metrics
    #     all_issues = load_quality_metrics()

    #     if all_issues:
    #         # Get unique dates from issues
    #         dates = sorted(set(issue['date'] for issue in all_issues))
            
    #         # Add date filter dropdown
    #         selected_date = st.selectbox("Select Date", ["All"] + dates)
            
    #         # Filter issues based on selected date
    #         if selected_date == "All":
    #             filtered_issues = all_issues
    #         else:
    #             filtered_issues = [issue for issue in all_issues if issue['date'] == selected_date]
            
    #         if filtered_issues:
    #             # Display filtered issues in a table
    #             issues_df = pd.DataFrame(filtered_issues)
    #             st.dataframe(issues_df)
    #         else:
    #             st.info("No validation issues found for the selected date.")
                
    #     else:
    #         st.warning("No validation issues data available.")

if __name__ == "__main__":
    main()