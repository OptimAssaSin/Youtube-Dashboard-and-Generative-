
import pandas as pd
from googleapiclient.discovery import build
from sqlalchemy import create_engine
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
import datetime
import logging
import os
import re

# --- All helper functions (SentimentIntensityAnalyzer, get_db_connection, get_existing_video_ids,
# --- analyze_comment_threads, fetch_new_video_details, fetch_stats_for_all_videos)
# --- remain exactly the same as the previous version. For brevity, they are not repeated here.
# --- Assume they are present above this main function.

# (Paste all the previous functions here)
# ...

def main():
    if not API_KEY:
        logging.error("API_KEY environment variable not found.")
        return
    youtube = build('youtube', 'v3', developerKey=API_KEY)
    
    with get_db_connection() as conn:
        existing_ids = get_existing_video_ids(conn)
        logging.info(f"Found {len(existing_ids)} existing videos in the database.")

        # --- MODIFICATION START ---
        # Define a list of regions to fetch trending videos from
        region_codes = ['IN', 'US', 'GB', 'JP', 'BR'] # India, US, Great Britain, Japan, Brazil
        all_trending_ids = set()

        logging.info(f"Fetching trending videos for regions: {region_codes}")
        for region in region_codes:
            try:
                trending_req = youtube.videos().list(
                    part="id", 
                    chart="mostPopular", 
                    regionCode=region, 
                    maxResults=50
                )
                trending_res = trending_req.execute()
                region_ids = {item['id'] for item in trending_res.get('items', [])}
                all_trending_ids.update(region_ids)
                logging.info(f"Found {len(region_ids)} videos for region '{region}'.")
            except Exception as e:
                logging.error(f"Could not fetch trending videos for region '{region}': {e}")
        
        logging.info(f"Total unique trending videos found across all regions: {len(all_trending_ids)}")
        # --- MODIFICATION END ---
        
        new_ids = list(all_trending_ids - existing_ids)
        
        if new_ids:
            logging.info(f"Found {len(new_ids)} new videos to process.")
            df_new_details = fetch_new_video_details(youtube, new_ids)
            if not df_new_details.empty:
                df_new_details.to_sql('videos', conn, if_exists='append', index=False)
                logging.info(f"Saved details for {len(df_new_details)} new videos.")
        else:
            logging.info("No new trending videos found across specified regions.")
            
        all_tracked_ids = list(existing_ids.union(all_trending_ids))
        if all_tracked_ids:
            logging.info(f"Fetching latest stats for {len(all_tracked_ids)} total videos.")
            df_stats = fetch_stats_for_all_videos(youtube, all_tracked_ids)
            if not df_stats.empty:
                df_stats.to_sql('statistics', conn, if_exists='append', index=False)
                logging.info(f"Saved {len(df_stats)} new statistics records.")

if __name__ == '__main__':
    main()