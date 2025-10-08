# scraper.py (Finalized Version with all features and batching fix)
import pandas as pd
from googleapiclient.discovery import build
from sqlalchemy import create_engine
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
import datetime
import logging
import os
import re

# --- Configuration & Setup ---
API_KEY = os.environ.get('YOUTUBE_API_KEY')
DB_FILE = 'youtube_data.db'
engine = create_engine(f'sqlite:///{DB_FILE}')
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
sentiment_analyzer = SentimentIntensityAnalyzer()

# --- Database Functions ---
def get_db_connection():
    return engine.connect()

def get_existing_video_ids(conn):
    try:
        return set(pd.read_sql('SELECT video_id FROM videos', conn)['video_id'].tolist())
    except Exception:
        return set()

# --- API Functions ---
def analyze_comment_threads(youtube, video_id):
    try:
        request = youtube.commentThreads().list(
            part="snippet", videoId=video_id, maxResults=20, order="relevance"
        )
        response = request.execute()
        
        sentiments, reply_counts = [], []
        for item in response.get('items', []):
            snippet = item['snippet']
            comment_text = snippet['topLevelComment']['snippet']['textOriginal']
            score = sentiment_analyzer.polarity_scores(comment_text)['compound']
            sentiments.append(score)
            reply_counts.append(snippet.get('totalReplyCount', 0))
            
        if not sentiments:
            return {'avg_sentiment': 0.0, 'sentiment_std': 0.0, 'avg_reply_count': 0.0}

        return {
            'avg_sentiment': round(sum(sentiments) / len(sentiments), 4),
            'sentiment_std': round(pd.Series(sentiments).std(ddof=0), 4),
            'avg_reply_count': round(sum(reply_counts) / len(reply_counts), 2)
        }
    except Exception as e:
        logging.warning(f"Could not fetch/analyze comments for video {video_id}: {e}")
        return {'avg_sentiment': 0.0, 'sentiment_std': 0.0, 'avg_reply_count': 0.0}

def fetch_new_video_details(youtube, new_ids):
    if not new_ids: return pd.DataFrame()

    # --- FIX: Batch the video details request in chunks of 50 ---
    video_items = []
    for i in range(0, len(new_ids), 50):
        chunk = new_ids[i:i+50]
        try:
            video_request = youtube.videos().list(
                part="snippet,contentDetails,status,topicDetails,statistics",
                id=",".join(chunk)
            )
            video_response = video_request.execute()
            video_items.extend(video_response.get('items', []))
        except Exception as e:
            logging.error(f"Error fetching video details for chunk {i//50 + 1}: {e}")

    if not video_items: return pd.DataFrame()

    # The rest of the function now works on the collected 'video_items'
    channel_ids = list({item['snippet']['channelId'] for item in video_items})
    channel_data = {}
    
    for i in range(0, len(channel_ids), 50):
        chunk = channel_ids[i:i+50]
        channel_request = youtube.channels().list(part="statistics,snippet,brandingSettings,topicDetails", id=",".join(chunk))
        channel_response = channel_request.execute()
        for item in channel_response.get('items', []):
            channel_data[item['id']] = {
                'subscriber_count': int(item['statistics'].get('subscriberCount', 0)),
                'video_count': int(item['statistics'].get('videoCount', 0)),
                'published_at': item['snippet'].get('publishedAt'),
                'country': item['snippet'].get('country'),
                'keywords': item.get('brandingSettings', {}).get('channel', {}).get('keywords', ''),
                'topic_categories': "|".join(item.get('topicDetails', {}).get('topicCategories', []))
            }

    video_details = []
    for item in video_items:
        video_id = item['id']
        channel_id = item['snippet']['channelId']
        
        comment_analysis = analyze_comment_threads(youtube, video_id)
        chan_info = channel_data.get(channel_id, {})
        
        details = {
            'video_id': video_id,
            'published_at': item['snippet']['publishedAt'],
            'channel_id': channel_id,
            'title': item['snippet']['title'],
            'description': item['snippet']['description'],
            'channel_title': item['snippet']['channelTitle'],
            'subscriber_count': chan_info.get('subscriber_count', 0),
            'channel_published_at': chan_info.get('published_at'),
            'channel_country': chan_info.get('country'),
            'channel_video_count': chan_info.get('video_count', 0),
            'channel_keywords': chan_info.get('keywords'),
            'channel_topic_categories': chan_info.get('topic_categories'),
            'avg_comment_sentiment': comment_analysis['avg_sentiment'],
            'comment_sentiment_std': comment_analysis['sentiment_std'],
            'avg_top_comment_replies': comment_analysis['avg_reply_count'],
            'thumbnail_url': item['snippet']['thumbnails'].get('high', {}).get('url'),
            'tags': "|".join(item['snippet'].get('tags', [])),
            'category_id': item['snippet']['categoryId'],
            'topic_categories': "|".join(item.get('topicDetails', {}).get('topicCategories', [])),
            'license': item['status']['license'],
            'live_broadcast_content': item['snippet']['liveBroadcastContent'],
            'default_language': item['snippet'].get('defaultLanguage'),
            'default_audio_language': item['snippet'].get('defaultAudioLanguage'),
            'is_embeddable': item['status']['embeddable'],
            'made_for_kids': item['status'].get('madeForKids', False),
            'favorite_count': int(item['statistics'].get('favoriteCount', 0)),
            'duration': item['contentDetails']['duration'],
            'definition': item['contentDetails']['definition'],
            'caption': item['contentDetails']['caption'],
            'licensed_content': item['contentDetails']['licensedContent'],
            'added_at': datetime.datetime.utcnow().isoformat()
        }
        video_details.append(details)
        
    return pd.DataFrame(video_details)

def fetch_stats_for_all_videos(youtube, all_ids):
    if not all_ids: return pd.DataFrame()
    stats_list = []
    for i in range(0, len(all_ids), 50):
        chunk = all_ids[i:i+50]
        request = youtube.videos().list(part="statistics", id=",".join(chunk))
        response = request.execute()
        timestamp = datetime.datetime.utcnow().isoformat()
        for item in response.get('items', []):
            stats = {
                'video_id': item['id'], 'fetch_timestamp': timestamp,
                'view_count': int(item['statistics'].get('viewCount', 0)),
                'like_count': int(item['statistics'].get('likeCount', 0)),
                'comment_count': int(item['statistics'].get('commentCount', 0))
            }
            stats_list.append(stats)
    return pd.DataFrame(stats_list)

def main():
    if not API_KEY:
        logging.error("API_KEY environment variable not found.")
        return
    youtube = build('youtube', 'v3', developerKey=API_KEY)
    
    with get_db_connection() as conn:
        existing_ids = get_existing_video_ids(conn)
        logging.info(f"Found {len(existing_ids)} existing videos in the database.")

        region_codes = ['IN', 'US', 'GB', 'JP', 'BR']
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