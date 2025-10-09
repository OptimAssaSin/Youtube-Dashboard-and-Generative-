# import_legacy_data.py (Finalized Version)
import pandas as pd
from sqlalchemy import create_engine
import logging
import datetime
import os

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

workspace = os.environ.get('GITHUB_WORKSPACE')
DB_FILE = os.path.join(workspace, 'youtube_data.db') if workspace else 'youtube_data.db'

engine = create_engine(f'sqlite:///{DB_FILE}')

logging.info("Loading legacy CSV files...")
df_master = pd.read_csv('videos_master.csv')
df_daily = pd.read_csv('daily_stats.csv')

df_master.rename(columns={
    'video_published_at': 'published_at',
    'duration_seconds': 'duration'
}, inplace=True)

df_videos = df_master.drop_duplicates(subset=['video_id']).copy()

# Add ALL missing columns with default values to match the final schema
df_videos['description'] = ''
df_videos['thumbnail_url'] = ''
df_videos['channel_published_at'] = None
df_videos['channel_country'] = None
df_videos['channel_video_count'] = 0
df_videos['channel_keywords'] = ''
df_videos['channel_topic_categories'] = ''
df_videos['avg_comment_sentiment'] = 0.0
df_videos['comment_sentiment_std'] = 0.0
df_videos['avg_top_comment_replies'] = 0.0
df_videos['topic_categories'] = ''
df_videos['license'] = 'youtube'
df_videos['live_broadcast_content'] = 'none'
df_videos['default_language'] = None
df_videos['default_audio_language'] = None
df_videos['is_embeddable'] = True
df_videos['made_for_kids'] = False
df_videos['favorite_count'] = 0
df_videos['definition'] = 'hd'
df_videos['caption'] = 'false'
df_videos['licensed_content'] = False
df_videos['added_at'] = datetime.datetime.utcnow().isoformat()

# Select all columns in the correct order
df_videos = df_videos[[
    'video_id', 'published_at', 'channel_id', 'title', 'description', 'channel_title',
    'subscriber_count', 'channel_published_at', 'channel_country', 'channel_video_count',
    'channel_keywords', 'channel_topic_categories', 'avg_comment_sentiment',
    'comment_sentiment_std', 'avg_top_comment_replies', 'thumbnail_url', 'tags',
    'category_id', 'topic_categories', 'license', 'live_broadcast_content',
    'default_language', 'default_audio_language', 'is_embeddable', 'made_for_kids',
    'favorite_count', 'duration', 'definition', 'caption', 'licensed_content', 'added_at'
]]

df_stats = df_daily[['video_id', 'fetch_date', 'view_count', 'like_count', 'comment_count']].copy()
df_stats.rename(columns={'fetch_date': 'fetch_timestamp'}, inplace=True)

try:
    with engine.connect() as conn:
        df_videos.to_sql('videos', conn, if_exists='append', index=False)
        logging.info(f"Imported {len(df_videos)} unique video records.")
        df_stats.to_sql('statistics', conn, if_exists='append', index=False)
        logging.info(f"Imported {len(df_stats)} daily statistics records.")
except Exception as e:
    logging.error(f"Error importing legacy data: {e}")