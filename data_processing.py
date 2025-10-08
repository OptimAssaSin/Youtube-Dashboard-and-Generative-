# process_data.py (Corrected for Timezone and FutureWarnings)
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
import logging
import re

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

DB_FILE = 'youtube_data.db'
engine = create_engine(f'sqlite:///{DB_FILE}')

# --- 1. LOAD DATA FROM DATABASE ---
logging.info("Loading data from database...")
with engine.connect() as conn:
    df_videos = pd.read_sql('SELECT * FROM videos', conn)
    df_stats = pd.read_sql('SELECT * FROM statistics', conn)
df = pd.merge(df_videos, df_stats, on='video_id')

# --- 2. CLEAN & PREPROCESS DATA ---
logging.info("Cleaning and preprocessing data...")
# Convert dates and handle potential errors
df['published_at'] = pd.to_datetime(df['published_at'], errors='coerce')
df['fetch_timestamp'] = pd.to_datetime(df['fetch_timestamp'], errors='coerce')
df['channel_published_at'] = pd.to_datetime(df['channel_published_at'], errors='coerce')

# --- FIX 1: Make both timestamp columns timezone-aware (UTC) ---
df['fetch_timestamp'] = df['fetch_timestamp'].dt.tz_localize('UTC')
df['published_at'] = df['published_at'].dt.tz_localize('UTC')
df['channel_published_at'] = df['channel_published_at'].dt.tz_localize('UTC')

df.dropna(subset=['published_at', 'fetch_timestamp'], inplace=True)

# Clean text fields
# --- FIX 2: Use modern syntax for fillna to avoid warnings ---
for col in ['tags', 'title', 'description', 'channel_keywords', 'topic_categories']:
    df[col] = df[col].fillna('')
    df[col] = df[col].str.lower().str.replace('[^a-z0-9\s|]', '', regex=True).str.replace('|', ' ')

# Parse duration to seconds
def parse_duration(duration_str):
    if not isinstance(duration_str, str) or not duration_str.startswith('PT'):
        try: return int(float(duration_str))
        except: return 0
    seconds = 0
    parts = re.findall(r'(\d+)([HMS])', duration_str)
    for value, unit in parts:
        value = int(value)
        if unit == 'H': seconds += value * 3600
        elif unit == 'M': seconds += value * 60
        elif unit == 'S': seconds += value
    return seconds

df['duration_seconds'] = df['duration'].apply(parse_duration)

# --- 3. FEATURE ENGINEERING ---
logging.info("Engineering features and labels...")
# Time-based features (This will now work correctly)
df['days_since_published'] = (df['fetch_timestamp'] - df['published_at']).dt.days
df['channel_age_days'] = (df['published_at'] - df['channel_published_at']).dt.days
df['views_per_day'] = df['view_count'] / (df['days_since_published'] + 1)

# Text-based features
df['title_length'] = df['title'].str.len()
df['tag_count'] = df['tags'].str.split().str.len()

# Handle Categorical Features
df['license'] = df['license'].astype('category')
df['live_broadcast_content'] = df['live_broadcast_content'].astype('category')

# Fill NaN values for new numeric features
for col in ['avg_comment_sentiment', 'comment_sentiment_std', 'avg_top_comment_replies', 'favorite_count', 'channel_video_count']:
    df[col].fillna(0, inplace=True)

# --- Create Target Labels for ML Models ---
logging.info("Calculating target labels for models...")
df['peak_view_count'] = df.groupby('video_id')['view_count'].transform('max')
view_threshold = df['peak_view_count'].quantile(0.75)
df['will_trend'] = np.where(df['peak_view_count'] >= view_threshold, 1, 0)

peak_view_idx = df.groupby('video_id')['view_count'].idxmax()
df_peaks = df.loc[peak_view_idx][['video_id', 'days_since_published']].rename(columns={'days_since_published': 'days_to_peak'})
df = pd.merge(df, df_peaks, on='video_id', how='left')

bins = df['peak_view_count'].quantile([0, 0.5, 0.75, 0.9, 1.0])
labels = ['Standard', 'Popular', 'High-Performing', 'Viral']
df['performance_bucket'] = pd.cut(df['peak_view_count'], bins=bins.values, labels=labels, include_lowest=True)

# --- 4. SAVE FINAL DATASETS ---
df.to_parquet('final_processed_data.parquet', index=False)
logging.info("Final processed data saved to 'final_processed_data.parquet'.")

# --- 5. CREATE CORPUS FOR GPT-2 ---
df_unique_videos = df.drop_duplicates(subset=['video_id'])
corpus_text = (df_unique_videos['title'] + ' ' + df_unique_videos['tags']).tolist()
with open('corpus.txt', 'w', encoding='utf-8') as f:
    for line in corpus_text:
        f.write(str(line) + '\n')
logging.info("Text corpus for generative model saved to 'corpus.txt'.")