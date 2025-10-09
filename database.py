# setup_database.py (Finalized Version)
import sqlite3
import os

workspace = os.environ.get('GITHUB_WORKSPACE')
DB_FILE = os.path.join(workspace, 'youtube_data.db') if workspace else 'youtube_data.db'


conn = sqlite3.connect(DB_FILE)
cursor = conn.cursor()

cursor.execute('''
CREATE TABLE IF NOT EXISTS videos (
    video_id TEXT PRIMARY KEY,
    published_at TEXT,
    channel_id TEXT,
    title TEXT,
    description TEXT,
    channel_title TEXT,
    subscriber_count INTEGER,
    channel_published_at TEXT,
    channel_country TEXT,
    channel_video_count INTEGER,
    channel_keywords TEXT,          -- ADDED
    channel_topic_categories TEXT, -- ADDED
    avg_comment_sentiment REAL,
    comment_sentiment_std REAL,
    avg_top_comment_replies REAL, -- ADDED
    thumbnail_url TEXT,
    tags TEXT,
    category_id TEXT,
    topic_categories TEXT,
    license TEXT,
    live_broadcast_content TEXT, -- ADDED
    default_language TEXT,       -- ADDED
    default_audio_language TEXT, -- ADDED
    is_embeddable BOOLEAN,       -- ADDED
    made_for_kids BOOLEAN,       -- ADDED
    favorite_count INTEGER,      -- ADDED
    duration TEXT,
    definition TEXT,
    caption TEXT,
    licensed_content BOOLEAN,
    added_at TEXT
)
''')

cursor.execute('''
CREATE TABLE IF NOT EXISTS statistics (
    video_id TEXT,
    fetch_timestamp TEXT,
    view_count INTEGER,
    like_count INTEGER,
    comment_count INTEGER,
    FOREIGN KEY(video_id) REFERENCES videos(video_id)
)
''')

conn.commit()
conn.close()
print("Final database schema created successfully.")