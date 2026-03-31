from pathlib import Path
import os
import pandas as pd

import re
import emoji

# ================= CONFIG =================
n_comments = 3           # max number of comments per post
text_preview_len = 80    # truncate long text for display

# ================= PROJECT ROOT & DATA PATHS =================
try:
    PROJECT_ROOT = Path(__file__).resolve().parent
except NameError:
    PROJECT_ROOT = Path.cwd()  # fallback if running interactively

DEFAULT_DATA_DIR = Path(r"C:/Users/DELL/Documents/project_data/output")
DATA_DIR = Path(os.getenv("DATA_DIR", DEFAULT_DATA_DIR))

POSTS_PATH = DATA_DIR / "safaricom_posts.csv"
COMMENTS_PATH = DATA_DIR / "safaricom_comments22.csv"

# ================= FILE EXISTENCE CHECK =================
print("Using DATA_DIR:", DATA_DIR)
if not POSTS_PATH.exists():
    raise FileNotFoundError(f"Posts file not found: {POSTS_PATH}")
if not COMMENTS_PATH.exists():
    raise FileNotFoundError(f"Comments file not found: {COMMENTS_PATH}")

# ================= LOAD CSVs =================
posts = pd.read_csv(POSTS_PATH)
comments = pd.read_csv(COMMENTS_PATH)

# ------------------ BEFORE PREPROCESSING ------------------
print("\n=== BEFORE PREPROCESSING ===")
print(f"Posts shape: {posts.shape}")
print(f"Comments shape: {comments.shape}")
print("\nMissing values per column (posts):")
print(posts.isna().sum())
print("\nMissing values per column (comments):")
print(comments.isna().sum())
print(f"\nDuplicate posts: {posts.duplicated(subset='msg_id').sum()}")
print(f"Duplicate comments: {comments.duplicated(subset='comment_id').sum()}")

# Checking data types
print("\nData types in Posts:")
print(posts.dtypes)
print("\nData types in Comments:")
print(comments.dtypes)

# ================= DEDUPLICATE =================
posts = posts.drop_duplicates(subset='msg_id')
comments = comments.drop_duplicates(subset='comment_id')

# ================= FILL MISSING VALUES =================
posts['views'] = posts['views'].fillna(0)
posts['forwards'] = posts['forwards'].fillna(0)
posts['replies'] = posts['replies'].fillna(0)
posts['media_type'] = posts['media_type'].fillna('Unknown')
posts['text'] = posts['text'].fillna('<media_only>')
posts['reply_to_msg_id'] = posts['reply_to_msg_id'].fillna('None')

comments['text'] = comments['text'].fillna('<deleted>')
comments['sender_id'] = comments['sender_id'].fillna('Unknown')

# ================= ENSURE IDs ARE INTEGERS =================
posts["msg_id"] = posts["msg_id"].fillna(-1).astype(int)
comments["post_id"] = comments["post_id"].fillna(-1).astype(int)

# ================= CREATE OR FIX num_comments =================
if 'num_comments' not in posts.columns:
    comments_count = comments.groupby('post_id')['comment_id'].count().reset_index()
    comments_count.rename(columns={'comment_id': 'num_comments'}, inplace=True)
    posts = posts.merge(comments_count, how='left', left_on='msg_id', right_on='post_id')
    posts['num_comments'] = posts['num_comments'].fillna(0).astype(int)
    posts.drop(columns=['post_id'], inplace=True)
else:
    # fill missing values in existing column
    posts['num_comments'] = posts['num_comments'].fillna(0).astype(int)

# ================= DATA QUALITY CHECKS =================
no_comment_count = (posts['num_comments'] == 0).sum()
posts_with_comments_count = posts.shape[0] - no_comment_count
invalid_post_comments = comments[~comments['post_id'].isin(posts['msg_id'])]
total_invalid_comments = invalid_post_comments.shape[0]

print("\n=== AFTER PREPROCESSING ===")
print(f"Posts shape: {posts.shape}")
print(f"Comments shape: {comments.shape}")
print("Missing values per column (posts):")
print(posts.isna().sum())
print("Missing values per column (comments):")
print(comments.isna().sum())
print(f"\nPosts with no comments: {no_comment_count}")
print(f"Posts with at least 1 comment: {posts_with_comments_count}")
print(f"Total comments pointing to missing posts: {total_invalid_comments}")

# ================= COMMENTS PER POST SUMMARY =================
comments_per_post = comments.groupby('post_id').size()
print("\nComments per post stats:")
print(comments_per_post.describe())

# ================= GROUP COMMENTS BY POST FOR PREVIEW =================
comments_grouped = comments.groupby("post_id")["text"].apply(list).to_dict()

print("\n--- Preview first 20 posts with comments ---")
for _, post in posts.head(20).iterrows():
    post_id = post["msg_id"]
    post_text = str(post["text"])
    post_text_short = (post_text[:text_preview_len] + "...") if len(post_text) > text_preview_len else post_text
    print(f"\nPost ID {post_id} - Text: {post_text_short}")

    post_comments = comments_grouped.get(post_id, [])
    if not post_comments:
        print("  No comments.")
        continue

    for i, comment_text in enumerate(post_comments[:n_comments], start=1):
        comment_text = str(comment_text)
        comment_text_short = (comment_text[:text_preview_len] + "...") if len(comment_text) > text_preview_len else comment_text
        print(f"  Comment {i}: {comment_text_short}")

print("\n✅ Data preprocessing completed successfully.")

#================ other preprocessing steps to consider==============")
def clean_text(text):
    if not isinstance(text, str):
        return ""
    
    # 1. Remove URLs (links)
    text = re.sub(r'http\S+|www\S+|https\S+', '', text, flags=re.MULTILINE)
    
    # 2. Remove mentions and hashtags (optional - keep if needed for analysis)
    text = re.sub(r'@\w+', '', text)  # remove @mentions
    # text = re.sub(r'#\w+', '', text)  # uncomment to remove hashtags
    
    # 3. Remove emojis OR convert them to text
    # Option A: Remove emojis
    text = emoji.replace_emoji(text, replace='')
    # Option B: Convert emojis to text (useful for sentiment)
    # text = emoji.demojize(text, delimiters=(":", ":"))
    
    # 4. Remove special characters and digits (keep only letters and spaces)
    text = re.sub(r'[^a-zA-Z\u1200-\u137F\s]', '', text)  # keeps Amharic and English
    
    # 5. Convert to lowercase (if English-heavy, but Amharic doesn't have case)
    text = text.lower()
    
    # 6. Remove extra whitespace
    text = ' '.join(text.split())
    
    return text

# Apply cleaning
posts['cleaned_text'] = posts['text'].apply(clean_text)
comments['cleaned_text'] = comments['text'].apply(clean_text)

# Detect if text is Amharic, English, or mixed
from langdetect import detect, DetectorFactory
DetectorFactory.seed = 0

def detect_lang(text):
    try:
        return detect(text)
    except:
        return 'unknown'

# Sample first 1000 comments for language distribution
comments_sample = comments['cleaned_text'].head(1000).apply(detect_lang)
print(comments_sample.value_counts())

