from pathlib import Path
import os
import pandas as pd

# --- Configuration ---
n_comments = 3           # max number of comments per post
text_preview_len = 80    # truncate long text for display

# --- Project root ---
try:
    PROJECT_ROOT = Path(__file__).resolve().parent
except NameError:
    PROJECT_ROOT = Path.cwd()  # fallback if running interactively

# --- Data paths ---
DEFAULT_DATA_DIR = Path(r"C:/Users/DELL/Documents/project_data/output")
DATA_DIR = Path(os.getenv("DATA_DIR", DEFAULT_DATA_DIR))

POSTS_PATH = DATA_DIR / "safaricom_posts.csv"
COMMENTS_PATH = DATA_DIR / "safaricom_comments22.csv"

# --- File existence check ---
print("Using DATA_DIR:", DATA_DIR)
if not POSTS_PATH.exists():
    raise FileNotFoundError(f"Posts file not found: {POSTS_PATH}")
if not COMMENTS_PATH.exists():
    raise FileNotFoundError(f"Comments file not found: {COMMENTS_PATH}")

# --- Load CSVs ---
posts = pd.read_csv(POSTS_PATH)
comments = pd.read_csv(COMMENTS_PATH)

# --- Deduplicate ---
posts = posts.drop_duplicates(subset='msg_id')
comments = comments.drop_duplicates(subset='comment_id')

# --- Fill Missing Values ---
posts['views'] = posts['views'].fillna(0)
posts['forwards'] = posts['forwards'].fillna(0)
posts['replies'] = posts['replies'].fillna(0)
posts['media_type'] = posts['media_type'].fillna('Unknown')
posts['text'] = posts['text'].fillna('<media_only>')
posts['reply_to_msg_id'] = posts['reply_to_msg_id'].fillna('None')

comments['text'] = comments['text'].fillna('<deleted>')
comments['sender_id'] = comments['sender_id'].fillna('Unknown')

# --- Ensure IDs are integers ---
posts["msg_id"] = posts["msg_id"].fillna(-1).astype(int)
comments["post_id"] = comments["post_id"].fillna(-1).astype(int)

# --- Count comments per post ---
comments_count = comments.groupby('post_id')['comment_id'].count().reset_index()
comments_count.rename(columns={'comment_id': 'num_comments'}, inplace=True)
posts = posts.merge(comments_count, how='left', left_on='msg_id', right_on='post_id')
posts['num_comments'] = posts['num_comments'].fillna(0).astype(int)
posts.drop(columns=['post_id'], inplace=True)

# --- Posts with no comments ---
no_comment_count = (posts['num_comments'] == 0).sum()

# --- Data quality summary ---
missing_posts = posts.isna().sum()
missing_comments = comments.isna().sum()

print(f"\nPosts shape: {posts.shape}")
print("Missing values per column in posts:")
print(missing_posts)
print(f"\nComments shape: {comments.shape}")
print("Missing values per column in comments:")
print(missing_comments)
print(f"\nPosts with no comments: {no_comment_count}")

# Number of posts with at least 1 comment
posts_with_comments_count = posts.shape[0] - 2228
print("Posts with at least 1 comment:", posts_with_comments_count)

# Number of comments per post (summary)
comments_per_post = comments.groupby('post_id').size()
print("Comments per post stats:")
print(comments_per_post.describe())

# Comments whose post_id does not exist in posts
invalid_post_comments = comments[~comments['post_id'].isin(posts['msg_id'])]

# Total number of such comments
total_invalid_comments = invalid_post_comments.shape[0]

print(f"Total comments pointing to missing posts: {total_invalid_comments}")

# --- Group comments by post_id for preview ---
comments_grouped = comments.groupby("post_id")["text"].apply(list).to_dict()

# --- Preview first 20 posts with comments ---
print("\n--- Posts with comments preview ---")
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