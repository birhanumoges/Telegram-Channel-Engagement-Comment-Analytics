from pathlib import Path
import os
import pandas as pd

# --- Configuration ---
n_comments = 3  # max number of comments per post
text_preview_len = 80  # truncate long text for display

# --- Project root ---
try:
    PROJECT_ROOT = Path(__file__).resolve().parent
except NameError:
    PROJECT_ROOT = Path.cwd()  # fallback if running interactively

# --- Data paths ---
DEFAULT_DATA_DIR = Path(r"C:\Users\DELL\Documents\project_data\output")
DATA_DIR = Path(os.getenv("DATA_DIR", DEFAULT_DATA_DIR))

POSTS_PATH = DATA_DIR / "safaricom_telegram.csv"
COMMENTS_PATH = DATA_DIR / "safaricom_comments.csv"

# --- File existence check ---
print("Using DATA_DIR:", DATA_DIR)
if not POSTS_PATH.exists():
    raise FileNotFoundError(f"Posts file not found: {POSTS_PATH}")
if not COMMENTS_PATH.exists():
    raise FileNotFoundError(f"Comments file not found: {COMMENTS_PATH}")

# --- Load CSVs ---
posts = pd.read_csv(POSTS_PATH)
comments = pd.read_csv(COMMENTS_PATH)

# --- Configure pandas display ---
pd.set_option('display.max_columns', None)
pd.set_option('display.width', 120)
pd.set_option('display.max_colwidth', text_preview_len)

# --- Show shapes and columns ---
print(f"\nPosts shape: {posts.shape}, columns: {posts.columns.tolist()}")
print(posts.head(10).to_string(index=False))
# Total missing values per column in posts
missing_posts = posts.isna().sum()
print("Missing values per column in posts:")
print(missing_posts)

# Total missing values per column in comments
missing_comments = comments.isna().sum()
print("\nMissing values per column in comments:")
print(missing_comments)

print(f"\nComments shape: {comments.shape}, columns: {comments.columns.tolist()}")

if "text" in comments.columns:
    print(comments[['post_id','text']].head(10).to_string(index=False))

# --- Ensure IDs are integers ---
posts["msg_id"] = posts["msg_id"].fillna(-1).astype(int)
comments["post_id"] = comments["post_id"].fillna(-1).astype(int)

# --- Group comments by post_id ---
comments_grouped = comments.groupby("post_id")["text"].apply(list).to_dict()

# --- Print posts with up to n_comments ---
print("\n--- Posts with comments preview ---")
for _, post in posts.head(20).iterrows():  # first 20 posts only
    post_id = post["msg_id"]
    post_text = str(post["text"]) if pd.notna(post["text"]) else ""
    post_text_short = (post_text[:text_preview_len] + "...") if len(post_text) > text_preview_len else post_text
    print(f"\nPost ID {post_id} - Text: {post_text_short}")

    post_comments = comments_grouped.get(post_id, [])
    if not post_comments:
        print("  No comments.")
        continue

    for i, comment_text in enumerate(post_comments[:n_comments], start=1):
        comment_text = str(comment_text) if pd.notna(comment_text) else ""
        comment_text_short = (comment_text[:text_preview_len] + "...") if len(comment_text) > text_preview_len else comment_text
        print(f"  Comment {i}: {comment_text_short}")

print("\nProcessed first 10 posts successfully.")