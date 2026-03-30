from pathlib import Path
import os
import pandas as pd
from dotenv import load_dotenv

# --- Load .env from project root reliably ---
PROJECT_ROOT = Path(__file__).resolve().parent
ENV_PATH = PROJECT_ROOT / ".env"

# --- Fallback if DATA_DIR not set ---
DEFAULT_DATA_DIR = Path(r"C:\Users\DELL\Documents\project_data\output")

DATA_DIR = Path(os.getenv("DATA_DIR", DEFAULT_DATA_DIR))

print("Using DATA_DIR:", DATA_DIR)

POSTS_PATH = DATA_DIR / "safaricom_telegram.csv"
COMMENTS_PATH = DATA_DIR / "safaricom_comments.csv"

# --- Strong validation ---
if not POSTS_PATH.exists():
    raise FileNotFoundError(f"Posts file not found: {POSTS_PATH}")

if not COMMENTS_PATH.exists():
    raise FileNotFoundError(f"Comments file not found: {COMMENTS_PATH}")

posts = pd.read_csv(POSTS_PATH)
comments = pd.read_csv(COMMENTS_PATH)

print("Posts shape:", posts.shape)
print("Comments shape:", comments.shape)