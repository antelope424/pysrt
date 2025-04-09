import os
import sqlite3
import subprocess
import time
import pickle
import asyncio
import argparse
from googleapiclient.discovery import build
from dl import main as download_video_script
import isodate
from googleapiclient.http import MediaFileUpload
from googletrans import Translator
from httpx import TimeoutException
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
import logging
import math

logging.basicConfig(level=logging.DEBUG, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# YouTube Data API key
API_KEY = os.getenv("YOUTUBE_API_KEY")
if not API_KEY:
    raise ValueError("YOUTUBE_API_KEY environment variable is not set")

# Combined two-dimensional array of channel lists
CHANNELS_TO_MONITOR = [
    # List 1
    [
        {"channel_id": "UCIALMKvObZNtJ6AmdCLP7Lg", "name": "Bloomberg Television"},
        {"channel_id": "UCEAZeUIeJs0IjQiqTCdVSIg", "name": "Yahoo Finance"},
        {"channel_id": "UCg40OxZ1GYh3u3jBntB6DLg", "name": "Forbes Breaking News"},
        {"channel_id": "UCCXoCcu9Rp7NPbTzIvogpZg", "name": "Fox Business"},
        # Add more channels as needed
    ],
    # List 2
    [
        {"channel_id": "UCIALMKvObZNtJ6AmdCLP7Lg", "name": "Bloomberg Television"},
        {"channel_id": "UCEAZeUIeJs0IjQiqTCdVSIg", "name": "Yahoo Finance"},
        {"channel_id": "UCupvZG-5ko_eiXAupbDfxWw", "name": "CNN"},
        {"channel_id": "UCuFFtHWoLl5fauMMD5Ww2jA", "name": "CBC News"},
        {"channel_id": "UCg40OxZ1GYh3u3jBntB6DLg", "name": "Forbes Breaking News"},
        {"channel_id": "UCaXkIU1QidjPwiAYu6GcHjg", "name": "MSNBC"},
        {"channel_id": "UChqUTb7kYRX8-EiaN3XFrSQ", "name": "Reuters"},
        {"channel_id": "UC52X5wxOL_s5yw0dQk7NtgA", "name": "Associated Press"},
        {"channel_id": "UCAuUUnT6oDeKwE6v1NGQx4g", "name": "The Hill"},
        {"channel_id": "UCCXoCcu9Rp7NPbTzIvogpZg", "name": "Fox Business"},
        # Add more channels as needed
    ],
    # List 3
    [
        {"channel_id": "UCwWhs_6x42TyRM4Wstoq8HA", "name": "The Daily Show"},
    ],
]

# YouTube API scopes
SCOPES = ["https://www.googleapis.com/auth/youtube.upload"]
DB_FILE = "downloaded_videos.db"

def init_db():
    """Initialize the SQLite database to store downloaded video IDs."""
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS downloaded_videos (
            video_id TEXT PRIMARY KEY
        )
    """)
    conn.commit()
    conn.close()

def is_video_downloaded(video_id):
    """Check if a video ID is already downloaded."""
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute("SELECT 1 FROM downloaded_videos WHERE video_id = ?", (video_id,))
    result = cursor.fetchone()
    conn.close()
    return result is not None

def mark_video_as_downloaded(video_id):
    """Mark a video ID as downloaded by adding it to the database."""
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute("INSERT OR IGNORE INTO downloaded_videos (video_id) VALUES (?)", (video_id,))
    conn.commit()
    conn.close()

def authenticate_youtube():
    """Authenticate and return a YouTube API client."""
    credentials = None

    # Check if credentials are already saved
    if os.path.exists("token.pickle"):
        with open("token.pickle", "rb") as token:
            credentials = pickle.load(token)

    # If no valid credentials, authenticate using OAuth2
    if not credentials or not credentials.valid:
        if credentials and credentials.expired and credentials.refresh_token:
            credentials.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                os.path.expanduser("~/client_secret.json"), SCOPES
            )
            credentials = flow.run_local_server(port=0)

        # Save the credentials for future use
        with open("token.pickle", "wb") as token:
            pickle.dump(credentials, token)

    return build("youtube", "v3", credentials=credentials)

def get_recent_videos(api_key, channel_id):
    """Fetch recent videos from a YouTube channel."""
    logger.debug(f"Debug: Fetching recent videos for channel ID: {channel_id}")

    youtube = build("youtube", "v3", developerKey=api_key)
    request = youtube.search().list(
        part="snippet",
        channelId=channel_id,
        maxResults=10,
        order="date",
        type="video"
    )
    response = request.execute()
    # logger.debug(f"API Response: {response}")

    videos = []
    
    for item in response.get("items", []):
        video_id = item["id"]["videoId"]
        title = item["snippet"]["title"]
        tags = item["snippet"].get("tags", [])
        videos.append({"id": video_id, "title": title, "tags": tags})
    return videos

def get_video_details(api_key, video_id):
    """Fetch the duration and dimensions of a video."""
    youtube = build("youtube", "v3", developerKey=api_key)
    request = youtube.videos().list(
        part="contentDetails",
        id=video_id
    )
    response = request.execute()
    # logger.debug(f"API Response: {response}")
    items = response.get("items", [])
    if not items:
        return None

    # Get duration
    duration = items[0]["contentDetails"]["duration"]
    duration_seconds = parse_duration(duration)

    return duration_seconds

def parse_duration(duration):
    """Convert ISO 8601 duration to seconds."""
    parsed_duration = isodate.parse_duration(duration)
    return int(parsed_duration.total_seconds())

async def translate_title(title, retries=3, delay=5):
    """Translate the video title to Chinese using googletrans with retries."""
    translator = Translator()
    for attempt in range(retries):
        try:
            result = await translator.translate(title, src="en", dest="zh-cn", timeout=10)  # Set a 10-second timeout
            return result.text
        except TimeoutException:
            logger.debug(f"Translation timed out for title: {title}. Retrying ({attempt + 1}/{retries})...")
            await asyncio.sleep(delay)  # Wait before retrying
    logger.debug(f"Failed to translate title after {retries} attempts: {title}")
    return f"{title} (Translation Failed)"

def main(args=None):
    # Set up the proxy if needed. e.g, os.environ["https_proxy"] = "http://my_proxy:port"
    proxy = os.getenv("my_proxy")
    if proxy:
        os.environ["https_proxy"] = proxy
    init_db()  # Initialize the database
    parser = argparse.ArgumentParser(description="Monitor YouTube channels for new videos.")
    parser.add_argument("list", type=int, choices=[1, 2, 3], help="Specify which list to monitor (1,2 or 3).")
    parser.add_argument("skip_upload", type=str, choices=["T", "F"], help="Specify whether to skip upload (T for True, F for False).")
    parser.add_argument("censor", type=str, choices=["T", "F"], help="Specify whether to censor subtitles (T for True, F for False).")
    parser.add_argument("max_video_length", type=int, default=15, 
                        help=f"Specify the maximum video length in minutes (default: 15).")

    if args is None:
        args = parser.parse_args()
    else:
        args = parser.parse_args(args)

    # Convert string arguments to boolean
    skip_upload = args.skip_upload == "T"
    censor = args.censor == "T"

    max_video_length = args.max_video_length

    # Select the list to monitor
    channels_to_monitor = CHANNELS_TO_MONITOR[args.list - 1]
    logger.debug(f"Monitoring list {args.list}")
    logger.debug(f"Skip upload: {skip_upload}, Censor: {censor}, Max video length: {max_video_length} minutes")

    # Authenticate YouTube only if not skipping upload
    youtube_upload = authenticate_youtube() if not skip_upload else None

    return channels_to_monitor, skip_upload, censor, max_video_length, youtube_upload


async def monitor_channel(channels_to_monitor, skip_upload, censor, max_video_length):
    """Monitor the channel for new videos and download short ones."""
    while True:
        logger.debug("Checking for new videos...")
        tasks = []
        semaphore = asyncio.Semaphore(3)  # Limit to 3 concurrent tasks

        async def process_channel(channel):
            async with semaphore:
                CHANNEL_ID = channel["channel_id"]
                logger.debug(f"Debug: Monitoring channel: {channel['name']} (ID: {CHANNEL_ID})")

                # Fetch recent videos
                videos = get_recent_videos(API_KEY, CHANNEL_ID)
                logger.debug(f"Number of videos fetched for channel {channel['name']}: {len(videos)}")
                for video in videos:
                    video_id = video["id"]
                    title = video["title"]
                    logger.debug(f"Processing video with ID: {video_id}, Title: {title}")
                    video_url = f"https://www.youtube.com/watch?v={video_id}"
                    font_size = "medium"
                    # Skip if the video is already downloaded
                    if not is_video_downloaded(video_id):
                        logger.debug(f"Video not downloaded yet: {title}")
                        duration = get_video_details(API_KEY, video_id)
                        logger.debug(f"Video ID: {video_id}, Duration: {duration} seconds")
                        # Telling by duration if this is a short video (1:2 aspect ratio)
                        if duration < 240:
                            font_size = "small"

                        logger.debug(f"Video ID: {video_id}, Duration: {duration} seconds, Font Size: {font_size}")

                        if duration is None:
                            logger.debug(f"Could not fetch duration for video: {title}")
                            continue

                        if duration <= max_video_length * 60:
                            logger.debug(f"Downloading video: {title} (Duration: {duration // 60} minutes)")
                            try:
                                downloaded_file = download_video_script([video_url, font_size], censor=censor, dry=False)
                                logger.debug(f"Downloaded and processed video: {title}")
                                mark_video_as_downloaded(video_id)  # Mark the video as downloaded
                            except Exception as e:
                                logger.debug(f"Failed to download video: {title}. Error: {e}")
                                downloaded_file = None  # Ensure downloaded_file is set to None if download fails
                        else:
                            logger.debug(f"Skipping video: {title} (Duration: {duration // 60} minutes)")
                            downloaded_file = None  # Set to None if the video is skipped
                    else:
                        logger.debug(f"Skipping video: {title} (Already downloaded in record)")
                        # Only get the video file name
                        downloaded_file = download_video_script([video_url, "medium"], censor=censor, dry=True)
                        if not os.path.exists(downloaded_file):
                            logger.debug(f"Skipping video: {title} (File not found)")
                            continue

                    # Ensure downloaded_file is valid before proceeding
                    if not downloaded_file:
                        logger.debug(f"Skipping re-gen for video: {title} as it was not downloaded.")
                        continue

                    if not skip_upload:
                        # Translate the title to Chinese
                        translated_title = await translate_title(title)

                        # Prepare the video upload request
                        body = {
                            "snippet": {
                                "title": translated_title,
                                "description": f"{title}. Video URL: https://www.youtube.com/watch?v={video_id}",
                                "tags": video["tags"],
                                "categoryId": "25"  # Category ID for "News & Politics"
                            },
                            "status": {
                                "privacyStatus": "public"  # Set to "public" if you want it public
                            }
                        }

                        media = MediaFileUpload(downloaded_file, chunksize=-1, resumable=True)
                        request = youtube_upload.videos().insert(
                            part="snippet,status",
                            body=body,
                            media_body=media
                        )

                        try:
                            logger.debug(f"Uploading video: {translated_title}")
                            response = request.execute()
                            logger.debug(f"Uploaded video: {translated_title} (Video ID: {response['id']})")

                        except Exception as e:
                            if "uploadLimitExceeded" in str(e):
                                logger.error("Upload limit exceeded. Exiting all monitor tasks.")
                                for task in asyncio.all_tasks():
                                    if task is not asyncio.current_task():
                                        task.cancel()
                                return  # Exit the current upload process
                            logger.debug(f"Failed to upload video: {translated_title}. Error: {e}")

        for channel in channels_to_monitor:
            tasks.append(process_channel(channel))

        await asyncio.gather(*tasks, return_exceptions=True)
        logger.debug("All channels processed. Waiting for the next check...")
        await asyncio.sleep(3600)  # Check every hour

if __name__ == "__main__":
    # Get the necessary variables from main
    channels_to_monitor, skip_upload, censor, max_video_length, youtube_upload = main()
    
    # Pass the variables to monitor_channel
    asyncio.run(monitor_channel(channels_to_monitor, skip_upload, censor, max_video_length))