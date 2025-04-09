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
from deep_translator import GoogleTranslator
from googletrans import Translator
from httpx import TimeoutException
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
import logging
import math
import json

logging.basicConfig(level=logging.DEBUG, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# YouTube Data API key
API_KEY = os.getenv("YOUTUBE_API_KEY")
if not API_KEY:
    raise ValueError("YOUTUBE_API_KEY environment variable is not set")

API_KEY_2 = os.getenv("YOUTUBE_API_KEY_2")
if not API_KEY_2:
    raise ValueError("YOUTUBE_API_KEY_2 environment variable is not set")

# Combined two-dimensional array of channel lists
CHANNELS_TO_MONITOR = [
    # List 1
    [
        {"channel_id": "UCIALMKvObZNtJ6AmdCLP7Lg", "name": "BloombergTelevision"},
        {"channel_id": "UCEAZeUIeJs0IjQiqTCdVSIg", "name": "YahooFinance"},
        {"channel_id": "UCg40OxZ1GYh3u3jBntB6DLg", "name": "ForbesBreakingNews"},
        {"channel_id": "UCCXoCcu9Rp7NPbTzIvogpZg", "name": "FoxBusiness"},
        # Add more channels as needed
    ],
    # List 2
    [
        {"channel_id": "UCIALMKvObZNtJ6AmdCLP7Lg", "name": "BloombergTelevision"},
        {"channel_id": "UCEAZeUIeJs0IjQiqTCdVSIg", "name": "YahooFinance"},
        {"channel_id": "UCupvZG-5ko_eiXAupbDfxWw", "name": "CNN"},
        {"channel_id": "UCuFFtHWoLl5fauMMD5Ww2jA", "name": "CBCNews"},
        {"channel_id": "UC8p1vwvWtl6T73JiExfWs1g", "name": "CBSNews"},
        {"channel_id": "UCg40OxZ1GYh3u3jBntB6DLg", "name": "ForbesBreakingNews"},
        {"channel_id": "UCaXkIU1QidjPwiAYu6GcHjg", "name": "MSNBC"},
        {"channel_id": "UChqUTb7kYRX8-EiaN3XFrSQ", "name": "Reuters"},
        {"channel_id": "UC52X5wxOL_s5yw0dQk7NtgA", "name": "AssociatedPress"},
        {"channel_id": "UCAuUUnT6oDeKwE6v1NGQx4g", "name": "TheHill"},
        {"channel_id": "UCCXoCcu9Rp7NPbTzIvogpZg", "name": "FoxBusiness"},
        # Add more channels as needed
    ],
    # List 3
    [
        {"channel_id": "UCwWhs_6x42TyRM4Wstoq8HA", "name": "TheDailyShow"},
    ],
    # List 4
    [
        {"channel_id": "UC2mKA8JTOCeodl9bEK7w42Q", "name": "MattRife"},
    ],
    # List 5
    [
        {"channel_id": "UC7kCeZ53sli_9XwuQeFxLqw", "name": "TickerSymbolYou"},
    ]
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

def get_all_videos(api_key, channel_id, duration):
    """Fetch all videos from a YouTube channel."""
    youtube = build("youtube", "v3", developerKey=api_key)
    videos = []
    next_page_token = None

    while True:
        request = youtube.search().list(
            part="snippet",
            channelId=channel_id,
            maxResults=50,  # Maximum allowed by the API
            order="date",
            type="video",
            videoDuration=duration,  # Filter for short videos
            pageToken=next_page_token
        )
        response = request.execute()
        # logger.debug(f"API Response: {response}")
    
        for item in response.get("items", []):
            video_id = item["id"]["videoId"]
            title = item["snippet"]["title"]
            tags = item["snippet"].get("tags", [])
            videos.append({"id": video_id, "title": title, "tags": tags})

        next_page_token = response.get("nextPageToken")
        if not next_page_token:
            break
    return videos

def get_recent_videos(api_keys, channel_id):
    """Fetch recent videos from a YouTube channel using multiple API keys."""
    logger.debug(f"Fetching recent videos for channel ID: {channel_id}")

    for api_key in api_keys:
        try:
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
                videos.append({"id": video_id})
            return videos, api_key
        except Exception as e:
            error = json.loads(e.content.decode())
            if error.get("error", {}).get("errors", [{}])[0].get("reason") == "quotaExceeded":
                logger.debug(f"Quota exceeded for API key: {api_key}. Trying next key...")
                continue
            else:
                logger.error(f"Error fetching videos: {e}")
                break

    logger.error("All API keys exhausted or failed.")
    return []

def get_video_details(api_key, video_ids):
    """
    Fetch the duration, title, description, and tags of multiple videos.
    
    Args:
        api_key (str): YouTube Data API key.
        video_ids (list): List of video IDs to fetch details for.

    Returns:
        list: A list of dictionaries containing video details.
    """
    # Join the video IDs into a comma-separated string
    video_ids_str = ",".join(video_ids)
    logger.debug(f"Fetching video details for IDs: {video_ids_str}")
    youtube = build("youtube", "v3", developerKey=api_key)
    request = youtube.videos().list(
        part="snippet,contentDetails",
        id=video_ids_str
    )
    response = request.execute()
    # logger.debug(f"API Response: {response}")
    items = response.get("items", [])
    if not items:
        return []

    video_details = []
    for item in items:
        # Get duration
        duration = item["contentDetails"]["duration"]
        duration_seconds = parse_duration(duration)

        # Get title, description, and tags
        snippet = item["snippet"]
        title = snippet["title"]
        description = snippet["description"].split("\n")[0]  # Save only the first line of the description
        tags = snippet.get("tags", [])

        video_details.append({
            "video_id": item["id"],
            "duration_seconds": duration_seconds,
            "title": title,
            "description": description,
            "tags": tags
        })

    return video_details

def parse_duration(duration):
    """Convert ISO 8601 duration to seconds."""
    parsed_duration = isodate.parse_duration(duration)
    return int(parsed_duration.total_seconds())

def translate_title(title, retries=3, delay=5):
    """Translate the video title to Chinese using googletrans with retries."""
    translator = GoogleTranslator(source='en', target='zh-CN')
    for attempt in range(retries):
        result = translator.translate(title)  # Remove 'await' as 'deep_translator' is not async
        return result
    logger.debug(f"Failed to translate title after {retries} attempts: {title}")
    return f"{title} (Translation Failed)"

def main(args=None):
    # Set up the proxy if needed. e.g, os.environ["https_proxy"] = "http://my_proxy:port"
    proxy = os.getenv("my_proxy")
    if proxy:
        os.environ["https_proxy"] = proxy
    init_db()  # Initialize the database
    parser = argparse.ArgumentParser(description="Monitor YouTube channels for new videos.")
    parser.add_argument("list", type=int, choices=[1, 2, 3, 4, 5], help="Specify which list to monitor (1,2,3 or 4).")
    parser.add_argument("skip_upload", type=str, choices=["T", "F"], help="Specify whether to skip upload (T for True, F for False).")
    parser.add_argument("censor", type=str, choices=["T", "F"], help="Specify whether to censor subtitles (T for True, F for False).")
    parser.add_argument("max_video_length", type=int, default=15, 
                        help=f"Specify the maximum video length in minutes (default: 15).")
    parser.add_argument("sub_lang", type=str, choices=["0", "1", "2", "3"], default="2", 
                        help=f"Specify which sub_lang to download (default: 2).")

    if args is None:
        args = parser.parse_args()
    else:
        args = parser.parse_args(args)

    # Convert string arguments to boolean
    skip_upload = args.skip_upload == "T"
    censor = args.censor == "T"

    max_video_length = args.max_video_length
    sub_lang = args.sub_lang
    # Select the list to monitor
    channels_to_monitor = CHANNELS_TO_MONITOR[args.list - 1]
    logger.debug(f"Monitoring list {args.list}")
    logger.debug(f"Skip upload: {skip_upload}, Censor: {censor}, Max video length: {max_video_length} minutes")

    # Authenticate YouTube only if not skipping upload
    youtube_upload = authenticate_youtube() if not skip_upload else None

    return channels_to_monitor, skip_upload, censor, max_video_length, sub_lang, youtube_upload


async def monitor_channel(channels_to_monitor, skip_upload, censor, max_video_length, sub_lang):
    """Monitor the channel for new videos and download short ones."""
    while True:
        logger.debug("Checking for new videos...")
        tasks = []
        semaphore = asyncio.Semaphore(3)  # Limit to 3 concurrent tasks

        async def process_channel(channel):
            async with semaphore:
                CHANNEL_ID = channel["channel_id"]
                CHANNEL_NAME = channel["name"]
                logger.debug(f"Monitoring channel: {channel['name']} (ID: {CHANNEL_ID})")

                # Fetch recent videos
                # videos = get_all_videos(API_KEY, CHANNEL_ID, "short")
                # Rotate through API keys to avoid quota exhaustion
                api_keys = [API_KEY_2, API_KEY]
                
                videos, api_key = get_recent_videos(api_keys, CHANNEL_ID)
                logger.debug(f"Number of videos fetched for channel {channel['name']}: {len(videos)}")
                video_ids = [video["id"] for video in videos]
                videodetails = get_video_details(api_key, video_ids)
                for videodetail in videodetails:
                    video_id = videodetail["video_id"]
                    logger.debug(f"Processing video with ID: {video_id}")
                    video_url = f"https://www.youtube.com/watch?v={video_id}"
                    font_size = "medium"
                    title = videodetail["title"]
                    description = videodetail["description"]
                    duration = videodetail["duration_seconds"]
                    tags = videodetail["tags"]
                    # Skip if the video is already downloaded
                    if not is_video_downloaded(video_id):
                        logger.debug(f"Video not downloaded yet: {video_id}")

                        logger.debug(f"Processing video with ID: {video_id}, Duration: {duration} seconds, Title: {title}, Description: {description}, Tags: {tags}")

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
                                downloaded_file = download_video_script([video_url, sub_lang, font_size], censor=censor, dry=False)
                                logger.debug(f"Downloaded and processed video: {title}")
                                mark_video_as_downloaded(video_id)  # Mark the video as downloaded
                                # Write a JSON file with the translated title and description
                                if downloaded_file:
                                    translated_title = translate_title(title)
                                    logger.debug(f"Write JSON file for video: {title}")
                                    json_file_path = os.path.splitext(downloaded_file)[0] + ".txt"
                                    logger.debug(f"JSON file path: {json_file_path}")
                                    modifiedtags = ' '.join([f'#{tag.replace(" ", "")}' for tag in tags])
                                    # Translate the title to Chinese
                                    json_content = {
                                        "title": f"{translated_title}",
                                        "description": json.dumps(f"{title} {description} {modifiedtags}"),
                                        "channel_name": f"{CHANNEL_NAME}",
                                    }
                                    logger.debug(f"JSON content: {json_content}")
                                    try:
                                        with open(json_file_path, "w", encoding="utf-8") as json_file:
                                            json.dump(json_content, json_file, ensure_ascii=False, indent=4)
                                        logger.debug(f"JSON file created: {json_file_path}")
                                    except Exception as e:
                                        logger.debug(f"Failed to write JSON file for video: {title}. Error: {e}")
                            except Exception as e:
                                logger.debug(f"Failed to download video: {title}. Error: {e}")
                                downloaded_file = None  # Ensure downloaded_file is set to None if download fails
                        else:
                            logger.debug(f"Skipping video: {title} (Duration: {duration // 60} minutes)")
                            downloaded_file = None  # Set to None if the video is skipped
                    else:
                        logger.debug(f"Skipping video: {title} (Already downloaded in record)")
                        # Only get the video file name
                        try:
                            downloaded_file = download_video_script([video_url, "2", "medium"], censor=censor, dry=True)
                        except Exception as e:
                            logger.debug(f"Failed to get video file name: {title}. Error: {e}")
                        if not os.path.exists(downloaded_file):
                            logger.debug(f"Skipping video: {title} (File not found)")
                            continue

                    # Ensure downloaded_file is valid before proceeding
                    if not downloaded_file:
                        logger.debug(f"Skipping re-gen for video: {title} as it was not downloaded.")
                        continue

                    if not skip_upload:
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

        results = await asyncio.gather(*tasks, return_exceptions=True)
        for result in results:
            if isinstance(result, Exception):
                logger.error(f"Task ended with exception: {result}")
        logger.debug("All channels processed. Waiting for the next check...")
        await asyncio.sleep(3600*4)  # Check 4 hours

if __name__ == "__main__":
    # Get the necessary variables from main
    channels_to_monitor, skip_upload, censor, max_video_length, sub_lang, youtube_upload = main()
    
    # Pass the variables to monitor_channel
    asyncio.run(monitor_channel(channels_to_monitor, skip_upload, censor, max_video_length, sub_lang))