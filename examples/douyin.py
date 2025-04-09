import os
import time
import logging
import argparse
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import json

logging.basicConfig(level=logging.DEBUG, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)
# Configure logging

# Path to the folder containing videos
video_folder = os.getcwd()

def upload_video(video_path, video_thumbnail):
    try:
        logger.debug(f"Starting upload for video: {video_path}")
        
        # Navigate to the upload page
        logger.debug("Navigating to the upload page.")
        driver.find_element(By.XPATH, '//*[text()="发布视频"]').click()
        time.sleep(2)  # Wait for the page to load

        # Upload the video file
        logger.debug(f"Uploading video file: {video_path}")
        driver.find_element(By.XPATH, '//input[@type="file"]').send_keys(video_path)
        # Wait for the preview video element to appear
        logger.debug("Waiting for the preview video element to appear.")
        WebDriverWait(driver, 600).until(
            EC.presence_of_element_located((By.XPATH, '//div[text()="预览视频"]'))
        )
        # Check if a corresponding .txt file exists for the video
        txt_file = video_path.replace(".mp4", ".txt")
        if os.path.exists(txt_file):
            logger.debug(f"Found metadata file: {txt_file}")
            with open(txt_file, 'r', encoding='utf-8') as f:
                metadata = json.load(f)
            video_title = metadata.get("title", "Default Title")
            video_description = metadata.get("description", "Default Description").strip()

            # Input title
            logger.debug("Entering video title.")
            title = WebDriverWait(driver, 10).until(
                EC.visibility_of_element_located((By.XPATH, '//input[contains(@placeholder, "填写作品标题")]'))
            )
            title.click()
            channel_name_tag = f"#{metadata.get('channel_name', 'Default Channel Name')}"
            # Check if the length of the video title is less than 30 Chinese characters
            if len(video_title.encode('utf-8')) <= 90:  # Each Chinese character is 3 bytes in UTF-8
                title.send_keys(video_title)
            else:
                title.send_keys(channel_name_tag)
            time.sleep(1)  # Wait for the title field to be visible
            # Input description and tags
            logger.debug("Entering video description.")
            description = WebDriverWait(driver, 10).until(
                EC.visibility_of_element_located((By.XPATH, '//div[@data-placeholder="添加作品简介"]'))
            )
            driver.execute_script("arguments[0].innerText = arguments[1];", description, f"{video_description} {channel_name_tag}")
            time.sleep(1)  # Wait for the description field to be visible
        # Add cover page
        logger.debug(f"Adding cover page: {video_thumbnail}")
        driver.find_element(By.XPATH, '//*[text()="选择封面"]').click()
        time.sleep(2)  # Wait for the cover page to load

        file_input = driver.find_element(By.XPATH, '//input[@class="semi-upload-hidden-input"]')
        driver.execute_script("arguments[0].style.display = 'block';", file_input)
        file_input.send_keys(video_thumbnail)  # Provide the path to the thumbnail file
        time.sleep(8)  # Wait for the cover to upload

        # Switch back to the main content
        driver.switch_to.default_content()

        # Confirm the upload
        driver.find_element(By.XPATH, '//*[text()="完成"]').click()
        time.sleep(10)  # Wait for the upload to complete

        # Click the publish button
        logger.debug("Clicking the publish button.")
        # Scroll to the bottom and wait for the publish button to be clickable
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        publish_button = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.XPATH, '//*[text()="发布"]'))
        )
        publish_button.click()
        logger.info(f"Successfully uploaded video: {video_path}")
        time.sleep(10)  # Wait before uploading the next video

    except Exception as e:
        logging.error(f"An error occurred while uploading video {video_path}: {e}")

def main():
    # Parse command-line arguments
    parser = argparse.ArgumentParser(description="Douyin Video Uploader")
    parser.add_argument(
        "debug_port",  # Positional argument for the debug port
        type=int,
        nargs="?",  # Makes it optional
        default=9222,  # Default value for the debug port
        help="Port for Chrome remote debugging (default: 9222)"
    )
    args = parser.parse_args()

    # Configure Chrome options
    options = webdriver.ChromeOptions()
    options.add_experimental_option("debuggerAddress", f"127.0.0.1:{args.debug_port}")
    global driver
    driver = webdriver.Chrome(options=options)  # Ensure chromedriver is in PATH

    # Iterate through all videos in the folder
    for video_file in os.listdir(video_folder):
        if video_file.endswith((".mp4")):  # Add other video formats if needed
            video_path = os.path.join(video_folder, video_file)
            video_thumbnail = os.path.join(video_folder, video_file.replace(".mp4", ".jpg"))
            upload_video(video_path, video_thumbnail)

    logging.info("All videos uploaded.")
    driver.quit()

if __name__ == "__main__":
    main()
