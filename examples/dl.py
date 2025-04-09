import subprocess
import argparse
from pysrt import SubRipFile
from pysrt import SubRipItem
from pysrt import SubRipTime
import os
import glob
import logging

logging.basicConfig(level=logging.DEBUG, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)
def download_video(video_url, title):
    # Downloading video
    # Ensure the video is downloaded in mp4 format
    subprocess.run([
        'yt-dlp', '-f', 'bestvideo[height<=1080]+bestaudio/best[height<=1080]',
        '-o', 'subtitles.%(ext)s', '--merge-output-format', 'mp4', '-o', title, video_url
    ])

def download_subtitles(video_url, title, sub_lang=None):
    # Use default languages if sub_lang is not specified
    if sub_lang is None:
        sub_lang = 'en,zh-Hans,zh-Hans-en'
    # Downloading subtitles with the specified or default language
    subprocess.run([
        'yt-dlp', '--write-auto-subs', '--skip-download',
        '--sub-lang', sub_lang, '--convert-subs', 'srt',
        '-o', title, video_url
    ])

# Remove repetitions in both subtitle files
def clean_duplicates(file_path):
    subs = SubRipFile.open(file_path)
    unique_subs = []
    seen_texts = set()

    for sub in subs:
        lines = sub.text.splitlines()
        for prev_sub in unique_subs[-3:]:
            seen_texts.update(prev_sub.text.splitlines())
            # print(f"Seen texts so far: {seen_texts}")
        filtered_lines = [line for line in lines if line not in seen_texts]
        sub.text = '\n'.join(filtered_lines)
        unique_subs.append(sub)

    unique_subs_file = SubRipFile(items=unique_subs)
    unique_subs_file.save(file_path)

def replace_text_in_file(file_path, target, replacement):
    subs = SubRipFile.open(file_path)
    for sub in subs:
        if target.lower() in sub.text.lower():
            sub.text = sub.text.replace(target, replacement)
        sub.text = sub.text.replace(target, replacement)
    subs.save(file_path)

def merge_subtitles(file1, file2, output_file):
    if os.path.exists(file1):
        clean_duplicates(file1)
    if os.path.exists(file2):
        clean_duplicates(file2)
    
    if os.path.exists(file1) and os.path.exists(file2):
        subs1 = SubRipFile.open(file1)
        subs2 = SubRipFile.open(file2)
        delta = SubRipTime(milliseconds=0)
        merged_subs = merge_subtitle(subs1, subs2, delta)
        merged_subs.save(output_file, encoding='utf-8')
    else:
        if os.path.exists(file1):
            os.rename(file1, output_file)
        elif os.path.exists(file2):
            os.rename(file2, output_file)
    # Adjust the first subtitle's start time to ensure the interval is at least 1000ms
    # Otherwise, ffmpeg will ignore the first subtitle
    merged_subs = SubRipFile.open(output_file)
    if len(merged_subs) > 0:
        for sub in merged_subs:
            if sub.text.strip():
                first_sub = sub
                break
        interval = first_sub.end.ordinal - first_sub.start.ordinal
        if interval < 2000:
            # Adjust the start time relative to the end time
            if first_sub.end.ordinal <= 2000:
                first_sub.start = SubRipTime(milliseconds=0)
            else:
                first_sub.start = SubRipTime(milliseconds=first_sub.end.ordinal - 2000)
        merged_subs.save(output_file, encoding='utf-8')

def join_lines(txtsub1, txtsub2):
    if (len(txtsub1) > 0) & (len(txtsub2) > 0):
        return txtsub1 + '\n' + txtsub2
    else:
        return txtsub1 + txtsub2


def find_subtitle(subtitle, from_t, to_t, lo=0):
    i = lo
    while (i < len(subtitle)):
        if (subtitle[i].start >= to_t):
            break

        if (subtitle[i].start <= from_t) & (to_t  <= subtitle[i].end):
            return subtitle[i].text, i
        i += 1

    return "", i

def merge_subtitle(sub_a, sub_b, delta):
    out = SubRipFile()
    intervals = [item.start.ordinal for item in sub_a]
    intervals.extend([item.end.ordinal for item in sub_a])
    intervals.extend([item.start.ordinal for item in sub_b])
    intervals.extend([item.end.ordinal for item in sub_b])
    intervals.sort()

    j = k = 0
    for i in range(1, len(intervals)):
        start = SubRipTime.from_ordinal(intervals[i-1])
        end = SubRipTime.from_ordinal(intervals[i])

        if (end-start) > delta:
            text_a, j = find_subtitle(sub_a, start, end, j)
            text_b, k = find_subtitle(sub_b, start, end, k)

            text = join_lines(text_a, text_b)
            if len(text) > 0:
                item = SubRipItem(0, start, end, text)
                out.append(item)

    out.clean_indexes()
    return out


def apply_subtitles_to_video(video_file, subtitle_file, output_file, font_size="medium"):
    """Apply subtitles to a video with the specified font size."""
    # Map font size values to actual font sizes
    font_size_map = {
        "small": 16,
        "medium": 24,
        "large": 32
    }
    # Default to medium if the font size is not recognized
    font_size_value = font_size_map.get(font_size, 24)

    if os.path.exists(subtitle_file):
        logger.debug(f"Applying subtitles from {subtitle_file} to {video_file}")
        os.environ["FFMPEG_LOG_LEVEL"] = "quiet"
        subprocess.run([
            'ffmpeg', '-i', video_file, '-vf',
            f"subtitles={subtitle_file}:force_style='FontName=Arial,FontSize={font_size_value},PrimaryColour=&H00FF00&,OutlineColour=&H54000008&,BackColour=&H80000000&,BorderStyle=3,Outline=1'",
            '-c:a', 'copy', output_file
        ])
    else:
        logger.debug(f"Subtitle file {subtitle_file} does not exist. Skipping subtitle application.")
        subprocess.run([
            'ffmpeg', '-i', video_file, '-c:a', 'copy', '-c:v', 'copy', output_file
        ])

def get_video_title(video_url):
    result = subprocess.run(
        ['yt-dlp', '--get-title', video_url],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True
    )
    if result.returncode == 0:
        return result.stdout.strip()
    else:
        raise RuntimeError(f"Failed to get video title: {result.stderr.strip()}")

def sanitize_subtitles(subtitle_file):
    """Sanitize subtitles by replacing specific text with placeholders."""
    replace_text_in_file(subtitle_file, 'xijinping', 'XXX')
    replace_text_in_file(subtitle_file, 'xiinping', 'XXX')
    replace_text_in_file(subtitle_file, 'Tiananmen', 'XXX')
    replace_text_in_file(subtitle_file, '习近平', 'XXX')
    replace_text_in_file(subtitle_file, '共产党', 'XXX')
    replace_text_in_file(subtitle_file, '中国', 'XX')
    replace_text_in_file(subtitle_file, '天安门', 'XX')

def download_thumbnail(video_url, output_file, extension="jpg"):
    """Download and convert the thumbnail of a YouTube video using yt-dlp."""
    try:
        subprocess.run(
            [
                'yt-dlp',
                '--write-thumbnail',
                '--convert-thumbnails', extension,
                '--skip-download',
                '-o', output_file,
                video_url
            ],
            check=True
        )
        print(f"Thumbnail downloaded and converted to {extension}: {output_file}")
    except subprocess.CalledProcessError as e:
        print(f"Failed to download thumbnail: {e}")

def main(args=None, censor=True, dry=False):
    parser = argparse.ArgumentParser(description='Download and merge YouTube subtitles.')
    parser.add_argument('video_url', type=str, help='URL of the YouTube video')
    parser.add_argument('sub_lang', type=int, nargs='?', choices=[0, 1, 2, 3], default=2,
                        help='Comma-separated list of subtitle languages (e.g., "en,zh-Hans")')
    parser.add_argument('font_size', type=str, nargs='?', choices=["small", "medium", "large"], default="medium",
                        help="Specify the font size for subtitles (small, medium, large)")

    if args is None:
        args = parser.parse_args()
    else:
        args = parser.parse_args(args)

    # Existing code for downloading and processing the video
    video_url = args.video_url
    font_size = args.font_size  # Get the font size from arguments
    if args.sub_lang == 1:
        sub_lang = "zh-Hans,zh-Hans-en"
    elif args.sub_lang == 2:
        sub_lang = "en,zh-Hans,zh-Hans-en"
    elif args.sub_lang == 3:
        sub_lang = "en"
    elif args.sub_lang == 0:
        sub_lang = None
    video_title = get_video_title(video_url)
    sanitized_title = "".join(c if c.isalnum() or c in "_-" else "_" for c in video_title)
    output_video_file = f"{sanitized_title}.subbed.mp4"
    if dry:
        return output_video_file
    # Remove existing subtitle files
    for file in glob.glob(f"{sanitized_title}.*.srt"):
        os.remove(file)

    # Remove existing video files
    for file in glob.glob(f"{sanitized_title}.mp4"):
        os.remove(file)
        # Determine subtitle language based on sub_lang argument
    if(sub_lang is not None):
        download_subtitles(video_url, sanitized_title, sub_lang)
        subtitle_file_zh = f'{sanitized_title}.zh-Hans.srt' if os.path.exists(f'{sanitized_title}.zh-Hans.srt') else f'{sanitized_title}.zh-Hans-en.srt'
        if censor:
            if os.path.exists(f'{sanitized_title}.en.srt'):
                sanitize_subtitles(f'{sanitized_title}.en.srt')
            if os.path.exists(subtitle_file_zh):
                sanitize_subtitles(subtitle_file_zh)
        merge_subtitles(f'{sanitized_title}.en.srt', subtitle_file_zh, f'{sanitized_title}.merged_subtitles.srt')
        print("Merging completed. The merged subtitles have been saved to 'merged_subtitles.srt'.")
    download_thumbnail(video_url, sanitized_title)
    download_video(video_url, sanitized_title)
    apply_subtitles_to_video(f'{sanitized_title}.mp4', f'{sanitized_title}.merged_subtitles.srt', output_video_file, font_size)
    
    # Delete the original video and all subtitle files
    for file in glob.glob(f"{sanitized_title}.*.srt"):
        os.remove(file)

    os.rename(output_video_file, f"{sanitized_title}.mp4")
    print(f"Subtitles have been applied to the video and saved as {output_video_file}.")
    return f"{sanitized_title}.mp4"
if __name__ == "__main__":
    main()
