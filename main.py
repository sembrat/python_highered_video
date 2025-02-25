import csv
import os
import requests
import re
import tldextract
import ssl
from urllib.parse import urlparse
from bs4 import BeautifulSoup

def main():
    input_file_name = "resource/hd2023.csv"
    output_file_name = "resource/crawler.csv"
    parent_dir = "output"

    # Step 1: Create crawler.csv if it doesn't exist
    #create_crawler_csv(input_file_name, output_file_name)

    # Step 2: Create output folders if they don't exist
    create_output_folders(output_file_name, parent_dir)

    # Step 3: Process videos in HTML files
    process_videos_in_html(parent_dir)

    # Step 4: Download videos from the source
    download_videos(parent_dir)

def create_crawler_csv(input_file_name, output_file_name):
    # Skip creation if the CSV already exists
    if os.path.exists(output_file_name):
        print(f"{output_file_name} already exists. Skipping creation.")
        return

    # Open the input CSV file
    with open(input_file_name, mode='r', newline='', encoding='latin-1') as infile:
        reader = csv.DictReader(infile)

        # Create the output CSV file
        with open(output_file_name, mode='w', newline='', encoding='latin-1') as outfile:
            fieldnames = ['WEBADDR', 'INSTNM']
            writer = csv.DictWriter(outfile, fieldnames=fieldnames)
            writer.writeheader()

            # Iterate over the records and process the URLs
            for row in reader:
                raw_url = row.get('WEBADDR')
                if raw_url:
                    full_url = ensure_https_scheme(raw_url)
                    if full_url:
                        parsed_url = tldextract.extract(full_url)
                        if parsed_url.suffix == "edu":
                            print("Adding {full_url}.")
                            instnm = row.get('INSTNM')
                            writer.writerow({'WEBADDR': full_url, 'INSTNM': instnm})
                        else:
                            print("Skipping {full_url}.")

def create_output_folders(output_file_name, parent_dir):
    ssl._create_default_https_context = ssl._create_unverified_context
    # Create the parent output directory if it doesn't exist
    if not os.path.exists(parent_dir):
        os.makedirs(parent_dir)

    # Open the crawler CSV file
    with open(output_file_name, mode='r', newline='', encoding='utf-8') as infile:
        reader = csv.DictReader(infile)

        # Iterate over the records and process the URLs
        for row in reader:
            raw_url = row.get('WEBADDR')
            print("Processing " + raw_url + " ...")
            if raw_url:
                full_url = ensure_https_scheme(raw_url)
                if full_url:
                    instnm = row.get('INSTNM').strip()
                    sanitized_instnm = sanitize_folder_name(instnm)

                    # Create folder named after INSTNM inside the parent directory if it doesn't exist
                    folder_name = os.path.join(parent_dir, sanitized_instnm)
                    html_output_path = os.path.join(folder_name, 'index.html')

                    # Skip fetching if the folder and HTML output already exist
                    if os.path.exists(folder_name) and os.path.exists(html_output_path):
                        print(f"Skipping {folder_name} as it already exists with index.html.")
                        # Write the full URL to base_url.txt
                        base_url_path = os.path.join(folder_name, 'base_url.txt')
                        with open(base_url_path, 'w', encoding='utf-8') as base_url_file:
                            base_url_file.write(full_url)
                        continue

                    if not os.path.exists(folder_name):
                        html_content = fetch_html(full_url)
                        if html_content:
                            os.makedirs(folder_name)
                            with open(html_output_path, 'w', encoding='utf-8') as html_file:
                                html_file.write(html_content)
                            # Write the full URL to base_url.txt
                            base_url_path = os.path.join(folder_name, 'base_url.txt')
                            with open(base_url_path, 'w', encoding='utf-8') as base_url_file:
                                base_url_file.write(full_url)

def process_videos_in_html(parent_dir):
    try:
        # Iterate through each subdirectory in the parent directory
        for entry in os.scandir(parent_dir):
            if entry.is_dir():
                subdir_path = entry.path
                html_file_path = os.path.join(subdir_path, "index.html")
                if os.path.exists(html_file_path):
                    video_elements = extract_video_elements(html_file_path)
                    save_video_elements(video_elements, subdir_path)
    except Exception as e:
        print(f"An error occurred: {e}")

def extract_video_elements(html_file_path):
    try:
        # Read the HTML file
        with open(html_file_path, 'r', encoding='utf-8') as file:
            html_content = file.read()
        
        # Parse the HTML content
        soup = BeautifulSoup(html_content, 'html.parser')

        # Find all video and iframe elements
        video_elements = soup.find_all(['video', 'iframe'])

        # Extract video elements
        video_elements_html = [str(video) for video in video_elements]

        return video_elements_html
    except Exception as e:
        print(f"An error occurred: {e}")
        return []

def save_video_elements(video_elements, output_dir):
    try:
        for i, element in enumerate(video_elements):
            # Define the output file path
            output_file_path = os.path.join(output_dir, f"video_{i + 1}.html")

            # Skip creation if the file already exists
            if os.path.exists(output_file_path):
                print(f"{output_file_path} already exists. Skipping creation.")
                continue

            # Save the video element to a new HTML file
            with open(output_file_path, 'w', encoding='utf-8') as file:
                file.write(element)
    except Exception as e:
        print(f"An error occurred: {e}")

def download_videos(parent_dir):
    try:
        # Iterate through each subdirectory in the parent directory
        for entry in os.scandir(parent_dir):
            if entry.is_dir():
                subdir_path = entry.path
                print(f"Processing {subdir_path}")
                for video_file in os.scandir(subdir_path):
                    if video_file.is_file() and video_file.name.startswith("video_"):
                        video_file_path = video_file.path
                        with open(video_file_path, 'r', encoding='utf-8') as file:
                            video_html = file.read()
                        
                        soup = BeautifulSoup(video_html, 'html.parser')
                        video_element = soup.find('video')
                        iframe_element = soup.find('iframe')

                        if video_element and video_element.get('src'):
                            src = video_element['src']
                            video_url = ensure_https_scheme(src)
                            video_filename = os.path.basename(src)
                            video_output_path = os.path.join(subdir_path, video_filename)

                            # Skip downloading if the video file already exists
                            if os.path.exists(video_output_path):
                                print(f"{video_output_path} already exists. Skipping download.")
                                continue

                            download_video(video_url, video_output_path)
                        elif iframe_element and iframe_element.get('src'):
                            src = iframe_element['src']
                            if "vimeo.com" in src:
                                vimeo_url = ensure_https_scheme(src)
                                vimeo_id = vimeo_url.split('/')[-1]
                                vimeo_api_url = f"https://player.vimeo.com/video/{vimeo_id}/config"

                                response = requests.get(vimeo_api_url)
                                vimeo_data = response.json()
                                video_src = vimeo_data["request"]["files"]["progressive"]["url"]
                                video_filename = f"vimeo_{vimeo_id}.mp4"
                                video_output_path = os.path.join(subdir_path, video_filename)

                                # Skip downloading if the video file already exists
                                if os.path.exists(video_output_path):
                                    print(f"{video_output_path} already exists. Skipping download.")
                                    continue

                                download_video(video_src, video_output_path)
                            else:
                                print(f"Video source URL for further examination: {src}")
    except Exception as e:
        print(f"An error occurred: {e}")

def ensure_https_scheme(url):
    if not url.startswith("http"):
        return "https://" + url.lstrip("/")
    return url

def download_video(video_url, output_path):
    try:
        response = requests.get(video_url)
        with open(output_path, 'wb') as file:
            file.write(response.content)
        print(f"Downloaded video from {video_url} to {output_path}")
    except Exception as e:
        print(f"An error occurred while downloading the video: {e}")

def ensure_https_scheme(url):
    parsed_url = urlparse(url)
    if not parsed_url.scheme:
        url = f"https://{url}"
    return url

def fetch_html(url):
    try:
        response = requests.get(url, verify=False, timeout=10)
    except requests.exceptions.RequestException as e:  # This is the correct syntax
        print("Request error, skipping")
        return None
    except requests.exceptions.Timeout:
        print("Request timed out, skipping")
        return None
    if response.status_code == 200:
        return response.text
    return None

def sanitize_folder_name(name):
    sanitized_name = re.sub(r'[^\w\s-]', '', name)
    return sanitized_name.replace(' ', '_')

if __name__ == "__main__":
    main()