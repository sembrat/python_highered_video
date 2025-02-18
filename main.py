import csv
import os
import requests
import re
import tldextract
import ssl
from urllib.parse import urlparse

def main():
    input_file_name = "resource/hd2023.csv"
    output_file_name = "resource/crawler.csv"
    parent_dir = "output"

    # Step 1: Create crawler.csv if it doesn't exist
    #create_crawler_csv(input_file_name, output_file_name)

    # Step 2: Create output folders if they don't exist
    create_output_folders(output_file_name, parent_dir)

    # Step 3: Process videos in HTML files
    # process_videos_in_html(parent_dir)

    # Step 4: Download videos from the source
    # download_videos(parent_dir)

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

def ensure_https_scheme(url):
    parsed_url = urlparse(url)
    if not parsed_url.scheme:
        url = f"https://{url}"
    return url

def fetch_html(url):
    response = requests.get(url, verify=False)
    if response.status_code == 200:
        return response.text
    return None

def sanitize_folder_name(name):
    sanitized_name = re.sub(r'[^\w\s-]', '', name)
    return sanitized_name.replace(' ', '_')

if __name__ == "__main__":
    main()