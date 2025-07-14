import requests
import csv
import os
import json
import datetime
import concurrent.futures
import re

# Requirements
# - requests
# - python-dateutil (for parsing dates)

# Configuration
CMS_API_URL = "https://data.cms.gov/provider-data/api/1/metastore/schemas/dataset/items"
OUTPUT_DIR = "hospital_datasets"
RUN_METADATA_FILE = "run_metadata.json"

def get_datasets(theme):
    response = requests.get(CMS_API_URL, params={"theme": theme})
    response.raise_for_status()
    return response.json()["items"]

def download_dataset(dataset):
    dataset_id = dataset["id"]
    url = f"{CMS_API_URL}/{dataset_id}/data"
    response = requests.get(url, stream=True)
    response.raise_for_status()
    filename = f"{dataset_id}.csv"
    filepath = os.path.join(OUTPUT_DIR, filename)
    with open(filepath, "wb") as f:
        for chunk in response.iter_content(chunk_size=1024):
            f.write(chunk)
    return filepath

def process_dataset(filepath):
    with open(filepath, "r") as f:
        reader = csv.reader(f)
        headers = next(reader)
        snake_case_headers = [re.sub(r"\W+", "_", header).lower() for header in headers]
        with open(filepath, "w", newline="") as g:
            writer = csv.writer(g)
            writer.writerow(snake_case_headers)
            for row in reader:
                writer.writerow(row)

def get_run_metadata():
    if os.path.exists(RUN_METADATA_FILE):
        with open(RUN_METADATA_FILE, "r") as f:
            return json.load(f)
    else:
        return {"last_run": None, "downloaded_datasets": []}

def update_run_metadata(metadata):
    with open(RUN_METADATA_FILE, "w") as f:
        json.dump(metadata, f)

def main():
    theme = "Hospitals"
    datasets = get_datasets(theme)
    metadata = get_run_metadata()
    last_run = metadata["last_run"]
    downloaded_datasets = metadata["downloaded_datasets"]

    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = []
        for dataset in datasets:
            dataset_id = dataset["id"]
            if last_run is None or dataset["modified"] > last_run:
                futures.append(executor.submit(download_dataset, dataset))
            else:
                print(f"Skipping {dataset_id} (not modified since last run)")

        for future in concurrent.futures.as_completed(futures):
            filepath = future.result()
            process_dataset(filepath)
            downloaded_datasets.append(dataset_id)

    metadata["last_run"] = datetime.datetime.now().isoformat()
    metadata["downloaded_datasets"] = downloaded_datasets
    update_run_metadata(metadata)

if __name__ == "__main__":
    main()