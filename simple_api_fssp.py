import pandas as pd
import requests
import time
import json
import os
from datetime import datetime
from fssp_call import api_call


# Global variables
INPUT_CSV_PATH = "example.csv"    # Path to your input CSV
OUTPUT_CSV_PATH = "output.csv"  # Path where responses are saved
METADATA_CSV_PATH = "metadata.csv"  # Path for timeout metadata
FAKE_API = True                 # Set to False for real FSSP API calls
CURRENT_TIMEOUT = 61           # Starting timeout in seconds

# Process a single row with dynamic timeout and metadata
def process_row(row, index):
    global CURRENT_TIMEOUT # TODO: make addMetadata, timeout shenanigans as params?
    ip = row["ip"]  # Assumes 'ip' column exists in CSV
    start_time = time.time()
    response = api_call(ip, CURRENT_TIMEOUT, FAKE_API)
    end_time = time.time()
    time_taken = end_time - start_time

    success = response.get("status") == 200
    # if success:
    #     # Reduce timeout: try 200s, then lower, but not below 60s
    #     if CURRENT_TIMEOUT > 200:
    #         CURRENT_TIMEOUT = 200
    #     else:
    #         CURRENT_TIMEOUT = max(60, CURRENT_TIMEOUT * 0.8)  # Gradually reduce
    # else:
    #     # Increase timeout on failure (timeout or 500), up to 400s
    #     CURRENT_TIMEOUT = min(400, CURRENT_TIMEOUT * 1.5)

    # Store metadata for analysis
    metadata = {
        "timeout_used": CURRENT_TIMEOUT,
        "time_taken": time_taken,
        "success": success,
        "time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }

    # Return row with response and metadata
    # Use ensure_ascii=False to preserve Cyrillics in JSON
    return {"fssp_resp": json.dumps(response, ensure_ascii=False)}, metadata

# Save responses to CSV
def save_to_csv(data, filepath, append=False):
    df = pd.DataFrame(data)
    if append and os.path.exists(filepath):
        df.to_csv(filepath, mode='a', header=False, index=False, encoding='utf-8-sig')
    else:
        df.to_csv(filepath, index=False, encoding='utf-8-sig')
    print(f'saved {data} to {filepath}')

# Main function to process CSV and save results
def main():
    # Read input CSV
    df = pd.read_csv(INPUT_CSV_PATH)
    results = []         # Store API responses
    metadata_list = []   # Store metadata for each call

    # Handle output.csv filename logic
    date_str = datetime.now().strftime("%d%m%y")
    base, ext = os.path.splitext(OUTPUT_CSV_PATH)
    output_path = f"{base}{date_str}{ext}"
    counter = 1
    while os.path.exists(output_path):
        output_path = f"{base}{date_str}_{counter}{ext}"
        counter += 1

    # Process each row
    for index, row in df.iterrows():
        result, metadata = process_row(row.to_dict(), index)
        results.append(result)
        metadata_list.append(metadata)
        save_to_csv(results, output_path, append=True)
        save_to_csv(metadata_list, METADATA_CSV_PATH, append=True)  

# Run the script
if __name__ == "__main__":
    main()