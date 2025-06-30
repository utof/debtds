import pandas as pd
import requests
import time
import json
import os
from datetime import datetime

# Global variables
INPUT_CSV_PATH = "dummy_data.csv"    # Path to your input CSV
OUTPUT_CSV_PATH = "output.csv"  # Path where responses are saved
METADATA_CSV_PATH = "metadata.csv"  # Path for timeout metadata
FAKE_API = True                 # Set to False for real FSSP API calls
CURRENT_TIMEOUT = 1           # Starting timeout in seconds
TOKEN = "your_token_here"       # Replace with your FSSP API token

# Combined API call function (fake or real based on FAKE_API)
def api_call(ip, timeout):
    if FAKE_API:
        time.sleep(2)  # Simple 2-second delay for fake call
        return {"status": 200, "data": f"Fake response for {ip}"}
    else:
        url = f"https://api-cloud.ru/api/fssp.php?type=ip&number={ip}&token={TOKEN}"
        try:
            response = requests.get(url, timeout=timeout)
            response.raise_for_status()  # Raise exception for bad status codes
            return response.json()
        except requests.exceptions.Timeout:
            return {"status": "timeout"}
        except requests.exceptions.RequestException as e:
            return {"status": e.response.status_code if e.response else 500}

# Process a single row with dynamic timeout and metadata
def process_row(row, index):
    global CURRENT_TIMEOUT
    ip = row["ip"]  # Assumes 'ip' column exists in CSV
    start_time = time.time()
    response = api_call(ip, CURRENT_TIMEOUT)
    end_time = time.time()
    time_taken = end_time - start_time

    # Check if call was successful
    success = response.get("status") == 200
    if success:
        # Reduce timeout: try 200s, then lower, but not below 60s
        if CURRENT_TIMEOUT > 200:
            CURRENT_TIMEOUT = 200
        else:
            CURRENT_TIMEOUT = max(60, CURRENT_TIMEOUT * 0.8)  # Gradually reduce
    else:
        # Increase timeout on failure (timeout or 500), up to 400s
        CURRENT_TIMEOUT = min(400, CURRENT_TIMEOUT * 1.5)

    # Store metadata for analysis
    metadata = {
        "timeout_used": CURRENT_TIMEOUT,
        "time_taken": time_taken,
        "success": success,
        "time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }

    # Return row with response and metadata
    return {"fssp_response": json.dumps(response), **row}, metadata

# Save responses to CSV
def save_to_csv(data, filepath, append=False):
    df = pd.DataFrame(data)
    if append and os.path.exists(filepath):
        df.to_csv(filepath, mode='a', header=False, index=False)
    else:
        df.to_csv(filepath, index=False)

# Main function to process CSV and save results
def main():
    # Read input CSV
    df = pd.read_csv(INPUT_CSV_PATH)
    results = []         # Store API responses
    metadata_list = []   # Store metadata for each call

    # Process each row
    for index, row in df.iterrows():
        result, metadata = process_row(row.to_dict(), index)
        results.append(result)
        metadata_list.append(metadata)

    # Handle output.csv filename logic
    date_str = datetime.now().strftime("%d%m%y")
    base, ext = os.path.splitext(OUTPUT_CSV_PATH)
    output_path = f"{base}{date_str}{ext}"
    counter = 1
    while os.path.exists(output_path):
        output_path = f"{base}{date_str}_{counter}{ext}"
        counter += 1

    # Save results to output file (never append, always new)
    save_to_csv(results, output_path)

    # Save or append metadata
    save_to_csv(metadata_list, METADATA_CSV_PATH, append=True)

# Run the script
if __name__ == "__main__":
    main()