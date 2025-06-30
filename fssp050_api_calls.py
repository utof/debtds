import pandas as pd
import requests
import time
import json
import os
from datetime import datetime
from fssp020_call import api_call


# Global variables
INPUT_CSV_PATH = "example.csv"    # Path to your input CSV
OUTPUT_CSV_PATH = "output.csv"  # Path where responses are saved
METADATA_CSV_PATH = "metadata.csv"  # Path for timeout metadata
FAKE_API = False                 # Set to False for real FSSP API calls
CURRENT_TIMEOUT = 400           # Starting timeout in seconds

# Process a single row with dynamic timeout (metadata removed)
def process_row(row, index):
    global CURRENT_TIMEOUT
    ip = row["ip"]  # Assumes 'ip' column exists in CSV
    start_time = time.time()
    response = api_call(ip, CURRENT_TIMEOUT, FAKE_API)
    end_time = time.time()
    # Return row with response only
    return {"fssp_resp": json.dumps(response, ensure_ascii=False)}

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
        result = process_row(row.to_dict(), index)
        results.append(result)
        save_to_csv([result], output_path, append=True)

# Run the script
if __name__ == "__main__":
    main()