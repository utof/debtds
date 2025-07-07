import pandas as pd
import requests
import time
import json
import os
from datetime import datetime
from fssp020_call import api_call


# Global variables
CURRENT_TIMEOUT = 400           # Starting timeout in seconds

# Process a single row with dynamic timeout (metadata removed)
def process_row(row, index, fake_api):
    ip = row["ip"]  # Assumes 'ip' column exists in CSV
    start_time = time.time()
    response = api_call(ip, fake_api)
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
# Now accepts input and output paths as arguments for pipeline compatibility
def main(input_csv_path, output_csv_path, fake_api=False):
    df = pd.read_csv(input_csv_path)
    results = []         # Store API responses

    # Handle output.csv filename logic
    date_str = datetime.now().strftime("%d%m%y")
    base, ext = os.path.splitext(output_csv_path)
    output_path = f"{base}{date_str}{ext}"
    counter = 1
    while os.path.exists(output_path):
        output_path = f"{base}{date_str}_{counter}{ext}"
        counter += 1

    # Process each row
    for index, row in df.iterrows():
        result = process_row(row.to_dict(), index, fake_api)
        results.append(result)
        save_to_csv([result], output_path, append=True)

# Run the script
if __name__ == "__main__":
    INPUT_CSV_PATH = 'example.csv'  # Default input CSV path
    OUTPUT_CSV_PATH = 'output.csv'   # Default output CSV path
    main(input_csv_path=INPUT_CSV_PATH, output_csv_path=OUTPUT_CSV_PATH, fake_api=False)