import pandas as pd
import json
import os
from fedresurs230_call import fedresurs_call

def process_inns(unique_inns, json_path, fake_api):
    """Load, update, and save API responses for a set of INNs to a JSON file."""
    # Load existing data if the file exists
    if os.path.exists(json_path):
        with open(json_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
    else:
        data = {}

    # Process each unique INN
    for inn in unique_inns:
        inn_str = str(inn)  # Convert to string for JSON keys
        if inn_str not in data:
            # Make API call only if we don’t have data for this INN
            response = fedresurs_call(inn, fake_api)
            data[inn_str] = response
            # Save the updated data immediately
            with open(json_path, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=4)

    return data

def main(input_csv_path, fake_api=False):
    """Process a CSV file and save Fedresurs API responses to JSON files."""
    # Read the CSV file
    df = pd.read_csv(input_csv_path)
    
    # Check if required columns exist
    if 'inn_debtor' not in df.columns or 'inn_creditor' not in df.columns:
        raise ValueError("CSV must have 'inn_debtor' and 'inn_creditor' columns")

    # Get unique INNs
    unique_debtor_inns = df['inn_debtor'].unique()
    unique_creditor_inns = df['inn_creditor'].unique()

    # Process Debtor INNs
    process_inns(unique_debtor_inns, 'debtor_responses.json', fake_api)
    # Process Creditor INNs
    process_inns(unique_creditor_inns, 'creditor_responses.json', fake_api)

if __name__ == "__main__":
    INPUT_CSV_PATH = '3fssp.csv'  # Replace with your CSV file path
    main(input_csv_path=INPUT_CSV_PATH, fake_api=False)  # Set fake_api=False for real API calls