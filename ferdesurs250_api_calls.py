import pandas as pd
import json
import os
from fedresurs230_call import fedresurs_call

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

    # --- Process Debtor INNs ---
    debtor_json_path = 'debtor_responses.json'
    # Load existing debtor data if the file exists
    if os.path.exists(debtor_json_path):
        with open(debtor_json_path, 'r', encoding='utf-8') as f:
            debtor_data = json.load(f)
    else:
        debtor_data = {}

    # Process each unique debtor INN
    for inn in unique_debtor_inns:
        inn_str = str(inn)  # Convert to string for JSON keys
        if inn_str not in debtor_data:
            # Make API call only if we don’t have data for this INN
            response = fedresurs_call(inn, fake_api)
            debtor_data[inn_str] = response
            # Save the updated data immediately
            with open(debtor_json_path, 'w', encoding='utf-8') as f:
                json.dump(debtor_data, f, ensure_ascii=False, indent=4)

    # --- Process Creditor INNs ---
    creditor_json_path = 'creditor_responses.json'
    # Load existing creditor data if the file exists
    if os.path.exists(creditor_json_path):
        with open(creditor_json_path, 'r', encoding='utf-8') as f:
            creditor_data = json.load(f)
    else:
        creditor_data = {}

    # Process each unique creditor INN
    for inn in unique_creditor_inns:
        inn_str = str(inn)  # Convert to string for JSON keys
        if inn_str not in creditor_data:
            # Make API call only if we don’t have data for this INN
            response = fedresurs_call(inn, fake_api)
            creditor_data[inn_str] = response
            # Save the updated data immediately
            with open(creditor_json_path, 'w', encoding='utf-8') as f:
                json.dump(creditor_data, f, ensure_ascii=False, indent=4)

if __name__ == "__main__":
    INPUT_CSV_PATH = '3testinput.csv'  # Replace with your CSV file path
    main(input_csv_path=INPUT_CSV_PATH, fake_api=True)  # Set fake_api=False for real API calls