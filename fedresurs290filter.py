import pandas as pd
import json

def add_status_columns(input_csv_path, debtor_json_path, creditor_json_path, output_csv_path=False):
    """Add status_debtor and status_creditor columns to the CSV based on JSON status codes."""
    # Load CSV and JSON files
    df = pd.read_csv(input_csv_path)
    with open(debtor_json_path, 'r', encoding='utf-8') as f:
        debtor_statuses = json.load(f)
    with open(creditor_json_path, 'r', encoding='utf-8') as f:
        creditor_statuses = json.load(f)

    # Add status columns by mapping INNs to their codes
    df['status_debtor'] = df['inn_debtor'].astype(str).map(debtor_statuses)
    df['status_creditor'] = df['inn_creditor'].astype(str).map(creditor_statuses)

    # Fill missing statuses with -1 (indicating no data)
    df['status_debtor'] = df['status_debtor'].fillna(-1).astype(int)
    df['status_creditor'] = df['status_creditor'].fillna(-1).astype(int)

    # Save to CSV if output path is provided
    if output_csv_path:
        df.to_csv(output_csv_path, index=False)
        print(f"Statuses added and saved to {output_csv_path}")

    return df

def filter_status_rows(df, output_csv_path=False):
    """Filter rows to keep only status_debtor in [0, 1, 4] and status_creditor in [0, 4]."""
    # Filter rows based on status conditions
    filtered_df = df[
        (df['status_debtor'].isin([0, 1, 4])) &
        (df['status_creditor'].isin([0, 4]))
    ]

    # Save to CSV if output path is provided
    if output_csv_path:
        filtered_df.to_csv(output_csv_path, index=False)
        print(f"Filtered statuses saved to {output_csv_path}")

    return filtered_df

def drop_status_columns(df, output_csv_path=True):
    """Drop status_debtor and status_creditor columns from the DataFrame."""
    # Create a new DataFrame without status columns
    result_df = df.drop(columns=['status_debtor', 'status_creditor'])

    # Save to CSV (default is True for final output)
    if output_csv_path:
        result_df.to_csv(output_csv_path, index=False)
        print(f"Statuses dropped and saved to {output_csv_path}")

    return result_df

if __name__ == "__main__":
    INPUT_CSV_PATH = '3fssp.csv'
    DEBTOR_JSON_PATH = 'debtor_codes.json'
    CREDITOR_JSON_PATH = 'creditor_codes.json'
    OUTPUT_CSV_1 = 'with_statuses.csv'
    OUTPUT_CSV_2 = 'filtered_statuses.csv'
    OUTPUT_CSV_3 = 'final_output.csv'

    # Step 1: Add status columns
    df_with_statuses = add_status_columns(INPUT_CSV_PATH, DEBTOR_JSON_PATH, CREDITOR_JSON_PATH, OUTPUT_CSV_1)

    # Step 2: Filter rows based on statuses
    df_filtered = filter_status_rows(df_with_statuses, OUTPUT_CSV_2)

    # Step 3: Drop status columns
    drop_status_columns(df_filtered, OUTPUT_CSV_3)