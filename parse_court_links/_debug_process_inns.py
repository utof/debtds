# File: main_runner.py

import json
import logging
import os
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv

# --- MODIFIED IMPORTS ---
# We only need search_cases and the relevant status constants now.
from api_processor import (
    search_cases,
    RESULT_API_RETRY_ERROR,
    RESULT_API_ERROR,
    RESULT_NO_CASES_FOUND
)

# --- Configuration (Unchanged) ---
log_file_path = "main_runner.log"
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(log_file_path, mode='w', encoding='utf-8'),
        logging.StreamHandler()
    ]
)

load_dotenv()

# --- Helper Functions for Data Persistence (Unchanged) ---

def load_or_create_results(file_path: Path) -> dict:
    """Loads the results JSON file, or creates an empty dict if it doesn't exist."""
    if not file_path.exists():
        logging.info(f"Results file '{file_path}' not found. Starting fresh.")
        return {}
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            data = json.load(f)
            logging.info(f"Successfully loaded {len(data)} processed pairs from '{file_path}'.")
            return data
    except (json.JSONDecodeError, IOError) as e:
        logging.warning(f"Could not read or parse results file: {e}. Starting fresh.")
        return {}

def save_results(file_path: Path, data: dict):
    """Saves the results dictionary to a JSON file."""
    try:
        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=4)
    except IOError as e:
        logging.error(f"Could not save results to {file_path}: {e}")

# --- Main Execution Logic (Modified) ---

def main():
    """Main function to run the simplified search-and-save process."""
    logging.info("--- Script Execution Started (Search Only Mode) ---")
    
    token = os.getenv("api_cloud")
    if not token:
        logging.error("FATAL: 'api_cloud' token not found in .env file. Please create it.")
        return

    # --- File Paths ---
    script_dir = Path(__file__).parent
    input_file = script_dir / "_test1.csv"
    results_file = script_dir / "_test1results_search_only.json"  # Using a new name for clarity
    output_file = script_dir / "_test1output_search_only.csv"
    
    # --- Column Names ---
    debtor_col = "debtor_inn"
    creditor_col = "creditor_inn"
    output_col = "search_results" # Renamed output column for clarity

    if not input_file.exists():
        logging.error(f"Input file not found at: {input_file}")
        return

    try:
        df = pd.read_csv(input_file, dtype=str).fillna('')
        logging.info(f"Loaded {len(df)} rows from {input_file}.")
        
        if debtor_col not in df.columns or creditor_col not in df.columns:
            logging.error(f"CSV must contain '{debtor_col}' and '{creditor_col}' columns.")
            return

        results_data = load_or_create_results(results_file)

        # --- Identify which pairs need processing ---
        df['processing_key'] = df[debtor_col] + '|' + df[creditor_col]
        unique_keys = df['processing_key'].unique()
        
        keys_to_process = [key for key in unique_keys if key not in results_data]
        logging.info(f"Found {len(unique_keys)} unique INN pairs. {len(keys_to_process)} pairs need processing.")

        # --- Loop through and process only the new pairs ---
        for i, key in enumerate(keys_to_process):
            logging.info(f"--- Processing new pair {i+1}/{len(keys_to_process)}: {key} ---")
            debtor_inn, creditor_inn = key.split('|')
            
            if not debtor_inn or not creditor_inn:
                result_to_save = "Invalid INN provided"
            else:
                # Call ONLY the search_cases function
                case_ids, is_retryable = search_cases(token, debtor_inn, creditor_inn)

                # If it was a network error, skip this key for now. It won't be saved.
                if is_retryable:
                    logging.warning(f"Pair '{key}' failed with a network error. It will be skipped and can be retried on the next run.")
                    continue

                # Determine what to save based on the result of the search
                if case_ids is None:
                    result_to_save = RESULT_API_ERROR
                elif not case_ids:
                    result_to_save = RESULT_NO_CASES_FOUND
                else:
                    # Success! Save the list of case IDs.
                    result_to_save = case_ids
            
            # Save the result (list of IDs or status string) to our data store
            results_data[key] = result_to_save
            save_results(results_file, results_data)
            logging.info(f"Result for '{key}' saved to '{results_file.name}'.")
        
        # --- Finalize the output CSV ---
        logging.info("All new pairs processed. Mapping results to the final DataFrame.")
        df[output_col] = df['processing_key'].map(results_data)
        
        # For any rows that failed with a network error, fill with the retry message
        df[output_col].fillna(RESULT_API_RETRY_ERROR, inplace=True)
        
        df.drop(columns=['processing_key'], inplace=True)
        df.to_csv(output_file, index=False, encoding='utf-8-sig')
        logging.info(f"--- Script Execution Finished. Results saved to {output_file} ---")

    except Exception as e:
        logging.error(f"An unexpected error occurred during file processing: {e}", exc_info=True)


if __name__ == "__main__":
    main()