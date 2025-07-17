# File: main_runner.py

import json
import logging
import os
from pathlib import Path
from typing import Any, Dict, List

import pandas as pd
from dotenv import load_dotenv

# Import the necessary functions and constants from our processor module
from api_processor import (
    search_cases,
    get_case_info,
    RESULT_API_RETRY_ERROR,
    RESULT_API_ERROR,
    RESULT_NO_CASES_FOUND
)

# --- Configuration ---
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

# --- Helper Functions for Data Persistence ---

def load_or_create_json(file_path: Path) -> Dict[str, Any]:
    """Loads a JSON file, or creates an empty dict if it doesn't exist."""
    if not file_path.exists():
        logging.info(f"JSON file '{file_path.name}' not found. Starting with an empty dictionary.")
        return {}
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            data = json.load(f)
            logging.info(f"Successfully loaded {len(data)} items from '{file_path.name}'.")
            return data
    except (json.JSONDecodeError, IOError) as e:
        logging.warning(f"Could not read or parse JSON file '{file_path.name}': {e}. Starting fresh.")
        return {}

def save_json(file_path: Path, data: Dict[str, Any]):
    """Saves a dictionary to a JSON file."""
    try:
        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=4)
    except IOError as e:
        logging.error(f"Could not save data to {file_path}: {e}")

# --- PHASE 1 FUNCTION ---

def run_search_phase(token: str, input_csv_path: Path, search_results_path: Path, output_csv_path: Path):
    """
    Phase 1: Reads the input CSV, runs 'search_cases' for each unique INN pair,
    and saves the results to a JSON file and a summary CSV.
    """
    logging.info("--- Starting Phase 1: Searching for Case IDs ---")
    
    try:
        df = pd.read_csv(input_csv_path, dtype=str).fillna('')
        logging.info(f"Loaded {len(df)} rows from {input_csv_path.name}.")
    except FileNotFoundError:
        logging.error(f"Input file not found at: {input_csv_path}")
        return

    debtor_col, creditor_col = "debtor_inn", "creditor_inn"
    if debtor_col not in df.columns or creditor_col not in df.columns:
        logging.error(f"CSV must contain '{debtor_col}' and '{creditor_col}' columns.")
        return

    search_results_data = load_or_create_json(search_results_path)

    df['processing_key'] = df[debtor_col] + '|' + df[creditor_col]
    unique_keys = df['processing_key'].unique()
    
    keys_to_process = [key for key in unique_keys if key not in search_results_data]
    logging.info(f"Found {len(unique_keys)} unique INN pairs. {len(keys_to_process)} pairs need processing.")

    for i, key in enumerate(keys_to_process):
        logging.info(f"--- Processing new pair {i+1}/{len(keys_to_process)}: {key} ---")
        debtor_inn, creditor_inn = key.split('|')
        
        if not debtor_inn or not creditor_inn:
            result_to_save = "Invalid INN provided"
        else:
            case_ids, is_retryable = search_cases(token, debtor_inn, creditor_inn)
            if is_retryable:
                logging.warning(f"Pair '{key}' failed with a network error. It will be skipped.")
                continue
            result_to_save = case_ids if case_ids is not None and case_ids else (RESULT_API_ERROR if case_ids is None else RESULT_NO_CASES_FOUND)
        
        search_results_data[key] = result_to_save
        save_json(search_results_path, search_results_data)
        logging.info(f"Result for '{key}' saved to '{search_results_path.name}'.")
    
    logging.info("Mapping search results to the final DataFrame.")
    df['search_results'] = df['processing_key'].map(search_results_data).fillna(RESULT_API_RETRY_ERROR)
    df.drop(columns=['processing_key'], inplace=True)
    df.to_csv(output_csv_path, index=False, encoding='utf-8-sig')
    logging.info(f"--- Phase 1 Finished. Summary CSV saved to {output_csv_path.name} ---")

# --- PHASE 2 FUNCTION ---

def run_case_info_phase(token: str, search_results_path: Path, case_info_details_path: Path):
    """
    Phase 2: Reads the JSON from Phase 1, extracts all Case IDs,
    runs 'get_case_info' for each ID, and saves the detailed responses to a new JSON file.
    """
    logging.info("--- Starting Phase 2: Fetching Case Info Details ---")

    search_results_data = load_or_create_json(search_results_path)
    if not search_results_data:
        logging.error(f"Search results file '{search_results_path.name}' is empty or not found. Cannot proceed with Phase 2.")
        return

    case_info_cache = load_or_create_json(case_info_details_path)

    # Extract all unique case IDs from the search results
    all_case_ids = set()
    for value in search_results_data.values():
        if isinstance(value, list):  # We only care about successful results which are lists of IDs
            for case_id in value:
                all_case_ids.add(case_id)

    # Determine which case IDs we haven't fetched yet
    ids_to_fetch = [case_id for case_id in all_case_ids if case_id not in case_info_cache]
    logging.info(f"Found {len(all_case_ids)} unique Case IDs in total. {len(ids_to_fetch)} new IDs to fetch.")

    for i, case_id in enumerate(ids_to_fetch):
        logging.info(f"--- Fetching details for Case ID {i+1}/{len(ids_to_fetch)}: {case_id} ---")
        
        case_info_response, is_retryable = get_case_info(token, case_id)
        
        if is_retryable:
            logging.warning(f"Network error for Case ID '{case_id}'. It will be skipped and can be retried on the next run.")
            continue # Skip to the next ID without saving

        # Save the result, even if it's None (which indicates a non-retryable API error for that ID)
        case_info_cache[case_id] = case_info_response
        save_json(case_info_details_path, case_info_cache)
        logging.info(f"Result for Case ID '{case_id}' saved to '{case_info_details_path.name}'.")

    logging.info(f"--- Phase 2 Finished. All new Case ID details are saved in {case_info_details_path.name} ---")


# --- MAIN ORCHESTRATOR ---

def main():
    """
    Main orchestrator. Uncomment the phase you want to run.
    """
    token = os.getenv("api_cloud")
    if not token:
        logging.error("FATAL: 'api_cloud' token not found in .env file. Please create it.")
        return

    # --- Define all file paths in one place ---
    script_dir = Path(__file__).parent
    # input_csv = script_dir / "testdata17.04.25.csv"
    input_csv = script_dir / "_test1.csv"
    
    # Files for Phase 1
    search_results_json = script_dir / "_1test1results_search_only.json"
    search_output_csv = script_dir / "output_search_only.csv"
    
    # File for Phase 2
    case_info_details_json = script_dir / "case_info_details.json"

    # ------------------------------------------------------------------
    # CHOOSE WHICH PHASE TO RUN BY UNCOMMENTING THE DESIRED LINE
    # ------------------------------------------------------------------

    # To run Phase 1 (Search for cases and create the first JSON and CSV):
    # run_search_phase(token, input_csv, search_results_json, search_output_csv)

    # To run Phase 2 (Use the first JSON to fetch details for each case ID):
    run_case_info_phase(token, search_results_json, case_info_details_json)
    # ------------------------------------------------------------------


if __name__ == "__main__":
    main()