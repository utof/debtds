import json
import logging
import os
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import requests
from dotenv import load_dotenv

# --- 1. CONFIGURATION: SET YOUR DEBUG PARAMETERS HERE ---

# Globally defined INNs for the debug run
DEBTOR_INN = "7536165991"  # ИНН Должника
CREDITOR_INN = "7536169450" # ИНН Кредитора

# --- End of Configuration ---


# --- Constants ---
BASE_URL = "https://api-cloud.ru/api/kad_arbitr.php"
API_TIMEOUT = 120
DEBUG_LOG_FILE = "debug_run.log"
DEBUG_JSON_DUMP_FILE = "debug_api_responses.json"

# This global dictionary will store all API responses for the final dump
API_RESPONSES_DUMP = {}


# --- 2. LOGGING SETUP ---

def setup_logger():
    """Configures a logger to output to console and a UTF-8 encoded file."""
    # Get the root logger
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    # Prevent adding duplicate handlers if this function is called multiple times
    if logger.hasHandlers():
        logger.handlers.clear()

    # Create a formatter
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

    # Console handler
    ch = logging.StreamHandler()
    ch.setFormatter(formatter)
    logger.addHandler(ch)

    # File handler with UTF-8 encoding for Cyrillic support
    fh = logging.FileHandler(DEBUG_LOG_FILE, mode='w', encoding='utf-8')
    fh.setFormatter(formatter)
    logger.addHandler(fh)

# Initialize the logger immediately
setup_logger()


# --- 3. API AND DATA PROCESSING FUNCTIONS (with extensive logging) ---

def get_api_token() -> Optional[str]:
    """Retrieves the API token from environment variables."""
    load_dotenv()
    token = os.getenv("api_cloud")
    if not token:
        logging.error("API_CLOUD_TOKEN not found in .env file.")
    else:
        logging.info("API token loaded successfully.")
    return token


def make_api_request(params: List[Tuple[str, str]], request_key: str) -> Optional[Dict]:
    """Makes a GET request, logs everything, and stores the response."""
    logging.info(f"Preparing to make API request '{request_key}'")
    logging.info(f"Request Parameters: {params}")
    
    try:
        response = requests.get(BASE_URL, params=params, timeout=API_TIMEOUT)
        logging.info(f"API Response Status Code: {response.status_code}")
        response.raise_for_status()
        
        json_response = response.json()
        
        # Log and dump the raw JSON response
        pretty_json = json.dumps(json_response, ensure_ascii=False, indent=4)
        logging.info(f"Raw JSON response for '{request_key}':\n{pretty_json}")
        API_RESPONSES_DUMP[request_key] = json_response
        
        if json_response.get("status") != 200:
            error_msg = json_response.get("errormsg", "Unknown API error")
            logging.warning(f"API returned non-200 status: {error_msg}")
            return None
            
        return json_response

    except requests.exceptions.RequestException as e:
        logging.error(f"API request failed for '{request_key}': {e}")
        API_RESPONSES_DUMP[request_key] = {"error": str(e)}
        return None


def search_cases(token: str, debtor_inn: str, creditor_inn: str) -> Optional[List[str]]:
    """Performs a 'search' API call to find case IDs."""
    logging.info(f"--- Step 1: Searching for cases with Debtor INN: {debtor_inn}, Creditor INN: {creditor_inn} ---")
    params = [
        ("token", token), ("type", "search"), ("CaseType", "G"),
        ("participant", debtor_inn), ("participantType", "1"),
        ("participant", creditor_inn), ("participantType", "0"),
    ]
    request_key = f"search_{debtor_inn}_{creditor_inn}"
    response = make_api_request(params, request_key)

    if response and response.get("Result"):
        case_ids = [item["caseId"] for item in response["Result"] if "caseId" in item]
        logging.info(f"Search successful. Found {len(case_ids)} case(s). Case IDs: {case_ids}")
        return case_ids
    
    logging.warning("Search returned no results or an error occurred.")
    return []


def get_case_info(token: str, case_id: str) -> Optional[Dict]:
    """Performs a 'caseInfo' API call."""
    logging.info(f"--- Step 2: Fetching detailed info for CaseId: {case_id} ---")
    params = [("token", token), ("type", "caseInfo"), ("CaseId", case_id)]
    request_key = f"caseInfo_{case_id}"
    response = make_api_request(params, request_key)
    
    if response and response.get("found"):
        logging.info(f"Successfully fetched details for CaseId: {case_id}")
        return response.get("Result")
        
    logging.warning(f"Could not find or fetch details for CaseId: {case_id}")
    return None


def filter_and_extract_documents(case_info_json: Dict) -> List[Dict]:
    """Filters events to find specific court decisions and logs the process."""
    logging.info(f"--- Step 3: Filtering documents for case {case_info_json.get('CaseInfo', {}).get('CaseNumber', 'N/A')} ---")
    documents = []
    if not case_info_json or "CaseInstances" not in case_info_json:
        logging.warning("No 'CaseInstances' found in the JSON to filter.")
        return documents

    for i, instance in enumerate(case_info_json.get("CaseInstances", [])):
        logging.info(f"Checking instance {i+1}...")
        for j, event in enumerate(instance.get("InstanceEvents", [])):
            event_type = event.get("EventTypeName", "")
            content_types = event.get("ContentTypes", [])
            
            logging.info(f"  - Event {j+1}: Type='{event_type}', Content='{content_types}'")
            
            is_decision_event = False
            if event_type == "Решение":
                is_decision_event = True
                logging.info("    -> Match found: EventTypeName is 'Решение'.")
            elif event_type == "Решения и постановления":
                if any("решение" in str(ct).lower() for ct in content_types):
                    is_decision_event = True
                    logging.info("    -> Match found: EventTypeName is 'Решения и постановления' and ContentTypes contains 'решение'.")
            
            if is_decision_event:
                file_url = event.get("File")
                date = event.get("Date")
                if file_url and date:
                    logging.info(f"    -> SUCCESS: Extracted document: Date='{date}', File='{file_url}'")
                    documents.append({"Date": date, "File": file_url})
                else:
                    logging.warning("    -> Matched event but missing File URL or Date.")
    
    logging.info(f"Filtering complete. Found {len(documents)} relevant document(s) for this case.")
    return documents


def format_results(documents: List[Dict]) -> str:
    """Formats the list of found documents into a numbered string."""
    logging.info("--- Step 4: Formatting final result string ---")
    if not documents:
        logging.info("No documents to format. Returning 'Подходящие документы не найдены'.")
        return "Подходящие документы не найдены"
    
    try:
        sorted_docs = sorted(
            documents, key=lambda x: pd.to_datetime(x['Date'], format='%d.%m.%Y'), reverse=True
        )
    except Exception:
        logging.warning("Could not sort documents by date. Using original order.")
        sorted_docs = documents

    formatted_lines = [
        f"{i+1}. {doc['Date']}: {doc['File']}" for i, doc in enumerate(sorted_docs)
    ]
    final_string = "\n".join(formatted_lines)
    logging.info(f"Final formatted result:\n{final_string}")
    return final_string


# --- 4. MAIN DEBUG EXECUTION ---

def main_debug():
    """Main function to run the entire debug script."""
    logging.info("========== STARTING DEBUG SCRIPT ==========")
    
    token = get_api_token()
    if not token:
        logging.error("Execution stopped due to missing API token.")
        return

    case_ids = search_cases(token, DEBTOR_INN, CREDITOR_INN)

    if not case_ids:
        logging.warning("No case IDs found. The process will stop here.")
        final_result = "нет результатов, нужна ручная проверка"
    else:
        all_documents = []
        for case_id in case_ids:
            case_info = get_case_info(token, case_id)
            if case_info:
                documents = filter_and_extract_documents(case_info)
                all_documents.extend(documents)
        
        final_result = format_results(all_documents)

    logging.info(f"\n\n========== FINAL RESULT ==========\n{final_result}\n==================================")

    # Dump all collected API responses to a file
    try:
        with open(DEBUG_JSON_DUMP_FILE, 'w', encoding='utf-8') as f:
            json.dump(API_RESPONSES_DUMP, f, ensure_ascii=False, indent=4)
        logging.info(f"Successfully dumped all API responses to '{DEBUG_JSON_DUMP_FILE}'")
    except Exception as e:
        logging.error(f"Failed to dump JSON responses: {e}")

    logging.info("========== DEBUG SCRIPT FINISHED ==========")


if __name__ == "__main__":
    # This check is needed to use pd.to_datetime in format_results
    # without it being a top-level import.
    try:
        import pandas as pd
    except ImportError:
        logging.error("Pandas is required for sorting by date. Please install it: pip install pandas")
        # Create a dummy object to avoid NameError if pandas is not installed
        class DummyPandas:
            def to_datetime(self, *args, **kwargs):
                raise NotImplementedError
        pd = DummyPandas()

    main_debug()