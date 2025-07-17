# File: parse_court_links.py

import json
import logging
import os
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import requests
from dotenv import load_dotenv

# --- Configuration ---
# Configure logging to output to both console and a file
log_file_path = "parse_court_links.log"
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(log_file_path, mode='w', encoding='utf-8'),
        logging.StreamHandler()
    ]
)

# Load environment variables from .env file
load_dotenv()

# --- Constants ---
BASE_URL = "https://api-cloud.ru/api/kad_arbitr.php"
API_TIMEOUT = 120  # Seconds, as recommended by the API documentation
CACHE_FILE = "_cache_parse_court_links.json"

# Result messages based on your instructions
RESULT_NO_CASES_FOUND = "нет результатов, нужна ручная проверка"
RESULT_NO_SUITABLE_DOCS = "Подходящие документы не найдены"
RESULT_API_ERROR = "API Error during processing"

# --- Type Hinting Aliases ---
ApiParams = List[Tuple[str, str]]
JsonDict = Dict[str, Any]
Document = Dict[str, str]
CacheDict = Dict[str, str]


# --- 1. Caching Functions ---

def load_cache(cache_path: Path) -> CacheDict:
    """Loads the API results cache from a JSON file."""
    if not cache_path.exists():
        logging.info("Cache file not found. Starting with an empty cache.")
        return {}
    try:
        with open(cache_path, "r", encoding="utf-8") as f:
            cache = json.load(f)
            logging.info(f"Successfully loaded {len(cache)} items from cache.")
            return cache
    except (json.JSONDecodeError, IOError) as e:
        logging.warning(f"Could not read or parse cache file: {e}. Starting fresh.")
        return {}


def save_cache(cache_path: Path, cache: CacheDict):
    """Saves the cache to a JSON file."""
    try:
        with open(cache_path, "w", encoding="utf-8") as f:
            json.dump(cache, f, ensure_ascii=False, indent=4)
    except IOError as e:
        logging.error(f"Could not save cache to {cache_path}: {e}")


# --- 2. Core API Interaction Functions ---

def get_api_token() -> Optional[str]:
    """Retrieves the API token from environment variables."""
    token = os.getenv("api_cloud")
    if not token:
        logging.error("FATAL: 'api_cloud' token not found in .env file. Please create it.")
    return token


def make_api_request(params: ApiParams) -> Optional[JsonDict]:
    """Makes a GET request to the API Cloud endpoint with error handling."""
    try:
        response = requests.get(BASE_URL, params=params, timeout=API_TIMEOUT)
        response.raise_for_status()  # Raises HTTPError for bad responses (4xx or 5xx)
        json_response = response.json()
        if json_response.get("status") != 200:
            error_msg = json_response.get('errormsg', 'Unknown API error')
            logging.warning(f"API returned non-200 status: {error_msg}")
            return None
        return json_response
    except requests.exceptions.RequestException as e:
        logging.error(f"API request failed: {e}")
        return None


def search_cases(token: str, debtor_inn: str, creditor_inn: str) -> Optional[List[str]]:
    """Performs a 'search' API call to find case IDs based on your rules."""
    logging.info(f"Searching for cases with Debtor(Resp): {debtor_inn}, Creditor(Plaint): {creditor_inn}")
    params: ApiParams = [
        ("token", token),
        ("type", "search"),
        ("CaseType", "G"),  # [G] - только гражданские дела
        ("participant", debtor_inn),
        ("participantType", "1"),  # [1] - ответчик
        ("participant", creditor_inn),
        ("participantType", "0"),  # [0] - истец
    ]
    response = make_api_request(params)
    if response and response.get("Result"):
        case_ids = [item["caseId"] for item in response["Result"] if "caseId" in item]
        logging.info(f"Found {len(case_ids)} potential case(s).")
        return case_ids
    logging.info("API search returned no matching cases.")
    return []


def get_case_info(token: str, case_id: str) -> Optional[JsonDict]:
    """Performs a 'caseInfo' API call to get detailed case data."""
    logging.info(f"Fetching details for CaseId: {case_id}")
    params: ApiParams = [("token", token), ("type", "caseInfo"), ("CaseId", case_id)]
    return make_api_request(params)


# --- 3. Data Processing and Analysis Functions ---

def filter_and_extract_documents(case_info_json: JsonDict, debtor_inn: str, creditor_inn: str) -> List[Document]:
    """
    Filters events within a case to find specific court decisions, after validating
    that the debtor and creditor INNs match the case participants' specific roles.
    """
    if not case_info_json or "Result" not in case_info_json:
        logging.warning("Case info JSON is empty or malformed. Skipping.")
        return []

    # --- 1. Validate Participant Roles ---
    participants = case_info_json.get("Result", {}).get("Participants", {})
    plaintiff_inns = {p.get("INN") for p in participants.get("Plaintiffs", []) if p.get("INN")}
    respondent_inns = {r.get("INN") for r in participants.get("Respondents", []) if r.get("INN")}

    if not (creditor_inn in plaintiff_inns and debtor_inn in respondent_inns):
        case_id = case_info_json.get("Result", {}).get("CaseInfo", {}).get("CaseId", "N/A")
        logging.warning(
            f"INN role mismatch for CaseId {case_id}. "
            f"Expected Debtor(Respondent): {debtor_inn}, Creditor(Plaintiff): {creditor_inn}. "
            f"Found Respondents: {respondent_inns}, Plaintiffs: {plaintiff_inns}. Skipping this case."
        )
        return []

    # --- 2. Extract Documents based on your specific rules ---
    documents: List[Document] = []
    case_instances = case_info_json.get("Result", {}).get("CaseInstances", [])

    for instance in case_instances:
        for event in instance.get("InstanceEvents", []):
            event_type = event.get("EventTypeName", "")
            content_types = event.get("ContentTypes", [])
            
            # Rule 1: EventTypeName is "Решение"
            is_direct_decision = (event_type == "Решение")
            
            # Rule 2: EventTypeName is "Решения и постановления" AND ContentTypes contains "решение"
            is_filtered_decision = (
                event_type == "Решения и постановления" and
                any("решение" in str(ct).lower() for ct in content_types)
            )

            if (is_direct_decision or is_filtered_decision) and event.get("File") and event.get("Date"):
                doc = {"Date": event["Date"], "File": event["File"]}
                documents.append(doc)
                logging.info(f"  -> Found matching document: {doc['Date']}")

    return documents


def format_results(documents: List[Document]) -> str:
    """Formats the list of found documents into a sorted, numbered string."""
    if not documents:
        return RESULT_NO_SUITABLE_DOCS
    try:
        # Sort documents by date, newest first
        sorted_docs = sorted(
            documents, key=lambda x: pd.to_datetime(x['Date'], format='%d.%m.%Y', errors='coerce'), reverse=True
        )
    except (ValueError, KeyError) as e:
        logging.warning(f"Could not sort documents by date due to format error: {e}. Using original order.")
        sorted_docs = documents
        
    return "\n".join([f"{i+1}. {doc['Date']}: {doc['File']}" for i, doc in enumerate(sorted_docs)])


def process_inn_pair(token: str, debtor_inn: str, creditor_inn: str) -> str:
    """Main processing logic for a single unique INN pair, aggregating all results."""
    case_ids = search_cases(token, debtor_inn, creditor_inn)
    if case_ids is None:  # Indicates a fatal API error during search
        return RESULT_API_ERROR
    if not case_ids:
        return RESULT_NO_CASES_FOUND

    all_documents: List[Document] = []
    for case_id in case_ids:
        case_info = get_case_info(token, case_id)
        if case_info:
            # The filter function already validates roles, so we just extend the list
            documents = filter_and_extract_documents(case_info, debtor_inn, creditor_inn)
            all_documents.extend(documents)
        else:
            logging.warning(f"Failed to retrieve or parse caseInfo for CaseId: {case_id}")
            
    return format_results(all_documents)


# --- 4. Main Execution Block ---

def main():
    """Main function to run the entire script."""
    logging.info("--- Script Execution Started ---")
    token = get_api_token()
    if not token:
        return

    script_dir = Path(__file__).parent
    input_file = script_dir / "testdata17.04.25.csv"
    output_file = script_dir / "output_with_links.csv"
    cache_file = script_dir / CACHE_FILE
    
    debtor_col = "debtor_inn"
    creditor_col = "creditor_inn"
    output_col = "court_decision_links"

    if not input_file.exists():
        logging.error(f"Input file not found at: {input_file}")
        return

    try:
        df = pd.read_csv(input_file, dtype=str).fillna('')
        df = df[[debtor_col, creditor_col, 'number']]
        df = df.head(6)  # For testing, load only the first 50 rows
        logging.info(f"Loaded {len(df)} rows from {input_file}.")
        
        if debtor_col not in df.columns or creditor_col not in df.columns:
            logging.error(f"CSV must contain '{debtor_col}' and '{creditor_col}' columns.")
            return

        cache = load_cache(cache_file)

        # --- OPTIMIZATION: Process only unique INN pairs ---
        df['cache_key'] = df[debtor_col] + '|' + df[creditor_col]
        unique_keys = df['cache_key'].unique()
        logging.info(f"Found {len(unique_keys)} unique INN pairs to process.")

        keys_to_fetch = [key for key in unique_keys if key not in cache]
        logging.info(f"{len(keys_to_fetch)} pairs are new and will be fetched from the API.")

        for i, key in enumerate(keys_to_fetch):
            logging.info(f"--- Processing new pair {i+1}/{len(keys_to_fetch)}: {key} ---")
            debtor_inn, creditor_inn = key.split('|')
            
            if not debtor_inn or not creditor_inn:
                logging.warning(f"Skipping invalid pair with empty INN: {key}")
                result = "Invalid INN provided"
            else:
                result = process_inn_pair(token, debtor_inn, creditor_inn)
            
            cache[key] = result
            save_cache(cache_file, cache) # Save progress immediately
        
        logging.info("All unique pairs processed. Mapping results back to the DataFrame.")
        df[output_col] = df['cache_key'].map(cache)
        
        df.drop(columns=['cache_key'], inplace=True)
        df.to_csv(output_file, index=False, encoding='utf-8-sig')
        logging.info(f"--- Script Execution Finished. Results saved to {output_file} ---")

    except Exception as e:
        logging.error(f"An unexpected error occurred during file processing: {e}", exc_info=True)


if __name__ == "__main__":
    main()