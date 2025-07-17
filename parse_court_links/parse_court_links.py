import json
import logging
import os
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import requests
from dotenv import load_dotenv

# --- Configuration ---
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

# Load environment variables from .env file
load_dotenv()

# --- Constants ---
BASE_URL = "https://api-cloud.ru/api/kad_arbitr.php"
API_TIMEOUT = 120  # As recommended by the API documentation
CACHE_FILE = "_cache_parse_court_links.json"

# Result messages from the instruction PDF
RESULT_NO_CASES_FOUND = "нет результатов, нужна ручная проверка"
RESULT_NO_SUITABLE_DOCS = "Подходящие документы не найдены"

# --- Type Hinting Aliases for Clarity ---
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
    token = os.getenv("API_CLOUD_TOKEN")
    if not token:
        logging.error(
            "API_CLOUD_TOKEN not found in .env file. Please create a .env file "
            "with 'API_CLOUD_TOKEN=your_key_here'."
        )
    return token


def make_api_request(params: ApiParams) -> Optional[JsonDict]:
    """Makes a GET request to the API Cloud endpoint."""
    try:
        response = requests.get(BASE_URL, params=params, timeout=API_TIMEOUT)
        response.raise_for_status()
        json_response = response.json()
        if json_response.get("status") != 200:
            error_msg = json_response.get("errormsg", "Unknown API error")
            logging.warning(f"API returned non-200 status: {error_msg}")
            return None
        return json_response
    except requests.exceptions.RequestException as e:
        logging.error(f"API request failed: {e}")
        return None


def search_cases(
    token: str, debtor_inn: str, creditor_inn: str
) -> Optional[List[str]]:
    """Performs a 'search' API call to find case IDs."""
    params: ApiParams = [
        ("token", token),
        ("type", "search"),
        ("CaseType", "G"),
        ("participant", debtor_inn),
        ("participantType", "1"),
        ("participant", creditor_inn),
        ("participantType", "0"),
    ]
    logging.info(f"Searching cases for debtor {debtor_inn} and creditor {creditor_inn}")
    response = make_api_request(params)
    if response and response.get("Result"):
        case_ids = [item["caseId"] for item in response["Result"] if "caseId" in item]
        logging.info(f"Found {len(case_ids)} case(s).")
        return case_ids
    logging.info("No cases found in initial search.")
    return []


def get_case_info(token: str, case_id: str) -> Optional[JsonDict]:
    """Performs a 'caseInfo' API call."""
    params: ApiParams = [
        ("token", token),
        ("type", "caseInfo"),
        ("CaseId", case_id),
    ]
    logging.info(f"Fetching case info for CaseId: {case_id}")
    response = make_api_request(params)
    return response.get("Result") if response and response.get("found") else None


# --- 3. Data Processing and Analysis Functions ---

def filter_and_extract_documents(case_info_json: JsonDict) -> List[Document]:
    """Filters events within a case to find specific court decisions."""
    documents: List[Document] = []
    if not case_info_json or "CaseInstances" not in case_info_json:
        return documents

    for instance in case_info_json.get("CaseInstances", []):
        for event in instance.get("InstanceEvents", []):
            event_type = event.get("EventTypeName", "")
            content_types = event.get("ContentTypes", [])
            is_decision_event = False
            if event_type == "Решение":
                is_decision_event = True
            elif event_type == "Решения и постановления" and any(
                "решение" in str(ct).lower() for ct in content_types
            ):
                is_decision_event = True
            
            if is_decision_event and event.get("File") and event.get("Date"):
                documents.append({"Date": event["Date"], "File": event["File"]})
    return documents


def format_results(documents: List[Document]) -> str:
    """Formats the list of found documents into a numbered string."""
    if not documents:
        return RESULT_NO_SUITABLE_DOCS
    try:
        sorted_docs = sorted(
            documents,
            key=lambda x: pd.to_datetime(x['Date'], format='%d.%m.%Y'),
            reverse=True,
        )
    except (ValueError, KeyError):
        logging.warning("Could not sort documents by date, using original order.")
        sorted_docs = documents
    return "\n".join(
        [f"{i+1}. {doc['Date']}: {doc['File']}" for i, doc in enumerate(sorted_docs)]
    )


def process_row_logic(token: str, debtor_inn: str, creditor_inn: str) -> str:
    """
    Main processing logic for a single row.
    Orchestrates the workflow: search -> get info -> filter -> format.
    """
    case_ids = search_cases(token, debtor_inn, creditor_inn)
    if case_ids is None:
        return "API Error during search"
    if not case_ids:
        return RESULT_NO_CASES_FOUND

    all_documents: List[Document] = []
    for case_id in case_ids:
        case_info = get_case_info(token, case_id)
        if case_info:
            documents = filter_and_extract_documents(case_info)
            all_documents.extend(documents)
    return format_results(all_documents)


# --- 4. Main Execution Block ---

def main():
    """Main function to run the entire script."""
    logging.info("Script started.")
    token = get_api_token()
    if not token:
        return

    script_dir = Path(__file__).parent
    input_file = script_dir / "input.csv"
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
        logging.info(f"Loaded {len(df)} rows from {input_file}.")
        
        if debtor_col not in df.columns or creditor_col not in df.columns:
            logging.error(f"CSV must contain '{debtor_col}' and '{creditor_col}'.")
            return

        cache = load_cache(cache_file)
        results_list = []

        for index, row in df.iterrows():
            debtor_inn = row[debtor_col]
            creditor_inn = row[creditor_col]
            cache_key = f"{debtor_inn}|{creditor_inn}"
            
            logging.info(f"--- Processing row {index+1}/{len(df)} ---")

            if cache_key in cache:
                logging.info(f"Found result for '{cache_key}' in cache. Skipping API call.")
                result = cache[cache_key]
            else:
                logging.info(f"No cache entry for '{cache_key}'. Fetching from API.")
                result = process_row_logic(token, debtor_inn, creditor_inn)
                cache[cache_key] = result
                save_cache(cache_file, cache) # Save progress immediately
                logging.info(f"Result for '{cache_key}' saved to cache.")
            
            results_list.append(result)
        
        df[output_col] = results_list
        df.to_csv(output_file, index=False, encoding='utf-8-sig')
        logging.info(f"Processing complete. Results saved to {output_file}")

    except Exception as e:
        logging.error(f"An unexpected error occurred during file processing: {e}")


if __name__ == "__main__":
    main()