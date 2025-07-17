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
# save to file
file_handler = logging.FileHandler("parse_court_links.log")
file_handler.setLevel(logging.INFO)
file_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
logging.getLogger().addHandler(file_handler)



# Load environment variables from .env file
load_dotenv()

# --- Constants ---
BASE_URL = "https://api-cloud.ru/api/kad_arbitr.php"
API_TIMEOUT = 120  # As recommended by the API documentation
CACHE_FILE = "_cache_parse_court_links.json"

# Result messages
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


# --- 2. Core API Interaction Functions (Unchanged) ---

def get_api_token() -> Optional[str]:
    """Retrieves the API token from environment variables."""
    token = os.getenv("api_cloud")
    if not token:
        logging.error("api_cloud not found in .env file.")
    return token


def make_api_request(params: ApiParams) -> Optional[JsonDict]:
    """Makes a GET request to the API Cloud endpoint."""
    try:
        response = requests.get(BASE_URL, params=params, timeout=API_TIMEOUT)
        response.raise_for_status()
        json_response = response.json()
        if json_response.get("status") != 200:
            logging.warning(f"API returned non-200 status: {json_response.get('errormsg')}")
            return None
        return json_response
    except requests.exceptions.RequestException as e:
        logging.error(f"API request failed: {e}")
        return None


def search_cases(token: str, debtor_inn: str, creditor_inn: str) -> Optional[List[str]]:
    """Performs a 'search' API call to find case IDs."""
    params: ApiParams = [
        ("token", token), ("type", "search"), ("CaseType", "G"),
        ("participant", debtor_inn), ("participantType", "1"),
        ("participant", creditor_inn), ("participantType", "0"),
    ]
    response = make_api_request(params)
    if response and response.get("Result"):
        return [item["caseId"] for item in response["Result"] if "caseId" in item]
    return []


def get_case_info(token: str, case_id: str) -> Optional[JsonDict]:
    """Performs a 'caseInfo' API call."""
    params: ApiParams = [("token", token), ("type", "caseInfo"), ("CaseId", case_id)]
    response = make_api_request(params)
    # logging.info(f"Fetched case info for CaseId {case_id}: {response}")
    return response.get("Result") if response and response.get("found") else None


# --- 3. Data Processing and Analysis Functions (Unchanged) ---

def filter_and_extract_documents(case_info_json: JsonDict, debtor_inn: str, creditor_inn: str) -> List[Document]:
    """
    Filters events within a case to find specific court decisions, AFTER validating
    that the debtor and creditor INNs match the case participants' specific roles.
    """
    # --- Your INN Validation Block (which is correct for your needs) ---
    if not case_info_json or "Result" not in case_info_json:
        return []

    participants = case_info_json.get("Result", {}).get("Participants", {})
    
    # Create sets of all plaintiff and respondent INNs found in the case
    plaintiff_inns = {p.get("INN") for p in participants.get("Plaintiffs", []) if p.get("INN")}
    respondent_inns = {r.get("INN") for r in participants.get("Respondents", []) if r.get("INN")}

    # Check if our specific creditor is among the plaintiffs AND our debtor is among the respondents
    is_creditor_plaintiff = creditor_inn in plaintiff_inns
    is_debtor_respondent = debtor_inn in respondent_inns

    if not (is_creditor_plaintiff and is_debtor_respondent):
        case_id = case_info_json.get("Result", {}).get("CaseInfo", {}).get("CaseId", "N/A")
        logging.warning(
            f"INN role mismatch for CaseId {case_id}. "
            f"Expected Debtor(Respondent): {debtor_inn}, Creditor(Plaintiff): {creditor_inn}. "
            f"Found Respondents: {respondent_inns}, Plaintiffs: {plaintiff_inns}. Skipping case."
        )
        return [] # Return empty list if the roles do not match

    # --- Corrected Document Extraction Logic ---
    documents: List[Document] = []
    
    # THIS IS THE LINE THAT WAS FIXED:
    # We now correctly look inside the "Result" object to find "CaseInstances"
    for instance in case_info_json.get("Result", {}).get("CaseInstances", []):
        for event in instance.get("InstanceEvents", []):
            event_type = event.get("EventTypeName", "")
            content_types = event.get("ContentTypes", [])
            
            is_decision_event = (event_type == "Решение") or \
                                (event_type == "Решения и постановления" and any(
                                    "решение" in str(ct).lower() for ct in content_types))
            
            if is_decision_event and event.get("File") and event.get("Date"):
                documents.append({"Date": event["Date"], "File": event["File"]})
    
    return documents


def format_results(documents: List[Document]) -> str:
    """Formats the list of found documents into a numbered string."""
    if not documents:
        return RESULT_NO_SUITABLE_DOCS
    try:
        sorted_docs = sorted(
            documents, key=lambda x: pd.to_datetime(x['Date'], format='%d.%m.%Y'), reverse=True
        )
    except (ValueError, KeyError):
        sorted_docs = documents
    return "\n".join([f"{i+1}. {doc['Date']}: {doc['File']}" for i, doc in enumerate(sorted_docs)])


def process_inn_pair(token: str, debtor_inn: str, creditor_inn: str) -> str:
    """Main processing logic for a single unique INN pair."""
    case_ids = search_cases(token, debtor_inn, creditor_inn)
    if case_ids is None:
        return RESULT_API_ERROR
    if not case_ids:
        return RESULT_NO_CASES_FOUND

    all_documents: List[Document] = []
    for case_id in case_ids:
        case_info = get_case_info(token, case_id)
        if case_info:
            documents = filter_and_extract_documents(case_info, debtor_inn, creditor_inn)
            all_documents.extend(documents)
    return format_results(all_documents)


# --- 4. Main Execution Block (Optimized for Duplicates) ---

def main():
    """Main function to run the entire script."""
    logging.info("Script started.")
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
            logging.error(f"CSV must contain '{debtor_col}' and '{creditor_col}'.")
            return

        cache = load_cache(cache_file)

        # --- OPTIMIZATION LOGIC ---
        # 1. Create a unique key for each row based on the INN pair.
        df['cache_key'] = df[debtor_col] + '|' + df[creditor_col]
        
        # 2. Identify the unique set of keys that need processing.
        unique_keys = df['cache_key'].unique()
        logging.info(f"Found {len(unique_keys)} unique INN pairs to process.")

        # 3. Determine which of these unique keys are not already in the cache.
        keys_to_fetch = [key for key in unique_keys if key not in cache]
        logging.info(f"{len(keys_to_fetch)} pairs are new and will be fetched from the API.")

        # 4. Process only the new keys and update the cache.
        for i, key in enumerate(keys_to_fetch):
            logging.info(f"--- Fetching API data for new pair {i+1}/{len(keys_to_fetch)}: {key} ---")
            debtor_inn, creditor_inn = key.split('|')
            
            result = process_inn_pair(token, debtor_inn, creditor_inn)
            
            cache[key] = result  # Add the new result to the in-memory cache
            save_cache(cache_file, cache) # Save progress to disk immediately
        
        # 5. Map the results from the now-complete cache back to the original DataFrame.
        # This is much faster than iterating row-by-row.
        logging.info("Mapping cached results back to the DataFrame.")
        df[output_col] = df['cache_key'].map(cache)
        
        # 6. Clean up and save.
        df.drop(columns=['cache_key'], inplace=True)
        df.to_csv(output_file, index=False, encoding='utf-8-sig')
        logging.info(f"Processing complete. Results saved to {output_file}")

    except Exception as e:
        logging.error(f"An unexpected error occurred during file processing: {e}", exc_info=True)


if __name__ == "__main__":
    main()