import json
import logging
import os
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import requests
from dotenv import load_dotenv

# --- Debug Configuration ---
# Create a dedicated directory for logs if it doesn't exist
log_dir = Path(__file__).parent / "logs"
log_dir.mkdir(exist_ok=True)
debug_log_file = log_dir / "parser_debug.log"
json_log_file = log_dir / "parser_jsons.log"

# Configure the main logger to write to a file and the console
logging.basicConfig(
    level=logging.DEBUG,  # Set to DEBUG to capture all levels of logs
    format="%(asctime)s - %(levelname)s - [%(funcName)s] - %(message)s",
    handlers=[
        logging.FileHandler(debug_log_file, mode='w', encoding='utf-8'),
        logging.StreamHandler()
    ]
)

# Create a separate logger specifically for dumping JSON data
json_logger = logging.getLogger("json_logger")
json_logger.setLevel(logging.DEBUG)
json_handler = logging.FileHandler(json_log_file, mode='w', encoding='utf-8')
json_formatter = logging.Formatter('%(asctime)s - %(message)s')
json_handler.setFormatter(json_formatter)
json_logger.addHandler(json_handler)
# Prevent json_logger from propagating to the root logger
json_logger.propagate = False

def log_json(title: str, data: Any):
    """Helper function to log JSON data with a title."""
    json_logger.debug(f"--- {title} ---\n{json.dumps(data, ensure_ascii=False, indent=4)}\n")

logging.info("Debugger script started. Detailed logs will be in parser_debug.log and parser_jsons.log")

# --- Load Environment Variables ---
load_dotenv()

# --- Constants (from original script) ---
BASE_URL = "https://api-cloud.ru/api/kad_arbitr.php"
API_TIMEOUT = 120
CACHE_FILE = "_cache_parse_court_links.json"
RESULT_NO_CASES_FOUND = "нет результатов, нужна ручная проверка"
RESULT_NO_SUITABLE_DOCS = "Подходящие документы не найдены"
RESULT_API_ERROR = "API Error during processing"

# --- Type Hinting Aliases (from original script) ---
ApiParams = List[Tuple[str, str]]
JsonDict = Dict[str, Any]
Document = Dict[str, str]
CacheDict = Dict[str, str]

# --- 1. Caching Functions (with added logging) ---

def load_cache(cache_path: Path) -> CacheDict:
    """Loads the API results cache from a JSON file."""
    logging.debug(f"Attempting to load cache from: {cache_path}")
    if not cache_path.exists():
        logging.info("Cache file not found. Starting with an empty cache.")
        return {}
    try:
        with open(cache_path, "r", encoding="utf-8") as f:
            cache = json.load(f)
            logging.info(f"Successfully loaded {len(cache)} items from cache.")
            log_json(f"Loaded Cache from {cache_path}", cache)
            return cache
    except (json.JSONDecodeError, IOError) as e:
        logging.warning(f"Could not read or parse cache file: {e}. Starting fresh.")
        return {}

def save_cache(cache_path: Path, cache: CacheDict):
    """Saves the cache to a JSON file."""
    logging.debug(f"Saving cache with {len(cache)} items to {cache_path}")
    try:
        with open(cache_path, "w", encoding="utf-8") as f:
            json.dump(cache, f, ensure_ascii=False, indent=4)
        log_json(f"Saved Cache to {cache_path}", cache)
    except IOError as e:
        logging.error(f"Could not save cache to {cache_path}: {e}")

# --- 2. Core API Interaction Functions (with added logging) ---

def get_api_token() -> Optional[str]:
    """Retrieves the API token from environment variables."""
    logging.debug("Attempting to retrieve API token from .env file.")
    token = os.getenv("api_cloud")
    if not token:
        logging.error("CRITICAL: 'api_cloud' token not found in .env file. Script cannot proceed.")
    else:
        logging.debug("API token found.")
    return token

def make_api_request(params: ApiParams) -> Optional[JsonDict]:
    """Makes a GET request and logs the full JSON response."""
    logging.debug(f"Making API request to {BASE_URL} with params: {params}")
    try:
        response = requests.get(BASE_URL, params=params, timeout=API_TIMEOUT)
        response.raise_for_status()  # Raise HTTPError for bad responses (4xx or 5xx)
        
        json_response = response.json()
        log_json(f"API Response for type={params[1][1]}", json_response)

        if json_response.get("status") != 200:
            logging.warning(f"API returned non-200 status. Error: {json_response.get('errormsg')}")
            return None
        
        logging.debug("API request successful and status is 200.")
        return json_response
        
    except requests.exceptions.Timeout:
        logging.error(f"API request timed out after {API_TIMEOUT} seconds.")
        return None
    except requests.exceptions.RequestException as e:
        logging.error(f"An error occurred during API request: {e}")
        return None
    except json.JSONDecodeError:
        logging.error("Failed to decode JSON from API response.")
        logging.debug(f"Raw response text: {response.text}")
        return None

def search_cases(token: str, debtor_inn: str, creditor_inn: str) -> Optional[List[str]]:
    """Performs a 'search' API call and logs the process."""
    logging.debug(f"Searching for cases with Debtor INN: {debtor_inn}, Creditor INN: {creditor_inn}")
    params: ApiParams = [
        ("token", token), ("type", "search"), ("CaseType", "G"),
        ("participant", debtor_inn), ("participantType", "1"),
        ("participant", creditor_inn), ("participantType", "0"),
    ]
    response = make_api_request(params)
    if response and response.get("Result"):
        case_ids = [item["caseId"] for item in response["Result"] if "caseId" in item]
        logging.info(f"Found {len(case_ids)} case(s): {case_ids}")
        return case_ids
    logging.warning("No 'Result' field in API response or response was None.")
    return []

def get_case_info(token: str, case_id: str) -> Optional[JsonDict]:
    """Performs a 'caseInfo' API call and logs the process."""
    logging.debug(f"Getting case info for Case ID: {case_id}")
    params: ApiParams = [("token", token), ("type", "caseInfo"), ("CaseId", case_id)]
    response = make_api_request(params)
    if response and response.get("found"):
        logging.debug(f"Case info found for {case_id}.")
        return response.get("Result")
    logging.warning(f"No case info found for {case_id} or API error occurred.")
    return None

# --- 3. Data Processing and Analysis Functions (with added logging) ---

def filter_and_extract_documents(case_info_json: JsonDict) -> List[Document]:
    """Filters events and logs the logic."""
    documents: List[Document] = []
    logging.debug(f"Filtering documents for a case. Full case info JSON is in the json log.")
    log_json("Filtering Case Info", case_info_json)

    if not case_info_json or "CaseInstances" not in case_info_json:
        logging.warning("Case info is empty or missing 'CaseInstances'.")
        return documents

    for i, instance in enumerate(case_info_json.get("CaseInstances", [])):
        logging.debug(f"Processing Instance {i+1}/{len(case_info_json.get('CaseInstances', []))}")
        for j, event in enumerate(instance.get("InstanceEvents", [])):
            event_type = event.get("EventTypeName", "N/A")
            content_types = event.get("ContentTypes", [])
            logging.debug(f"  Event {j+1}: Type='{event_type}', ContentTypes={content_types}")
            
            # Check if the event is a "Решение" (Decision)
            is_decision_event = (event_type == "Решение") or \
                                (event_type == "Решения и постановления" and any(
                                    "решение" in str(ct).lower() for ct in content_types))
            
            if is_decision_event:
                logging.debug(f"    -> Match found: Event type is a decision.")
                if event.get("File") and event.get("Date"):
                    doc = {"Date": event["Date"], "File": event["File"]}
                    documents.append(doc)
                    logging.info(f"      -> Extracted document: {doc}")
                else:
                    logging.warning("    -> Matched event but 'File' or 'Date' is missing.")
            else:
                logging.debug("    -> No match.")
                
    logging.info(f"Found {len(documents)} suitable documents in this case.")
    return documents

def format_results(documents: List[Document]) -> str:
    """Formats results and logs the sorting process."""
    if not documents:
        logging.debug("No documents to format, returning standard message.")
        return RESULT_NO_SUITABLE_DOCS
    
    logging.debug(f"Formatting {len(documents)} documents.")
    try:
        # Sort by date, newest first
        sorted_docs = sorted(
            documents, key=lambda x: pd.to_datetime(x['Date'], format='%d.%m.%Y'), reverse=True
        )
        logging.debug("Successfully sorted documents by date.")
    except (ValueError, KeyError) as e:
        logging.warning(f"Could not sort documents by date due to error: {e}. Using original order.")
        sorted_docs = documents
        
    formatted_string = "\n".join([f"{i+1}. {doc['Date']}: {doc['File']}" for i, doc in enumerate(sorted_docs)])
    logging.debug(f"Formatted result string:\n{formatted_string}")
    return formatted_string

def process_inn_pair(token: str, debtor_inn: str, creditor_inn: str) -> str:
    """Main processing logic with extensive logging."""
    logging.info(f"--- Starting processing for pair: Debtor={debtor_inn}, Creditor={creditor_inn} ---")
    
    case_ids = search_cases(token, debtor_inn, creditor_inn)
    if case_ids is None:
        logging.error("search_cases returned an API error.")
        return RESULT_API_ERROR
    if not case_ids:
        logging.warning("No cases found for this INN pair.")
        return RESULT_NO_CASES_FOUND

    all_documents: List[Document] = []
    for case_id in case_ids:
        logging.info(f"Processing Case ID: {case_id}")
        case_info = get_case_info(token, case_id)
        if case_info:
            documents = filter_and_extract_documents(case_info)
            if documents:
                all_documents.extend(documents)
                logging.info(f"Added {len(documents)} documents from case {case_id}.")
        else:
            logging.warning(f"Skipping document extraction for case {case_id} as no info was returned.")
            
    logging.info(f"Total documents found for INN pair: {len(all_documents)}")
    return format_results(all_documents)

# --- 4. Main Execution Block (from original script, with added logging) ---

def main():
    """Main function to run the entire script."""
    logging.info("="*50)
    logging.info("Main execution block started.")
    token = get_api_token()
    if not token:
        return

    script_dir = Path(__file__).parent
    input_file = script_dir / "testdata17.04.25.csv"
    output_file = script_dir / "output_with_links_DEBUG.csv"
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
        df = df.head(6)
        logging.info(f"Loaded {len(df)} rows from {input_file}.")
        
        if debtor_col not in df.columns or creditor_col not in df.columns:
            logging.error(f"CSV must contain '{debtor_col}' and '{creditor_col}'.")
            return

        cache = load_cache(cache_file)

        df['cache_key'] = df[debtor_col] + '|' + df[creditor_col]
        unique_keys = df['cache_key'].unique()
        logging.info(f"Found {len(unique_keys)} unique INN pairs to process: {unique_keys}")

        keys_to_fetch = [key for key in unique_keys if key not in cache]
        logging.info(f"{len(keys_to_fetch)} pairs are new and will be fetched from the API: {keys_to_fetch}")
        
        if not keys_to_fetch:
            logging.info("All required data is already in the cache. No API calls needed.")

        for i, key in enumerate(keys_to_fetch):
            logging.info(f"--- Fetching API data for new pair {i+1}/{len(keys_to_fetch)}: {key} ---")
            debtor_inn, creditor_inn = key.split('|')
            
            result = process_inn_pair(token, debtor_inn, creditor_inn)
            
            cache[key] = result
            save_cache(cache_file, cache)
        
        logging.info("Mapping cached results back to the DataFrame.")
        df[output_col] = df['cache_key'].map(cache)
        
        df.drop(columns=['cache_key'], inplace=True)
        df.to_csv(output_file, index=False, encoding='utf-8-sig')
        logging.info(f"Processing complete. Results saved to {output_file}")
        logging.info(f"Debug logs are in: {debug_log_file}")
        logging.info(f"JSON dumps are in: {json_log_file}")

    except Exception as e:
        logging.error(f"An unexpected error occurred in main: {e}", exc_info=True)

if __name__ == "__main__":
    main()
