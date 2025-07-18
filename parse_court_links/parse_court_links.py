# File: sourt_links.py

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
log_file_path = "parse_court_links170725.log"
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
LOW_BALANCE_THRESHOLD = 100.0 # Stop processing if balance is at or below this value

# Result messages based on your instructions
RESULT_NO_CASES_FOUND = "Нет результатов, нужна ручная проверка"
RESULT_NO_SUITABLE_DOCS = "Документы с решениями не найдены"
RESULT_API_ERROR = "API Error during processing"
RESULT_API_RETRY_ERROR = "Сетевая ошибка, требуется повторная попытка"


# --- Custom Exception for Balance Control ---
class LowBalanceError(Exception):
    """Custom exception raised when API balance is too low."""
    pass


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


def make_api_request(params: ApiParams) -> Tuple[Optional[JsonDict], bool]:
    """
    Makes a GET request to the API Cloud endpoint.
    Returns a tuple: (response_json, is_retryable_error)
    """
    try:
        response = requests.get(BASE_URL, params=params, timeout=API_TIMEOUT)
        response.raise_for_status()  # Raises HTTPError for bad responses (4xx or 5xx)
        json_response = response.json()

        # --- Balance Check START ---
        if "inquiry" in json_response and "balance" in json_response["inquiry"]:
            try:
                current_balance = float(json_response["inquiry"]["balance"])
                logging.info(f"API Balance updated: {current_balance:.2f}")
                if current_balance <= LOW_BALANCE_THRESHOLD:
                    logging.warning(f"API balance is {current_balance:.2f}, which is at or below the threshold of {LOW_BALANCE_THRESHOLD}. Stopping.")
                    raise LowBalanceError("API balance is too low.")
            except (ValueError, TypeError):
                logging.warning("Could not parse balance from API response.")
        # --- Balance Check END ---

        if json_response.get("status") != 200:
            error_msg = json_response.get('errormsg', 'Unknown API error')
            logging.warning(f"API returned non-200 status: {error_msg}")
            return None, False

        return json_response, False

    except requests.exceptions.RequestException as e:
        logging.error(f"API request failed with a network error: {e}")
        return None, True

def search_cases(token: str, debtor_inn: str, creditor_inn: str) -> Tuple[Optional[List[JsonDict]], bool]:
    """
    Performs a 'search' API call and returns the full list of case result objects.
    Returns a tuple: (list_of_case_objects, is_retryable_error)
    """
    logging.info(f"Searching for cases with Debtor(Resp): {debtor_inn}, Creditor(Plaint): {creditor_inn}")
    
    base_params: ApiParams = [
        ("token", token), ("type", "search"), ("CaseType", "G"),
        ("participant", debtor_inn), ("participantType", "1"),
        ("participant", creditor_inn), ("participantType", "0"),
    ]
    
    all_case_objects = []
    page_num = 1
    total_pages = 1 

    while page_num <= total_pages:
        logging.info(f"  -> Fetching page {page_num}/{total_pages}...")
        
        paged_params = base_params + [("page", str(page_num))]
        response, is_retryable = make_api_request(paged_params)
        
        if is_retryable:
            logging.error(f"Network error while fetching page {page_num}. Aborting this INN pair and marking for retry.")
            return None, True
        
        if response and response.get("Result"):
            all_case_objects.extend(response.get("Result", []))
            
            try:
                current_pages_count = int(response.get("PagesCount", 1))
                if current_pages_count > total_pages:
                    logging.info(f"API updated total pages from {total_pages} to {current_pages_count}.")
                    total_pages = current_pages_count
            except (ValueError, TypeError):
                logging.warning(f"Could not parse 'PagesCount' from API response: {response.get('PagesCount')}. Pagination may be incomplete.")
        
        else:
            logging.warning(f"Page {page_num} returned no 'Result' data or a non-200 status. Stopping pagination for this search.")
            break

        page_num += 1

    if not all_case_objects:
         logging.info("API search returned no matching cases.")
         return [], False

    logging.info(f"Finished search. Total raw case results found: {len(all_case_objects)}.")
    return all_case_objects, False


def filter_search_results_by_role(search_results: List[JsonDict], debtor_inn: str, creditor_inn: str) -> List[str]:
    """
    Pre-filters the raw results from a 'search' API call.
    """
    validated_case_ids = []
    for case in search_results:
        plaintiff_inns = {str(p.get("inn", "")).strip() for p in case.get("plaintiff", []) if p is not None and p.get("inn") is not None}
        respondent_inns = {str(r.get("inn", "")).strip() for r in case.get("respondent", []) if r is not None and r.get("inn") is not None}

        if creditor_inn in plaintiff_inns and debtor_inn in respondent_inns:
            if "caseId" in case:
                validated_case_ids.append(case["caseId"])
        
    logging.info(
        f"Pre-filtering complete. "
        f"Reduced {len(search_results)} raw results to {len(validated_case_ids)} cases with correct participant roles."
    )
    return validated_case_ids



def get_case_info(token: str, case_id: str) -> Tuple[Optional[JsonDict], bool]:
    """
    Performs a 'caseInfo' API call.
    Returns a tuple: (case_info_json, is_retryable_error)
    """
    logging.info(f"Fetching details for CaseId: {case_id}")
    params: ApiParams = [("token", token), ("type", "caseInfo"), ("CaseId", case_id)]
    
    return make_api_request(params)


# --- 3. Data Processing and Analysis Functions ---

def filter_and_extract_documents(case_info_json: JsonDict, debtor_inn: str, creditor_inn: str) -> List[Document]:
    """
    Filters events within a case to find specific court decisions.
    """
    if not case_info_json or "Result" not in case_info_json:
        logging.warning("Case info JSON is empty or malformed. Skipping.")
        return []

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

    documents: List[Document] = []
    case_instances = case_info_json.get("Result", {}).get("CaseInstances", [])

    for instance in case_instances:
        for event in instance.get("InstanceEvents", []):
            event_type = event.get("EventTypeName", "")
            content_types = event.get("ContentTypes", [])
            
            is_direct_decision = (event_type == "Решение")
            is_direct_decisions = (event_type == "Решения")
            
            is_filtered_decision = (
                event_type == "Решения и постановления" and
                any("решени" in str(ct).lower() for ct in content_types)
            )

            if (is_direct_decision or is_direct_decisions or is_filtered_decision) and event.get("File") and event.get("Date"):
                doc = {"Date": event["Date"], "File": event["File"]}
                documents.append(doc)
                logging.info(f"  -> Found matching document: {doc['Date']}")

    return documents

def format_results(documents: List[Document]) -> str:
    """Formats the list of found documents into a sorted, numbered string."""
    if not documents:
        return RESULT_NO_SUITABLE_DOCS
    try:
        sorted_docs = sorted(
            documents, key=lambda x: pd.to_datetime(x['Date'], format='%d.%m.%Y', errors='coerce'), reverse=True
        )
    except (ValueError, KeyError) as e:
        logging.warning(f"Could not sort documents by date due to format error: {e}. Using original order.")
        sorted_docs = documents
        
    return "\n".join([f"{doc['Date']}: {doc['File']}" for i, doc in enumerate(sorted_docs)])



def process_inn_pair(token: str, debtor_inn: str, creditor_inn: str) -> str:
    """Main processing logic for a single unique INN pair, with robust error handling."""
    raw_search_results, is_retryable = search_cases(token, debtor_inn, creditor_inn)
    
    if is_retryable:
        return RESULT_API_RETRY_ERROR
        
    if raw_search_results is None:
        return RESULT_API_ERROR
    if not raw_search_results:
        return RESULT_NO_CASES_FOUND

    validated_case_ids = filter_search_results_by_role(raw_search_results, debtor_inn, creditor_inn)

    if not validated_case_ids:
        return RESULT_NO_SUITABLE_DOCS

    all_documents: List[Document] = []
    for case_id in validated_case_ids:
        case_info, is_retryable = get_case_info(token, case_id)
        
        if is_retryable:
            logging.warning(f"Network error while fetching CaseId {case_id}. Marking entire pair for retry.")
            return RESULT_API_RETRY_ERROR
            
        if case_info:
            documents = filter_and_extract_documents(case_info, debtor_inn, creditor_inn)
            all_documents.extend(documents)
        else:
            logging.warning(f"Failed to retrieve or parse caseInfo for CaseId: {case_id} (non-retryable error).")
            
    return format_results(all_documents)


# --- 4. Main Execution Block ---

def main():
    """Main function to run the entire script."""
    logging.info("--- Script Execution Started ---")
    token = get_api_token()
    if not token:
        return

    script_dir = Path(__file__).parent
    input_file = script_dir / "filtered_regions_descending_groupsum.csv"
    output_file = script_dir / "filtered_reigons_with_links.csv"
    cache_file = script_dir / "cache_filtered_reigons_with_links.json"
    
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
            logging.error(f"CSV must contain '{debtor_col}' and '{creditor_col}' columns.")
            return

        cache = load_cache(cache_file)

        df['cache_key'] = df[debtor_col] + '|' + df[creditor_col]
        unique_keys = df['cache_key'].unique()
        logging.info(f"Found {len(unique_keys)} unique INN pairs to process.")

        keys_to_fetch = [key for key in unique_keys if key not in cache]
        logging.info(f"{len(keys_to_fetch)} pairs are new and will be fetched from the API.")

        # --- MODIFIED Main Loop with Balance Check ---
        try:
            for i, key in enumerate(keys_to_fetch):
                logging.info(f"--- Processing new pair {i+1}/{len(keys_to_fetch)}: {key} ---")
                debtor_inn, creditor_inn = key.split('|')
                
                if not debtor_inn or not creditor_inn:
                    logging.warning(f"Skipping invalid pair with empty INN: {key}")
                    result = "Invalid INN provided"
                else:
                    result = process_inn_pair(token, debtor_inn, creditor_inn)
                
                if result != RESULT_API_RETRY_ERROR:
                    cache[key] = result
                    save_cache(cache_file, cache)
                else:
                    logging.warning(f"Pair {key} resulted in a network error. It will NOT be cached and will be retried on the next run.")
        
        except LowBalanceError:
            logging.warning("Low API balance detected. Halting processing and proceeding to save partial results.")
        # --- End of Modified Loop ---
        
        logging.info("Mapping results back to the DataFrame.")
        df[output_col] = df['cache_key'].map(cache)
        df[output_col].fillna(RESULT_API_RETRY_ERROR, inplace=True)
        
        df.drop(columns=['cache_key'], inplace=True)
        df.to_csv(output_file, index=False, encoding='utf-8-sig')
        logging.info(f"--- Script Execution Finished. Results saved to {output_file} ---")

    except Exception as e:
        logging.error(f"An unexpected error occurred during file processing: {e}", exc_info=True)

if __name__ == "__main__":
    main()