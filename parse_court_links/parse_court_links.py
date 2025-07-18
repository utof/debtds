# File: court_links.py

import json
import logging
import os
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Set

import pandas as pd
import requests
from dotenv import load_dotenv

# --- Configuration ---
# Configure logging to output to both console and a file

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
CacheDict = Dict[str, Any] # Modified to handle more complex cache structures


# --- 1. Caching Functions ---

def load_cache(cache_path: Path) -> CacheDict:
    """Loads the API results cache from a JSON file."""
    if not cache_path.exists():
        logging.info(f"Cache file {cache_path.name} not found. Starting with an empty cache.")
        return {}
    try:
        with open(cache_path, "r", encoding="utf-8") as f:
            cache = json.load(f)
            logging.info(f"Successfully loaded {len(cache)} items from {cache_path.name}.")
            return cache
    except (json.JSONDecodeError, IOError) as e:
        logging.warning(f"Could not read or parse cache file {cache_path.name}: {e}. Starting fresh.")
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

def search_cases(
    token: str, debtor_inn: str, creditor_inn: str, search_cache: CacheDict
) -> Tuple[Optional[List[JsonDict]], Optional[Dict[str, Set[str]]], bool]:
    """
    Performs a 'search' API call, using a cache.
    Returns a tuple: (list_of_case_responses, found_names_dict, is_retryable_error)
    """
    inn_pair_key = f"{debtor_inn}|{creditor_inn}"
    if inn_pair_key in search_cache:
        logging.info(f"Cache HIT for search: {inn_pair_key}. Using cached data.")
        cached_data = search_cache[inn_pair_key]
        # Convert lists back to sets for names
        names = {
            'debtor': set(cached_data.get('names', {}).get('debtor', [])),
            'creditor': set(cached_data.get('names', {}).get('creditor', []))
        }
        return cached_data.get('responses', []), names, False

    logging.info(f"Cache MISS for search: {inn_pair_key}. Calling API.")
    base_params: ApiParams = [
        ("token", token), ("type", "search"), ("CaseType", "G"),
        ("participant", debtor_inn), ("participantType", "1"),
        ("participant", creditor_inn), ("participantType", "0"),
    ]
    
    all_page_responses = []
    found_names = {'debtor': set(), 'creditor': set()}
    page_num = 1
    total_pages = 1

    while page_num <= total_pages:
        logging.info(f"  -> Fetching page {page_num}/{total_pages}...")
        paged_params = base_params + [("page", str(page_num))]
        response, is_retryable = make_api_request(paged_params)
        
        if is_retryable:
            return None, None, True
        
        if response and response.get("Result"):
            all_page_responses.append(response)
            
            # Extract names while iterating
            for case in response.get("Result", []):
                for p in case.get("plaintiff", []):
                    if p and str(p.get("inn", "")).strip() == creditor_inn and p.get("name"):
                        found_names['creditor'].add(p['name'])
                for r in case.get("respondent", []):
                    if r and str(r.get("inn", "")).strip() == debtor_inn and r.get("name"):
                        found_names['debtor'].add(r['name'])

            try:
                current_pages_count = int(response.get("PagesCount", 1))
                if page_num == 1:
                    total_pages = current_pages_count
            except (ValueError, TypeError):
                logging.warning(f"Could not parse 'PagesCount'. Pagination may be incomplete.")
        else:
            break
        page_num += 1

    if not all_page_responses:
         logging.info("API search returned no matching cases.")
         return [], {}, False

    logging.info(f"Finished search. Found names: Debtor: {found_names['debtor']}, Creditor: {found_names['creditor']}")
    
    # Save to cache before returning
    search_cache[inn_pair_key] = {
        'responses': all_page_responses,
        'names': {
            'debtor': list(found_names['debtor']),
            'creditor': list(found_names['creditor'])
        }
    }
    return all_page_responses, found_names, False


def filter_search_results_by_role(
    search_responses: List[JsonDict], debtor_inn: str, creditor_inn: str, found_names: Dict[str, Set[str]]
) -> List[str]:
    """
    Pre-filters the raw results from a 'search' API call using INN and Name.
    This function operates on ALREADY DOWNLOADED data.
    """
    validated_case_ids = []
    all_results = [case for resp in search_responses for case in resp.get("Result", [])]

    for case in all_results:
        # Check if any plaintiff matches the creditor's INN or one of its known names
        is_creditor_in_plaintiffs = any(
            str(p.get("inn", "")).strip() == creditor_inn or p.get("name") in found_names['creditor']
            for p in case.get("plaintiff", []) if p
        )
        # Check if any respondent matches the debtor's INN or one of its known names
        is_debtor_in_respondents = any(
            str(r.get("inn", "")).strip() == debtor_inn or r.get("name") in found_names['debtor']
            for r in case.get("respondent", []) if r
        )

        if is_creditor_in_plaintiffs and is_debtor_in_respondents:
            if "caseId" in case:
                validated_case_ids.append(case["caseId"])
        
    unique_case_ids = sorted(list(set(validated_case_ids)))
    logging.info(
        f"Pre-filtering complete. "
        f"Reduced {len(all_results)} raw results to {len(unique_case_ids)} unique cases with correct participants."
    )
    return unique_case_ids


def get_case_info(token: str, case_id: str, case_info_cache: CacheDict) -> Tuple[Optional[JsonDict], bool]:
    """
    Performs a 'caseInfo' API call, using a cache.
    Returns a tuple: (case_info_json, is_retryable_error)
    """
    if case_id in case_info_cache:
        logging.info(f"Cache HIT for caseInfo: {case_id}. Using cached data.")
        return case_info_cache[case_id], False

    logging.info(f"Cache MISS for caseInfo: {case_id}. Calling API.")
    params: ApiParams = [("token", token), ("type", "caseInfo"), ("CaseId", case_id)]
    
    response, is_retryable = make_api_request(params)
    if not is_retryable and response:
        case_info_cache[case_id] = response # Cache successful responses
    
    return response, is_retryable


# --- 3. Data Processing and Analysis Functions ---

def filter_and_extract_documents(case_info_json: JsonDict, debtor_inn: str, creditor_inn: str) -> List[Document]:
    """
    Filters events within a case to find specific court decisions.
    """
    if not case_info_json or "Result" not in case_info_json or not case_info_json.get("Result"):
        logging.warning("Case info JSON is empty or malformed. Skipping.")
        return []

    # This secondary check is still valuable as a safeguard
    participants = case_info_json.get("Result", {}).get("Participants", {})
    plaintiff_inns = {p.get("INN") for p in participants.get("Plaintiffs", []) if p.get("INN")}
    respondent_inns = {r.get("INN") for r in participants.get("Respondents", []) if r.get("INN")}

    if not (creditor_inn in plaintiff_inns and debtor_inn in respondent_inns):
        case_id = case_info_json.get("Result", {}).get("CaseInfo", {}).get("CaseId", "N/A")
        logging.warning(
            f"INN role mismatch in caseInfo for CaseId {case_id}. "
            f"Expected Debtor(R): {debtor_inn}, Creditor(P): {creditor_inn}. "
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
        # Sort by date, newest first
        sorted_docs = sorted(
            documents, key=lambda x: pd.to_datetime(x['Date'], format='%d.%m.%Y', errors='coerce'), reverse=True
        )
    except (ValueError, KeyError) as e:
        logging.warning(f"Could not sort documents by date due to format error: {e}. Using original order.")
        sorted_docs = documents
        
    return "\n".join([f"{doc['Date']}: {doc['File']}" for doc in sorted_docs])


def process_inn_pair(
    token: str, debtor_inn: str, creditor_inn: str, search_cache: CacheDict, case_info_cache: CacheDict
) -> str:
    """Main processing logic for a single unique INN pair, using caches."""
    raw_search_responses, found_names, is_retryable = search_cases(token, debtor_inn, creditor_inn, search_cache)
    
    if is_retryable:
        return RESULT_API_RETRY_ERROR
    if raw_search_responses is None:
        return RESULT_API_ERROR
    if not raw_search_responses:
        return RESULT_NO_CASES_FOUND

    validated_case_ids = filter_search_results_by_role(raw_search_responses, debtor_inn, creditor_inn, found_names)

    if not validated_case_ids:
        return RESULT_NO_SUITABLE_DOCS

    all_documents: List[Document] = []
    for case_id in validated_case_ids:
        case_info, is_retryable = get_case_info(token, case_id, case_info_cache)
        
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
    # addname = "_081825_1"
    # input_file = script_dir / "filtered_regions_descending_groupsum.csv"
    input_file = script_dir / "_test1.csv"
    output_file = script_dir / "_test1_TEST180725.csv"
    # New cache file paths
    search_cache_file = script_dir / "_TEST180725search_cache.json"
    case_info_cache_file = script_dir / "_TEST180725caseinfo_cache.json"
    results_cache_file = script_dir / "_TEST180725results_cache.json"
    
    log_file_path = script_dir / "parse_court_links180725_test180725.log"
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[
            logging.FileHandler(log_file_path, mode='w', encoding='utf-8'),
            logging.StreamHandler()
        ]
    )

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

        # Load all caches
        search_cache = load_cache(search_cache_file)
        case_info_cache = load_cache(case_info_cache_file)
        results_cache = load_cache(results_cache_file)

        df['cache_key'] = df[debtor_col] + '|' + df[creditor_col]
        unique_keys = df['cache_key'].unique()
        logging.info(f"Found {len(unique_keys)} unique INN pairs to process.")

        keys_to_fetch = [key for key in unique_keys if key not in results_cache]
        logging.info(f"{len(keys_to_fetch)} pairs are new (or failed previously) and will be processed.")

        try:
            for i, key in enumerate(keys_to_fetch):
                logging.info(f"--- Processing new pair {i+1}/{len(keys_to_fetch)}: {key} ---")
                debtor_inn, creditor_inn = key.split('|')
                
                if not debtor_inn or not creditor_inn:
                    logging.warning(f"Skipping invalid pair with empty INN: {key}")
                    result = "Invalid INN provided"
                else:
                    result = process_inn_pair(token, debtor_inn, creditor_inn, search_cache, case_info_cache)
                
                if result != RESULT_API_RETRY_ERROR:
                    results_cache[key] = result
                    # Save all caches on success
                    save_cache(search_cache_file, search_cache)
                    save_cache(case_info_cache_file, case_info_cache)
                    save_cache(results_cache_file, results_cache)
                else:
                    logging.warning(f"Pair {key} resulted in a network error. It will NOT be cached and will be retried on the next run.")
        
        except LowBalanceError:
            logging.warning("Low API balance detected. Halting processing and proceeding to save partial results.")
        
        logging.info("Mapping results back to the DataFrame.")
        df[output_col] = df['cache_key'].map(results_cache)
        df[output_col].fillna(RESULT_API_RETRY_ERROR, inplace=True)
        
        df.drop(columns=['cache_key'], inplace=True)
        df.to_csv(output_file, index=False, encoding='utf-8-sig')
        logging.info(f"--- Script Execution Finished. Results saved to {output_file} ---")

    except Exception as e:
        logging.error(f"An unexpected error occurred during file processing: {e}", exc_info=True)

if __name__ == "__main__":
    main()