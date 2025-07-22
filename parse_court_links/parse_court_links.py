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
CacheDict = Dict[str, Any]


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
    token: str, debtor_inn: str, creditor_inn: str, search_cache: CacheDict, max_pages_to_fetch: int
) -> Tuple[Optional[List[str]], bool, Optional[str]]:
    """
    Performs a 'search' API call, using a stateful cache to allow resuming.
    Returns a tuple: (list_of_case_ids, is_retryable_error, page_limit_warning_message)
    """
    inn_pair_key = f"{debtor_inn}|{creditor_inn}"
    
    # --- Cache Handling Logic ---
    cached_data = search_cache.get(inn_pair_key)
    
    # Initialize state for the search
    all_case_ids: Set[str] = set()
    page_num = 1
    total_pages = 1
    
    if cached_data:
        # Handle new dictionary-based cache format
        if isinstance(cached_data, dict) and 'is_complete' in cached_data:
            if cached_data['is_complete']:
                logging.info(f"Cache HIT (Complete) for search: {inn_pair_key}. Using cached data.")
                return cached_data['case_ids'], False, None
            else:
                logging.info(f"Cache HIT (Partial) for search: {inn_pair_key}. Resuming.")
                all_case_ids = set(cached_data.get('case_ids', []))
                page_num = cached_data.get('last_page_fetched', 0) + 1
                total_pages = cached_data.get('total_pages', 1)
        # Handle old list-based cache format (for backward compatibility)
        elif isinstance(cached_data, list):
            logging.info(f"Cache HIT (Legacy format) for search: {inn_pair_key}. Treating as complete.")
            return cached_data, False, None
    else:
        logging.info(f"Cache MISS for search: {inn_pair_key}. Starting new search.")

    # --- API Call Loop ---
    multi_participant_str = f"{creditor_inn}:0,{debtor_inn}:1"
    base_params: ApiParams = [
        ("token", token), ("type", "search"), ("CaseType", "G"), ("participant", multi_participant_str),
    ]
    page_limit_warning: Optional[str] = None
    
    # --- CHANGE 1: Add a flag to track if we broke because results ended ---
    did_break_early = False
    
    # The loop now starts from where it left off (or page 1)
    while page_num <= total_pages and page_num <= max_pages_to_fetch:
        logging.info(f"  -> Fetching page {page_num}/{total_pages} (limit: {max_pages_to_fetch})...")
        paged_params = base_params + [("page", str(page_num))]
        response, is_retryable = make_api_request(paged_params)

        if is_retryable:
            return None, True, None

        # The API can return a valid response with an empty or null "Result"
        if response and response.get("Result"):
            for case in response.get("Result", []):
                if "caseId" in case:
                    all_case_ids.add(case["caseId"])

            try:
                # Update total_pages if it has changed or is new
                current_total_pages = int(response.get("PagesCount", 1))
                if current_total_pages != total_pages:
                    logging.info(f"Total pages for {inn_pair_key} is {current_total_pages}.")
                    total_pages = current_total_pages
            except (ValueError, TypeError):
                logging.warning(f"Could not parse 'PagesCount'. Pagination may be incomplete.")
                # --- CHANGE 2: Set the flag and break if pagination info is bad ---
                did_break_early = True
                break
        else:
            # This means the API stopped returning results, so the search is complete.
            # --- CHANGE 3: Set the flag before breaking the loop ---
            did_break_early = True
            break
        page_num += 1

    # --- Process and Save Results ---
    last_page_processed = page_num - 1
    
    # --- CHANGE 4: A search is complete if we processed all pages OR if the API stopped returning results ---
    is_now_complete = (last_page_processed >= total_pages) or did_break_early
    
    if not all_case_ids and is_now_complete:
        logging.info("API search returned no matching cases. Caching as complete.")
        # Cache the empty but complete result
        search_cache[inn_pair_key] = {'case_ids': [], 'last_page_fetched': last_page_processed, 'total_pages': total_pages, 'is_complete': True}
        return [], False, None

    # Generate warning if we stopped due to the fetch limit but there were more pages
    if not is_now_complete and total_pages > max_pages_to_fetch:
        page_limit_warning = f"собрано только {max_pages_to_fetch}/{total_pages} страниц"
        logging.warning(f"Page limit hit for {inn_pair_key}: Fetched up to page {max_pages_to_fetch} of {total_pages}.")

    sorted_case_ids = sorted(list(all_case_ids))
    logging.info(f"Finished search. Found {len(sorted_case_ids)} unique case(s).")

    # ALWAYS update the cache with the latest state (partial or complete)
    search_cache[inn_pair_key] = {
        'case_ids': sorted_case_ids,
        'last_page_fetched': last_page_processed,
        'total_pages': total_pages,
        'is_complete': is_now_complete
    }
    logging.info(f"Search state for {inn_pair_key} saved to cache (Complete: {is_now_complete}).")

    return sorted_case_ids, False, page_limit_warning

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

def filter_and_extract_documents(case_info_json: JsonDict) -> List[Document]:
    """
    Filters events within a case to find specific court decisions based on your rules.
    """
    if not case_info_json or not case_info_json.get("Result"):
        logging.warning("Case info JSON is empty or malformed. Skipping.")
        return []

    documents: List[Document] = []
    case_instances = case_info_json.get("Result", {}).get("CaseInstances", [])

    for instance in case_instances:
        for event in instance.get("InstanceEvents", []):
            event_type = event.get("EventTypeName", "")
            content_types = event.get("ContentTypes", [])

            # Rule 1: EventTypeName is "Решение" or "Решения"
            is_direct_decision = event_type in ("Решение", "Решения")

            # Rule 2: EventTypeName is "Решения и постановления" AND ContentTypes contains "решение"
            is_filtered_decision = (
                event_type == "Решения и постановления"
                and any(
                    "решени" in str(ct).lower() and "резолют" not in str(ct).lower()
                    for ct in content_types
                )
            )

            if (is_direct_decision or is_filtered_decision) and event.get("File") and event.get("Date"):
                doc = {"Date": event["Date"], "File": event["File"]}
                documents.append(doc)
                logging.info(f"  -> Found matching document: {doc['Date']}")

    return documents

def format_results(documents: List[Document]) -> str:
    """Formats the list of found documents into a sorted, string."""
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
    token: str, debtor_inn: str, creditor_inn: str, search_cache: CacheDict, case_info_cache: CacheDict, max_pages_to_fetch: int
) -> str:
    """Main processing logic for a single unique INN pair, using caches."""
    case_ids, is_retryable, page_limit_warning = search_cases(token, debtor_inn, creditor_inn, search_cache, max_pages_to_fetch)

    if is_retryable:
        return RESULT_API_RETRY_ERROR
    if case_ids is None: # Non-retryable API error
        return RESULT_API_ERROR
    if not case_ids:
        return RESULT_NO_CASES_FOUND

    all_documents: List[Document] = []
    for case_id in case_ids:
        case_info, is_retryable = get_case_info(token, case_id, case_info_cache)

        if is_retryable:
            logging.warning(f"Network error while fetching CaseId {case_id}. Marking entire pair for retry.")
            return RESULT_API_RETRY_ERROR

        if case_info:
            documents = filter_and_extract_documents(case_info)
            all_documents.extend(documents)
        else:
            logging.warning(f"Failed to retrieve or parse caseInfo for CaseId: {case_id} (non-retryable error).")

    final_result_string = format_results(all_documents)

    if page_limit_warning:
        return f"{page_limit_warning}\n{final_result_string}"
    else:
        return final_result_string


# --- 4. Main Execution Block ---

def main():
    """Main function to run the entire script."""
    script_dir = Path(__file__).parent
    
    # NEW: Maximum number of pages to fetch per search.
    MAX_PAGES_TO_FETCH = 1

    # addname = "_081825_1"
    input_file = script_dir / "filtered_regions_descend_summa_no_groups.csv"
    output_file = script_dir / "filtered_rdsng_1_output.csv"

    # input_file = script_dir / "_test1.csv"
    # output_file = script_dir / "_test1_2_output.csv"
    # log_file_path = script_dir / "_test1_2.log"
    # search_cache_file = script_dir / "_test1_2_cache_search.json"
    # case_info_cache_file = script_dir / "_test1_2_cache_caseinfo.json"
    # results_cache_file = script_dir / "_test1_2_cache_results.json"

    search_cache_file = script_dir / "filtered_rdsng_1_cache_search.json"
    case_info_cache_file = script_dir / "filtered_rdsng_1_cache_caseinfo.json"
    results_cache_file = script_dir / "filtered_rdsng_1_cache_results.json"

    log_file_path = script_dir / "filtered_rdsng_1.1.log"
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[
            logging.FileHandler(log_file_path, mode='w', encoding='utf-8'),
            logging.StreamHandler()
        ]
    )
    logging.info("--- Script Execution Started ---")
    token = get_api_token()
    if not token:
        return

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

        # Create a key for unique INN pairs to process
        df['cache_key'] = df[debtor_col].str.strip() + '|' + df[creditor_col].str.strip()
        unique_keys = df['cache_key'].unique()
        logging.info(f"Found {len(unique_keys)} unique INN pairs to process.")

        # Process only keys that are not already in the final results cache
        keys_to_fetch = [key for key in unique_keys if key not in results_cache]
        logging.info(f"{len(keys_to_fetch)} pairs are new (or failed previously) and will be processed.")

        try:
            for i, key in enumerate(keys_to_fetch):
                logging.info(f"--- Processing new pair {i+1}/{len(keys_to_fetch)}: {key} ---")

                try:
                    debtor_inn, creditor_inn = key.split('|')
                except ValueError:
                    logging.warning(f"Skipping invalid key format: {key}")
                    results_cache[key] = "Invalid INN pair key"
                    continue

                if not debtor_inn or not creditor_inn:
                    logging.warning(f"Skipping invalid pair with empty INN: {key}")
                    result = "Invalid INN provided"
                else:
                    result = process_inn_pair(token, debtor_inn, creditor_inn, search_cache, case_info_cache, MAX_PAGES_TO_FETCH)

                # Only cache final results, not retryable errors
                if result != RESULT_API_RETRY_ERROR:
                    results_cache[key] = result
                    # Save all caches on success to prevent data loss
                    save_cache(search_cache_file, search_cache)
                    save_cache(case_info_cache_file, case_info_cache)
                    save_cache(results_cache_file, results_cache)
                else:
                    logging.warning(f"Pair {key} resulted in a network error. It will NOT be cached and will be retried on the next run.")

        except LowBalanceError:
            logging.warning("Low API balance detected. Halting processing and proceeding to save partial results.")

        logging.info("Mapping results back to the DataFrame.")
        df[output_col] = df['cache_key'].map(results_cache)
        # Mark rows that were skipped due to errors for easy filtering
        df[output_col].fillna(RESULT_API_RETRY_ERROR, inplace=True)

        df.drop(columns=['cache_key'], inplace=True)
        df.to_csv(output_file, index=False, encoding='utf-8-sig')
        logging.info(f"--- Script Execution Finished. Results saved to {output_file} ---")

    except Exception as e:
        logging.error(f"An unexpected error occurred during file processing: {e}", exc_info=True)

if __name__ == "__main__":
    main()