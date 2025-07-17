# File: api_processor.py

import logging
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import requests

# --- Constants ---
BASE_URL = "https://api-cloud.ru/api/kad_arbitr.php"
API_TIMEOUT = 120

# --- Result Messages (self-contained in this module) ---
RESULT_NO_CASES_FOUND = "нет результатов, нужна ручная проверка"
RESULT_NO_SUITABLE_DOCS = "Подходящие документы не найдены"
RESULT_API_ERROR = "API Error during processing"
RESULT_API_RETRY_ERROR = "Сетевая ошибка, требуется повторная попытка"

# --- Type Hinting Aliases ---
ApiParams = List[Tuple[str, str]]
JsonDict = Dict[str, Any]
Document = Dict[str, str]


def make_api_request(params: ApiParams) -> Tuple[Optional[JsonDict], bool]:
    """
    Makes a GET request to the API Cloud endpoint.
    Returns a tuple: (response_json, is_retryable_error)
    """
    try:
        response = requests.get(BASE_URL, params=params, timeout=API_TIMEOUT)
        response.raise_for_status()
        json_response = response.json()
        
        if json_response.get("status") != 200:
            error_msg = json_response.get('errormsg', 'Unknown API error')
            logging.warning(f"API returned non-200 status: {error_msg}")
            return None, False
            
        return json_response, False

    except requests.exceptions.RequestException as e:
        logging.error(f"API request failed with a network error: {e}")
        return None, True


def search_cases(token: str, debtor_inn: str, creditor_inn: str) -> Tuple[Optional[List[str]], bool]:
    """
    Performs a 'search' API call.
    Returns a tuple: (list_of_case_ids, is_retryable_error)
    """
    logging.info(f"Searching for cases with Debtor(Resp): {debtor_inn}, Creditor(Plaint): {creditor_inn}")
    params: ApiParams = [
        ("token", token), ("type", "search"), ("CaseType", "G"),
        ("participant", debtor_inn), ("participantType", "1"),
        ("participant", creditor_inn), ("participantType", "0"),
    ]
    
    response, is_retryable = make_api_request(params)
    
    if is_retryable:
        return None, True

    if response and response.get("Result"):
        case_ids = [item["caseId"] for item in response["Result"] if "caseId" in item]
        logging.info(f"Found {len(case_ids)} potential case(s).")
        return case_ids, False
        
    logging.info("API search returned no matching cases.")
    return [], False


def get_case_info(token: str, case_id: str) -> Tuple[Optional[JsonDict], bool]:
    """
    Performs a 'caseInfo' API call.
    Returns a tuple: (case_info_json, is_retryable_error)
    """
    logging.info(f"Fetching details for CaseId: {case_id}")
    params: ApiParams = [("token", token), ("type", "caseInfo"), ("CaseId", case_id)]
    return make_api_request(params)


def filter_and_extract_documents(case_info_json: JsonDict, debtor_inn: str, creditor_inn: str) -> List[Document]:
    """Filters events to find specific court decisions after validating participant roles."""
    if not case_info_json or "Result" not in case_info_json:
        return []

    participants = case_info_json.get("Result", {}).get("Participants", {})
    plaintiff_inns = {p.get("INN") for p in participants.get("Plaintiffs", []) if p.get("INN")}
    respondent_inns = {r.get("INN") for r in participants.get("Respondents", []) if r.get("INN")}

    if not (creditor_inn in plaintiff_inns and debtor_inn in respondent_inns):
        case_id = case_info_json.get("Result", {}).get("CaseInfo", {}).get("CaseId", "N/A")
        logging.warning(f"INN role mismatch for CaseId {case_id}. Skipping.")
        return []

    documents: List[Document] = []
    for instance in case_info_json.get("Result", {}).get("CaseInstances", []):
        for event in instance.get("InstanceEvents", []):
            event_type = event.get("EventTypeName", "")
            content_types = event.get("ContentTypes", [])
            
            is_direct_decision = (event_type == "Решение")
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
        sorted_docs = sorted(
            documents, key=lambda x: pd.to_datetime(x['Date'], format='%d.%m.%Y', errors='coerce'), reverse=True
        )
    except (ValueError, KeyError):
        sorted_docs = documents
    return "\n".join([f"{i+1}. {doc['Date']}: {doc['File']}" for i, doc in enumerate(sorted_docs)])


def process_single_inn_pair(token: str, debtor_inn: str, creditor_inn: str) -> str:
    """
    Main processing function for a single unique INN pair.
    This is the primary function to be called from the main runner script.
    """
    case_ids, is_retryable = search_cases(token, debtor_inn, creditor_inn)
    
    if is_retryable:
        return RESULT_API_RETRY_ERROR
    if case_ids is None:
        return RESULT_API_ERROR
    if not case_ids:
        return RESULT_NO_CASES_FOUND

    all_documents: List[Document] = []
    for case_id in case_ids:
        case_info, is_retryable = get_case_info(token, case_id)
        
        if is_retryable:
            logging.warning(f"Network error while fetching CaseId {case_id}. Marking entire pair for retry.")
            return RESULT_API_RETRY_ERROR
            
        if case_info:
            documents = filter_and_extract_documents(case_info, debtor_inn, creditor_inn)
            all_documents.extend(documents)
        else:
            logging.warning(f"Failed to retrieve or parse caseInfo for CaseId: {case_id}.")
            
    return format_results(all_documents)