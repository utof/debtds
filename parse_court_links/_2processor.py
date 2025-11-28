# File: final_processor.py

import json
import logging
from pathlib import Path
from typing import Any, Dict, List

import pandas as pd

# --- Configuration ---
log_file_path = "final_processing.log"
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(log_file_path, mode='w', encoding='utf-8'),
        logging.StreamHandler()
    ]
)

# --- Constants for output messages ---
RESULT_NO_CASES_FOUND = "нет результатов, нужна ручная проверка"
RESULT_NO_SUITABLE_DOCS = "Подходящие документы не найдены"

# --- Type Hinting Aliases ---
JsonDict = Dict[str, Any]
Document = Dict[str, str]

# --- Core Logic Functions ---

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

def filter_and_extract_documents(case_info_json: JsonDict, debtor_inn: str, creditor_inn: str) -> List[Document]:
    """
    Filters events within a single case to find specific court decisions,
    AFTER validating that the debtor and creditor INNs match their specific roles.
    """
    # Gracefully handle cases where the case_id had no data in the JSON
    if not case_info_json or "Result" not in case_info_json:
        return []

    # --- 1. Validate Participant Roles (Crucial Step) ---
    participants = case_info_json.get("Result", {}).get("Participants", {})
    plaintiff_inns = {p.get("INN") for p in participants.get("Plaintiffs", []) if p.get("INN")}
    
    # Corrected this line as per your bug find (r.get instead of p.get)
    respondent_inns = {r.get("INN") for r in participants.get("Respondents", []) if r.get("INN")}

    # Check if our specific creditor is among the plaintiffs AND our debtor is among the respondents
    is_creditor_plaintiff = creditor_inn in plaintiff_inns
    is_debtor_respondent = debtor_inn in respondent_inns

    if not (is_creditor_plaintiff and is_debtor_respondent):
        case_id = case_info_json.get("Result", {}).get("CaseInfo", {}).get("CaseId", "N/A")
        logging.warning(
            f"Role mismatch for CaseId {case_id}. "
            f"Expected Debtor(Resp): {debtor_inn}, Creditor(Plaint): {creditor_inn}. "
            f"Found Respondents: {respondent_inns}, Plaintiffs: {plaintiff_inns}. Skipping this case."
        )
        return []  # Return empty list if the roles do not match

    # --- 2. Extract Documents based on your specific rules ---
    documents: List[Document] = []
    case_instances = case_info_json.get("Result", {}).get("CaseInstances", [])

    for instance in case_instances:
        for event in instance.get("InstanceEvents", []):
            event_type = event.get("EventTypeName", "")
            content_types = event.get("ContentTypes", [])
            
            # Rule 1: EventTypeName is exactly "Решение"
            is_direct_decision = (event_type == "Решение")
            
            # Rule 2: EventTypeName is "Решения и постановления" AND ContentTypes contains "решение"
            is_filtered_decision = (
                event_type == "Решения и постановления" and
                any("решение" in str(ct).lower() for ct in content_types)
            )

            if (is_direct_decision or is_filtered_decision) and event.get("File") and event.get("Date"):
                doc = {"Date": event["Date"], "File": event["File"]}
                documents.append(doc)
                logging.info(f"  -> Found matching document in CaseId {case_info_json.get('Result', {}).get('CaseInfo', {}).get('CaseId', 'N/A')}: {doc['Date']}")

    return documents


def process_inn_pair(row: pd.Series, search_data: JsonDict, case_details_data: JsonDict) -> str:
    """
    Processes a single row (INN pair) by looking up data in the provided JSONs,
    filtering, and formatting the final result string.
    """
    debtor_inn = row['debtor_inn']
    creditor_inn = row['creditor_inn']
    processing_key = f"{debtor_inn}|{creditor_inn}"
    
    logging.info(f"--- Processing Key: {processing_key} ---")

    # --- Step 1: Get the list of case IDs from the first JSON ---
    case_ids = search_data.get(processing_key)

    # Handle cases where the INN pair wasn't found or had no results in the search phase
    if not isinstance(case_ids, list):
        logging.warning(f"No valid case ID list found for key '{processing_key}'. Result: '{case_ids}'. Using default message.")
        return RESULT_NO_CASES_FOUND

    # --- Step 2: Aggregate documents from all relevant cases ---
    all_documents: List[Document] = []
    for case_id in case_ids:
        case_info = case_details_data.get(case_id)
        if not case_info:
            logging.warning(f"CaseId '{case_id}' from search results not found in case details JSON. Skipping.")
            continue
        
        # This function performs the critical role validation and document extraction
        documents = filter_and_extract_documents(case_info, debtor_inn, creditor_inn)
        all_documents.extend(documents)

    # --- Step 3: Format the final output string ---
    if not all_documents:
        logging.info(f"No suitable documents found for key '{processing_key}' after checking {len(case_ids)} case(s).")
        return RESULT_NO_SUITABLE_DOCS

    try:
        # Sort documents by date, newest first
        sorted_docs = sorted(
            all_documents, 
            key=lambda x: pd.to_datetime(x['Date'], format='%d.%m.%Y', errors='coerce'), 
            reverse=True
        )
    except (ValueError, KeyError) as e:
        logging.warning(f"Could not sort documents by date for key '{processing_key}' due to format error: {e}. Using original order.")
        sorted_docs = all_documents
        
    formatted_links = "\n".join([f"{i+1}. {doc['Date']}: {doc['File']}" for i, doc in enumerate(sorted_docs)])
    logging.info(f"Successfully formatted {len(sorted_docs)} links for key '{processing_key}'.")
    
    return formatted_links


def main():
    """
    Main orchestrator to load data and produce the final CSV.
    """
    logging.info("--- Final Processing Script Started ---")

    # --- Define file paths ---
    script_dir = Path(__file__).parent
    input_csv_path = script_dir / "_test1.csv"
    search_results_path = script_dir / "_1test1results_search_only.json"
    case_details_path = script_dir / "_1case_info_details.json"
    final_output_path = script_dir / "_1final_output_with_links.csv"

    # --- Load all necessary data ---
    try:
        df = pd.read_csv(input_csv_path, dtype=str).fillna('')
        search_data = load_or_create_json(search_results_path)
        case_details_data = load_or_create_json(case_details_path)
        logging.info("All input files loaded successfully.")
    except FileNotFoundError as e:
        logging.error(f"FATAL: Input file not found: {e}. Please ensure all required files are in the directory.")
        return

    # --- Define column names ---
    debtor_col = "debtor_inn"
    creditor_col = "creditor_inn"
    output_col = "Ссылки на решения судов и даты"

    if debtor_col not in df.columns or creditor_col not in df.columns:
        logging.error(f"Input CSV must contain '{debtor_col}' and '{creditor_col}' columns.")
        return

    # --- Apply the processing logic to each row of the DataFrame ---
    logging.info("Starting to apply processing logic to each row...")
    df[output_col] = df.apply(
        lambda row: process_inn_pair(row, search_data, case_details_data),
        axis=1
    )
    logging.info("Processing complete.")

    # --- Save the final result ---
    df.to_csv(final_output_path, index=False, encoding='utf-8-sig')
    logging.info(f"--- Script Finished. Final results saved to {final_output_path} ---")


if __name__ == "__main__":
    main()