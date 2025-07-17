import json
import logging
from pathlib import Path
from typing import List, Dict, Any

# Define types for clarity
JsonDict = Dict[str, Any]
Document = Dict[str, str]

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

# --- Example Usage ---
# Assuming 'case_data' is your loaded JSON from the file

# Plaintiff INN: "7536169450" (ООО ЛУНСЯН)
# Respondent INN: "7536165991" (ООО СТОИК)
this_folder = Path(__file__).parent
case_info_json = json.loads((this_folder / "_debug_json_caseinfo3.json").read_text(encoding="utf-8"))

# This call will now succeed because the roles match and the data path is correct.
found_docs = filter_and_extract_documents(
    case_info_json=case_info_json, # your full JSON object
    creditor_inn="7536169450", # Must be the plaintiff
    debtor_inn="7536165991"    # Must be the respondent
)

print(found_docs)