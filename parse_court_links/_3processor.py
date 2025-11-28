import json
import os
from pathlib import Path

def create_links_report(case_list_path, case_details_path, output_path):
    """
    Processes case data to generate a JSON report with links to court decisions.

    Args:
        case_list_path (str): Path to the JSON file containing debtor-creditor pairs and their case IDs.
                              Example: {"debtor_inn|creditor_inn": ["caseId1", "caseId2"]}
        case_details_path (str): Path to the JSON file containing detailed information for each caseId.
                                 Example: {"caseId1": { ... API response ... }}
        output_path (str): Path to save the resulting JSON report.
    """
    print("Starting the process...")

    # --- 1. Load input JSON files ---
    try:
        with open(case_list_path, 'r', encoding='utf-8') as f:
            case_lists = json.load(f)
        with open(case_details_path, 'r', encoding='utf-8') as f:
            case_details = json.load(f)
        print(f"Successfully loaded {len(case_lists)} keys from '{case_list_path}'.")
        print(f"Successfully loaded {len(case_details)} case details from '{case_details_path}'.")
    except FileNotFoundError as e:
        print(f"Error: Input file not found. {e}")
        return
    except json.JSONDecodeError as e:
        print(f"Error: Failed to decode JSON from a file. Check the file format. {e}")
        return

    final_results = {}

    # --- 2. Iterate through each debtor-creditor pair ---
    for key, case_ids in case_lists.items():
        try:
            debtor_inn, creditor_inn = key.split('|')
        except ValueError:
            print(f"Warning: Skipping invalid key format: '{key}'")
            continue

        found_docs_for_key = []

        # --- 3. Iterate through each case associated with the pair ---
        for case_id in case_ids:
            case_data_wrapper = case_details.get(case_id)

            # Skip if case_id is not in the details file or the response is empty/failed
            if not case_data_wrapper or not case_data_wrapper.get("Result"):
                continue

            case_data = case_data_wrapper["Result"]

            # --- 4. Verify that participants match the debtor and creditor ---
            participants = case_data.get("Participants", {})
            plaintiffs = participants.get("Plaintiffs", [])
            defendants = participants.get("Defendants", [])

            # Extract INNs for easy lookup. Using a set for efficient checking.
            plaintiff_inns = {p.get("INN") for p in plaintiffs if p.get("INN")}
            defendant_inns = {d.get("INN") for d in defendants if d.get("INN")}

            # If the creditor and debtor from our key don't match the case participants, skip this case
            if creditor_inn not in plaintiff_inns or debtor_inn not in defendant_inns:
                continue

            # --- 5. Filter documents within the matched case ---
            documents = case_data.get("CaseDocuments", [])
            for doc in documents:
                event_type_name = doc.get("EventTypeName", "")
                content_types = doc.get("ContentTypes", [])
                file_url = doc.get("File")
                doc_date = doc.get("Date")

                if not file_url or not doc_date:
                    continue  # We need both a date and a URL to create a result

                # Make the check case-insensitive
                event_type_lower = event_type_name.lower()
                is_match = False

                # RULE 1: EventTypeName contains "решени" (matches "Решение", "Решения")
                if "решени" in event_type_lower:
                    # RULE 2 (Additional check): If it's "Решения и постановления", check ContentTypes
                    if "постановления" in event_type_lower:
                        # Check if any content type contains "решени"
                        for content in content_types:
                            if isinstance(content, str) and "решени" in content.lower():
                                is_match = True
                                break # Found a match, no need to check other content types
                    else:
                        # It contains "решени" but not "постановления", so it's a direct match
                        is_match = True

                # --- 6. If it's a match, format and store the result ---
                if is_match:
                    formatted_string = f"{doc_date}: {file_url}"
                    found_docs_for_key.append(formatted_string)

        # Join all found documents for the key with a newline character
        final_results[key] = "\n".join(found_docs_for_key)

    # --- 7. Write the final dictionary to the output JSON file ---
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(final_results, f, ensure_ascii=False, indent=4)

    print(f"\nProcessing complete. Results saved to '{output_path}'.")


def debug_find_matching_participant_cases(case_list_path, case_details_path, output_path):
    """
    DEBUG FUNCTION: Checks which cases have matching participants.
    
    This function isolates the participant matching logic. It creates a JSON file
    mapping each 'debtor|creditor' key to a comma-separated string of case IDs
    where the debtor was found as a defendant/respondent and the creditor as a plaintiff.
    """
    print("--- RUNNING DEBUG FUNCTION ---")
    try:
        with open(case_list_path, 'r', encoding='utf-8') as f:
            case_lists = json.load(f)
        with open(case_details_path, 'r', encoding='utf-8') as f:
            case_details = json.load(f)
    except FileNotFoundError as e:
        print(f"Debug Error: Input file not found. {e}")
        return

    matching_cases = {}

    for key, case_ids in case_lists.items():
        try:
            debtor_inn, creditor_inn = key.split('|')
        except ValueError:
            continue

        found_matches_for_key = []
        for case_id in case_ids:
            case_data_wrapper = case_details.get(case_id)
            if not case_data_wrapper or not case_data_wrapper.get("Result"):
                continue

            case_data = case_data_wrapper["Result"]
            participants = case_data.get("Participants", {})
            
            # Get plaintiffs and defendants/respondents safely
            plaintiffs = participants.get("Plaintiffs", [])
            # *** KEY CHANGE: Check for both "Defendants" and "Respondents" ***
            defendants = participants.get("Defendants", []) or participants.get("Respondents", [])

            # Ensure lists are not None before creating sets
            plaintiff_inns = {p.get("INN") for p in plaintiffs if p and p.get("INN")}
            defendant_inns = {d.get("INN") for d in defendants if d and d.get("INN")}

            if creditor_inn in plaintiff_inns and debtor_inn in defendant_inns:
                found_matches_for_key.append(case_id)
        
        matching_cases[key] = ", ".join(found_matches_for_key)

    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(matching_cases, f, ensure_ascii=False, indent=4)
    
    print(f"Debug process complete. Participant match results saved to '{output_path}'.")
    # Print the content for immediate feedback
    print("\n--- Content of debug_results.json ---")
    print(json.dumps(matching_cases, ensure_ascii=False, indent=4))
    print("-------------------------------------\n")

# --- Demonstration ---
if __name__ == '__main__':

    this_folder = Path(__file__).parent

    info_details = this_folder / "_1case_info_details.json"
    case_list = this_folder / "_1test1results_search_only.json"
    output_path = this_folder / "_3results.json"
    # --- Run the main function ---
    debug_find_matching_participant_cases(
        case_list_path=case_list,
        case_details_path=info_details,
        output_path=output_path
    )
    # create_links_report(
    #     case_list_path=case_list,
    #     case_details_path=info_details,
    #     output_path=output_path
    # )
