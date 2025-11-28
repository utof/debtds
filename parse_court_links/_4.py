from pathlib import Path
import json

# --- Setup as requested by the user ---
# The script will look for the JSON files in the same folder it is run from.
this_folder = Path(__file__).parent
search_only_json_path = this_folder / "_1test1results_search_only.json"
case_info_json_path = this_folder / "_1case_info_details.json"

# The specific key you want to check
target_key = "1004004449|7707282610"


def run_verification():
    """
    Finds a specific key in the search JSON, gets its case IDs,
    and verifies their existence in the case info JSON.
    """
    print("--- Starting Case ID Verification ---")
    print(f"Target Key: {target_key}")
    print(f"Search File: {search_only_json_path.name}")
    print(f"Details File: {case_info_json_path.name}\n")

    # --- 1. Load both JSON files ---
    try:
        with open(search_only_json_path, 'r', encoding='utf-8') as f:
            search_data = json.load(f)
        with open(case_info_json_path, 'r', encoding='utf-8') as f:
            case_info_data = json.load(f)
    except FileNotFoundError as e:
        print(f"❌ ERROR: Could not find a required file: {e}")
        return
    except json.JSONDecodeError as e:
        print(f"❌ ERROR: Could not parse a JSON file. Please check its format: {e}")
        return

    # --- 2. Get the list of case IDs for the target key ---
    case_ids_to_check = search_data.get(target_key)

    if not case_ids_to_check:
        print(f"❌ ERROR: The key '{target_key}' was not found in '{search_only_json_path.name}'.")
        return

    total_ids = len(case_ids_to_check)
    print(f"Found {total_ids} case IDs for the target key. Now checking for them...")

    # --- 3. Check which case IDs are missing from the details file ---
    missing_ids = []
    for case_id in case_ids_to_check:
        if case_id not in case_info_data:
            missing_ids.append(case_id)

    # --- 4. Print the final report ---
    print("\n--- Verification Report ---")
    if not missing_ids:
        print(f"✅ SUCCESS: All {total_ids} case IDs were found in '{case_info_json_path.name}'.")
    else:
        found_count = total_ids - len(missing_ids)
        print(f"⚠️  PARTIAL MATCH: Found {found_count} out of {total_ids} case IDs.")
        print(f"The following {len(missing_ids)} case ID(s) were NOT found in '{case_info_json_path.name}':")
        for mid in missing_ids:
            print(f"  - {mid}")
    print("---------------------------\n")


if __name__ == "__main__":
    # # To make this script runnable for testing, we can create dummy files
    # # if they don't exist. In your real use case, your actual files will be used.
    
    # # Create a dummy search_only file if it doesn't exist
    # if not search_only_json_path.exists():
    #     print("Creating dummy search file for demonstration...")
    #     dummy_search_data = {
    #         "1004004449|7707282610": [
    #             "f44f84fd-0c4b-4f06-a501-00843a3251c1", # This one will be found
    #             "adba293e-7d43-4f3e-98ea-ef04406670e5", # This one will be missing
    #             "c1c1c1c1-c1c1-c1c1-c1c1-c1c1c1c1c1c1"  # This one will be found
    #         ],
    #         "another|key": ["..."]
    #     }
    #     with open(search_only_json_path, 'w', encoding='utf-8') as f:
    #         json.dump(dummy_search_data, f, indent=4)

    # # Create a dummy case_info file if it doesn't exist
    # if not case_info_json_path.exists():
    #     print("Creating dummy case info file for demonstration...")
    #     dummy_case_info = {
    #         "f44f84fd-0c4b-4f06-a501-00843a3251c1": {"details": "some case info..."},
    #         "c1c1c1c1-c1c1-c1c1-c1c1-c1c1c1c1c1c1": {"details": "more case info..."},
    #         "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx": {"details": "unrelated case info..."}
    #     }
    #     with open(case_info_json_path, 'w', encoding='utf-8') as f:
    #         json.dump(dummy_case_info, f, indent=4)
            
    run_verification()