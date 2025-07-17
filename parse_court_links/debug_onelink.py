import os
import logging
from pathlib import Path
import requests
from dotenv import load_dotenv

# --- Logging Setup ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
file_handler = logging.FileHandler("debug_onelink.log")
file_handler.setLevel(logging.INFO)
file_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
logging.getLogger().addHandler(file_handler)

# --- Load Environment Variables ---
load_dotenv()
API_TOKEN = os.getenv("api_cloud")  # Make sure your .env file has api_cloud=your_token

# --- Constants ---
BASE_URL = "https://api-cloud.ru/api/kad_arbitr.php"
API_TIMEOUT = 120

# --- Specific INNs ---
DEBTOR_INN = "7536165991"    # Example debtor INN
CREDITOR_INN = "7536169450"  # Example creditor INN

def search_cases(token: str, debtor_inn: str, creditor_inn: str):
    params = {
        "token": token,
        "debtor_inn": debtor_inn,
        "creditor_inn": creditor_inn,
        "action": "search"
    }
    try:
        logging.info(f"Sending request to API for debtor_inn={debtor_inn}, creditor_inn={creditor_inn}")
        response = requests.get(BASE_URL, params=params, timeout=API_TIMEOUT)
        response.raise_for_status()
        data = response.json()
        logging.info(f"API response: {data}")
        return data
    except Exception as e:
        logging.error(f"API request failed: {e}", exc_info=True)
        return None
    
def get_case_info(token: str, case_id: str):
    """
    Performs a 'caseInfo' API call.
    """
    params = {
        "token": token,
        "type": "caseInfo",
        "CaseId": case_id
    }
    try:
        logging.info(f"Sending caseInfo request for CaseId={case_id}")
        response = requests.get(BASE_URL, params=params, timeout=API_TIMEOUT)
        response.raise_for_status()
        data = response.json()
        logging.info(f"Fetched case info for CaseId {case_id}: {data}")
        if data.get("found"):
            return data.get("Result")
        return None
    except Exception as e:
        logging.error(f"caseInfo API request failed: {e}", exc_info=True)
        return None

def main():
    if not API_TOKEN:
        logging.error("API_TOKEN not found in environment variables.")
        return
    result = search_cases(API_TOKEN, DEBTOR_INN, CREDITOR_INN)
    if result and "Result" in result:
        logging.info("Successfully fetched data from API.")
        case_ids = [item["caseId"] for item in result["Result"] if "caseId" in item]
        for case_id in case_ids:
            case_info = get_case_info(API_TOKEN, case_id)
            if case_info:
                logging.info(f"Case info for {case_id}: {case_info}")
            else:
                logging.info(f"No case info found for {case_id}.")
    else:
        logging.info("No data returned or error occurred.")

if __name__ == "__main__":
    main()