import requests

import dotenv
# Load environment variables from .env file
dotenv.load_dotenv()
TOKEN = dotenv.get_key(dotenv.find_dotenv(), "api_cloud")

def fedresurs_call(inn, fake_api=False):
    """Make an API call to Fedresurs for a given INN."""
    if fake_api:
        # Return a mock response for testing
        return {"status": 200, "data": f"Mock response for INN {inn}"}
    else:
        # Real API call
        url = f"https://api-cloud.ru/api/bankrot.php?type=searchString&string={inn}&legalStatus=legal&token={TOKEN}"
        try:
            response = requests.get(url, timeout=10)  # 10-second timeout
            response.raise_for_status()  # Raise an error if the request fails
            return response.json()
        except requests.RequestException as e:
            # Return error info if the API call fails
            return {"status": "error", "message": str(e)}