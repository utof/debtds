import requests
import time
import dotenv

dotenv.load_dotenv()
TOKEN = dotenv.get_key(dotenv.find_dotenv(), "api_cloud")

# Combined API call function (fake or real based on FAKE_API)
def api_call(ip, timeout, FAKE_API=True):
    if FAKE_API:
        time.sleep(2)  # Simple 2-second delay for fake call
        return {"status": 200, "data": f"Fake response for {ip}"}
    else:
        url = f"https://api-cloud.ru/api/fssp.php?type=ip&number={ip}&token={TOKEN}"
        try:
            response = requests.get(url, timeout=timeout)
            response.raise_for_status()  # Raise exception for bad status codes
            return response.json()
        except requests.exceptions.Timeout:
            return {"status": "timeout"}
        except requests.exceptions.RequestException as e:
            return {"status": e.response.status_code if e.response else 500}
        




