from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
import requests
from PyPDF2 import PdfReader
from io import BytesIO
import time

class PDFSession:
    def __init__(self, wait_sec: int = 15):
        self.wait_sec = wait_sec
        self._init_browser()

    def _init_browser(self):
        chrome_options = Options()
        chrome_options.add_argument("--headless=new")
        chrome_options.add_argument("--disable-blink-features=AutomationControlled")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--window-size=1920,1080")
        chrome_options.add_argument("--lang=ru-RU")
        chrome_options.add_argument("user-agent=Mozilla/5.0")
        self.driver = webdriver.Chrome(
            service=Service(ChromeDriverManager().install()),
            options=chrome_options
        )

    def fetch_pdf_content(self, url: str) -> str | None:
        try:
            self.driver.get(url)
            time.sleep(self.wait_sec)  # give CAPTCHA time to auto-resolve
            cookies = self.driver.get_cookies()
            session = requests.Session()
            for cookie in cookies:
                session.cookies.set(cookie['name'], cookie['value'])
            response = session.get(url, timeout=20)
            if response.headers.get("Content-Type", "").lower() != "application/pdf":
                return None
            reader = PdfReader(BytesIO(response.content))
            return "\n".join([p.extract_text() or "" for p in reader.pages])
        except Exception as e:
            print(f"[ERROR] Failed to fetch {url}: {e}")
            return None

    def close(self):
        self.driver.quit()
