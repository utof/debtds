# pdf_session.py

import time
import random
import requests
from io import BytesIO
from PyPDF2 import PdfReader
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager

class PDFSession:
    def __init__(self, wait_sec: float = 15, headless: bool = True):
        self.wait_sec = wait_sec
        self.headless = headless
        self._init_browser()

    def _init_browser(self):
        chrome_options = Options()
        if self.headless:
            chrome_options.add_argument("--headless=new")
        chrome_options.add_argument("--disable-blink-features=AutomationControlled")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--window-size=1920,1080")
        chrome_options.add_argument("--lang=ru-RU")
        chrome_options.add_argument(
            "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.5735.91 Safari/537.36"
        )
        self.driver = webdriver.Chrome(
            service=Service(ChromeDriverManager().install()),
            options=chrome_options,
        )

    def fetch_pdf_content(self, url: str) -> str | None:
        print(f"[INFO] Navigating to URL:\n{url}")
        try:
            self.driver.get(url)
            print("[INFO] Waiting for potential CAPTCHA or auto-load...")
            time.sleep(self.wait_sec + random.uniform(1, 3))

            cookies = self.driver.get_cookies()
            session = requests.Session()
            for cookie in cookies:
                session.cookies.set(cookie["name"], cookie["value"])

            response = session.get(url, timeout=20)
            content_type = response.headers.get("Content-Type", "").lower()
            if not content_type.startswith("application/pdf"):
                print(f"[WARN] Not a PDF. Got content-type: {content_type}")
                return None

            reader = PdfReader(BytesIO(response.content))
            return "\n".join(page.extract_text() or "" for page in reader.pages)
        except Exception as e:
            print(f"[ERROR] Exception while fetching PDF: {e}")
            return None

    def close(self):
        self.driver.quit()
