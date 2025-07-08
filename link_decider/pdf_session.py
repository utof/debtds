# pdf_session.py

import random
import time
import logging
from io import BytesIO
from typing import Optional

import requests
from PyPDF2 import PdfReader
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.common.exceptions import WebDriverException
from webdriver_manager.chrome import ChromeDriverManager
import sys

# Basic logging setup
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("pdfsession.log", encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ]
)

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.5735.91 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_0) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.1 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; rv:109.0) Gecko/20100101 Firefox/115.0",
]

class PDFSession:
    def __init__(self, wait_sec: float = 15, headless: bool = True, retries: int = 2, captcha_prompt: bool = True):
        self.wait_sec = wait_sec
        self.headless = headless
        self.retries = retries
        self.captcha_prompt = captcha_prompt
        self.driver = self._init_browser(headless=self.headless)

    def _init_browser(self, headless: bool):
        chrome_options = Options()
        if headless:
            chrome_options.add_argument("--headless=new")

        chrome_options.add_argument("--disable-blink-features=AutomationControlled")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--window-size=1920,1080")
        chrome_options.add_argument("--lang=ru-RU")

        user_agent = random.choice(USER_AGENTS)
        chrome_options.add_argument(f"user-agent={user_agent}")
        logging.info(f"Using user-agent: {user_agent}")

        try:
            return webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)
        except WebDriverException as e:
            logging.warning("Headless browser failed. Trying headful mode.")
            if headless:
                return self._init_browser(headless=False)
            else:
                raise e

    def _simulate_wait(self):
        sleep_time = self.wait_sec + random.uniform(0.5, 2.5)
        logging.info(f"Sleeping for {sleep_time:.2f} seconds")
        time.sleep(sleep_time)

    def close(self):
        self.driver.quit()

    def fetch_pdf_content(self, url: str) -> Optional[str]:
        for attempt in range(1, self.retries + 1):
            try:
                logging.info(f"[Attempt {attempt}] Navigating to {url}")
                self.driver.get(url)

                self._simulate_wait()

                # Copy cookies to requests session
                cookies = self.driver.get_cookies()
                session = requests.Session()
                for cookie in cookies:
                    session.cookies.set(cookie["name"], cookie["value"])

                # Wait until PDF is actually served
                max_wait_time = 10  # seconds
                check_interval = 1  # seconds

                for i in range(int(max_wait_time / check_interval)):
                    response = session.get(url, timeout=20)
                    content_type = response.headers.get("Content-Type", "").lower()

                    if content_type.startswith("application/pdf"):
                        break

                    logging.info(f"[WAIT] PDF not ready (got {content_type}), retrying...")
                    time.sleep(check_interval)
                else:
                    logging.warning(f"[TIMEOUT] Still no PDF after {max_wait_time}s. Last content-type: {content_type}")
                    return None

                # Extract text
                reader = PdfReader(BytesIO(response.content))
                texts = []
                for i, page in enumerate(reader.pages):
                    text = page.extract_text()
                    if not text:
                        logging.warning(f"[WARN] Page {i+1} has no extractable text")
                    texts.append(text or "")
                final_text = "\n".join(texts).strip()

                if not final_text:
                    logging.warning(f"[EMPTY TEXT] PDF content is blank: {url}")
                    return None

                logging.info(f"[SUCCESS] Extracted PDF text (first 100 chars):\n{final_text[:100]!r}")
                return final_text

            except Exception as e:
                logging.error(f"Error on attempt {attempt} for {url}: {e}")
                self._simulate_wait()

        logging.error(f"[FAILED] Could not fetch PDF after {self.retries} attempts: {url}")
        return None

