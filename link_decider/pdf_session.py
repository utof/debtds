# pdf_session.py (updated)
import random
import time
import logging
import base64
from io import BytesIO
from typing import Optional
import sys

import requests
from PyPDF2 import PdfReader
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.common.exceptions import WebDriverException, TimeoutException
from webdriver_manager.chrome import ChromeDriverManager

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.5735.91 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_0) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.1 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; rv:109.0) Gecko/20100101 Firefox/115.0",
]

class PDFSession:
    def __init__(self, wait_sec: float = 15, headless: bool = True, retries: int = 2):
        self.wait_sec = wait_sec
        self.headless = headless
        self.retries = retries
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
        chrome_options.add_argument("--disable-extensions")
        chrome_options.add_argument("--disable-popup-blocking")
        chrome_options.add_argument("--disable-infobars")
        chrome_options.add_argument("--ignore-certificate-errors")

        user_agent = random.choice(USER_AGENTS)
        chrome_options.add_argument(f"user-agent={user_agent}")
        logging.info(f"Using user-agent: {user_agent}")

        try:
            return webdriver.Chrome(
                service=Service(ChromeDriverManager().install()), 
                options=chrome_options
            )
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

                # Try direct download first
                response = session.get(url, timeout=20)
                content_type = response.headers.get("Content-Type", "").lower()
                
                if content_type.startswith("application/pdf"):
                    reader = PdfReader(BytesIO(response.content))
                    texts = [page.extract_text() or "" for page in reader.pages]
                    final_text = "\n".join(texts).strip()
                    if final_text:
                        logging.info(f"Direct download successful: {url}")
                        return final_text
                
                # Fallback to CDP if direct download fails
                logging.error("Direct download failed.")
                # logging.info("Falling back to CDP method.")
                # result = self.driver.execute_cdp_cmd("Page.printToPDF", {
                #     "landscape": False,
                #     "displayHeaderFooter": False,
                #     "printBackground": True,
                #     "preferCSSPageSize": True,
                #     "transferMode": "ReturnAsBase64",
                #     "waitForReadyState": "complete",
                #     "timeout": 30000  # 30 seconds timeout
                # })
                
                # if pdf_data := result.get("data"):
                #     pdf_bytes = base64.b64decode(pdf_data)
                #     reader = PdfReader(BytesIO(pdf_bytes))
                #     texts = [page.extract_text() or "" for page in reader.pages]
                #     final_text = "\n".join(texts).strip()
                #     if final_text:
                #         logging.info(f"CDP fallback successful: {url}")
                #         return final_text
                #     else:
                #         logging.error("CDP fallback: PDF extracted empty text")
                # else:
                #     logging.error("CDP fallback: No PDF data received")

            except (WebDriverException, TimeoutException, requests.RequestException) as e:
                logging.error(f"Attempt {attempt} failed: {str(e)}")
                self._simulate_wait()
            except Exception as e:
                logging.error(f"Unexpected error: {str(e)}")
                self._simulate_wait()
        
        logging.error(f"All attempts failed for: {url}")
        return None