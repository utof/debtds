import time
import requests
from PyPDF2 import PdfReader
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from io import BytesIO
import tempfile
import os

PDF_URL = "https://kad.arbitr.ru/Document/Pdf/d66ec650-78eb-4458-a07d-119e6661dac5/74a5c57f-c70c-41ef-97ff-6b2e0564d1ce/A40-168310-2021_20221011_Reshenija_i_postanovlenija.pdf?isAddStamp=True"

def get_pdf_text_via_selenium(url):
    # Setup Chrome options
    chrome_options = Options()
    chrome_options.add_argument("--headless=new")
    chrome_options.add_argument("--disable-blink-features=AutomationControlled")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--window-size=1920,1080")
    chrome_options.add_argument("--lang=ru-RU")
    chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.5735.91 Safari/537.36")

    # Create driver
    driver = webdriver.Chrome(options=chrome_options)

    print("[INFO] Opening URL...")
    driver.get(url)

    # Optional: Wait up to 60s for PDF to load or CAPTCHA to complete manually
    print("[INFO] Waiting for potential captcha... (you can press Ctrl+C to skip if stuck)")
    time.sleep(15)

    # After 15 seconds, try to extract cookies and close browser
    cookies = driver.get_cookies()
    driver.quit()

    print("[INFO] Retrieved cookies from browser")

    # Transfer cookies into requests
    session = requests.Session()
    for cookie in cookies:
        session.cookies.set(cookie['name'], cookie['value'])

    print("[INFO] Downloading PDF using requests + browser cookies...")
    response = session.get(url, timeout=20)
    if response.headers.get("Content-Type", "").lower() != "application/pdf":
        raise Exception("Did not receive PDF. Possible CAPTCHA not solved or session blocked.")

    # Parse PDF from memory
    with tempfile.NamedTemporaryFile(delete=False, suffix=".pdf") as tmp_pdf:
        tmp_pdf.write(response.content)
        tmp_path = tmp_pdf.name

    print("[INFO] PDF downloaded. Extracting text...")
    reader = PdfReader(tmp_path)
    text = "\n".join([page.extract_text() or "" for page in reader.pages])
    os.remove(tmp_path)
    return text.strip()

if __name__ == "__main__":
    text = get_pdf_text_via_selenium(PDF_URL)
    print("\n========== EXTRACTED TEXT ==========\n")
    print(text[:3000] + "\n..." if len(text) > 3000 else text)
