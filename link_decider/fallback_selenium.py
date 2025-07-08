# test_pdf_fallback.py
from pdf_session import PDFSession
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()]
)

if __name__ == "__main__":

    test_url = 'https://kad.arbitr.ru/Document/pdf/1466b26b-a78b-4f06-b20b-5b585de8d924/0feb7f8d-68ea-4add-81ff-0d25151f934c/A40-166659-2022_20230217_Reshenija_i_postanovlenija.pdf'
    # test_url = 'https://kad.arbitr.ru/Document/pdf/e1586bee-e46e-4aae-9b96-2d5383606e94/4d1ba62c-187b-48c6-916e-db74386b8e1f/A40-144553-2022_20221212_Reshenija_i_postanovlenija.pdf'
    # test_url = 'https://kad.arbitr.ru/Document/pdf/bb256184-b4b6-46e7-892e-1b26086c4d33/e95aa916-2baf-49cc-9b7f-c39b02839bfa/A40-166077-2022_20221221_Reshenija_i_postanovlenija.pdf'
    
    session = PDFSession(
        wait_sec=5,
        headless=False,  # Always use headful for visibility
        retries=2
    )
    
    try:
        text = session.fetch_pdf_content(test_url)
        if text:
            print("\n" + "="*50)
            # print(f"Extracted Text (first 500 chars):\n{text[-1000:]}...")
            print(f"full extracted text:\n{text}")
            print("="*50)
        else:
            print("Failed to extract text")
    finally:
        session.close()


