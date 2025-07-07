import requests
from io import BytesIO
import PyPDF2

url = "https://kad.arbitr.ru/Document/Pdf/d66ec650-78eb-4458-a07d-119e6661dac5/74a5c57f-c70c-41ef-97ff-6b2e0564d1ce/A40-168310-2021_20221011_Reshenija_i_postanovlenija.pdf?isAddStamp=True"

response = requests.get(url, timeout=10)
print("Response status code:", response.status_code)
print("Content-Type:", response.headers.get('Content-Type'))
print("Content-Length:", response.headers.get('Content-Length'))
with open("temp.pdf", "wb") as f:
    f.write(response.content)  # Save to file for manual inspection
print("Saved to temp.pdf for manual check.")

pdf_file = BytesIO(response.content)
pdf_reader = PyPDF2.PdfReader(pdf_file)
for page_num in range(len(pdf_reader.pages)):
    page = pdf_reader.pages[page_num]
    print(page.extract_text())