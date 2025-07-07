import PyPDF2

def extract_pdf_text(file_path):
    with open(file_path, 'rb') as file:
        pdf_reader = PyPDF2.PdfReader(file)
        for page_num in range(len(pdf_reader.pages)):
            page = pdf_reader.pages[page_num]
            print(page.extract_text())

if __name__ == "__main__":
    extract_pdf_text("link_decider\\test.pdf")  # Replace with your PDF file path