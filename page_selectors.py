"""

These functions analyze a pdf page by page, looks for "description of plan" (or a variant)
and return the page number if found (starting from 0); if not found, return -1.
The functions transform text from pdf and from terms to lowercase. It also look when deleting spaces.

Note on pypdf, pypdf, textminer.six, etc.

pypdf
pypdf is not maintained anymore.
pypdf: The last commit to its main repository was a while ago, which led many users to believe that the library was not actively maintained. However, the repository itself does not state that it's deprecated or unmaintained. Regardless, the lack of recent activity suggests that it might not be the best choice for ongoing or new projects, especially if you anticipate needing fixes or updates in the future.
pypdf is now back to PyPDF.

pdfminer.six: This is a fork of the original pdfminer for Python 3. It's been more actively maintained compared to pypdf, and is often recommended for PDF parsing tasks in Python 3.
That said, while pdfminer (or pdfminer.six for Python 3) is more versatile for extracting information from PDFs, pypdf can be simpler for certain tasks, such as merging PDFs or extracting text from a few pages.
"""

import argparse
from shutil import copy  # , rmtree
import logging
import io
import os
import re
import subprocess as sp  # added this, maybe that's why it dint work before ???


import pypdf
import ocrmypdf
import fitz  #!pip install PyMuPDF
import pdfminer.high_level  # evolution of pdfminer, conda install pdfminer.six -c conda-forge
from SETTINGS import *

# find terms in page, e.g., terms = ["Description of the plan", "Plan description"]
def find_terms_in_page(pageText, terms):
    """Return True if any term appears in pageText, case-insensitive and space/char normalized."""
    # normalize page text
    normalized_text = pageText.lower()

    # first check: direct match
    if any(term.lower() in normalized_text for term in terms):
        return True

    # second check: remove non-alphanumeric except parentheses and spaces
    normalized_text = re.sub(r"[^a-z0-9() ]", "", normalized_text)
    for term in terms:
        normalized_term = re.sub(r"[^a-z0-9() ]", "", term.lower())
        if normalized_term in normalized_text:
            return True

    return False


# pdfminer.six
def get_number_of_pages(pdf_file_path):
    """Compute number of pages of pdf using pdfminer.six

    Args:
        pdf_file_path (str): path of the pdf

    Returns:
        int: number of page in pdf
    """
    from pdfminer.pdfparser import PDFParser
    from pdfminer.pdfdocument import PDFDocument
    from pdfminer.pdfpage import PDFPage

    original_level = logging.getLogger("pdfminer").level
    logging.getLogger("pdfminer").setLevel(logging.ERROR)
    with open(pdf_file_path, "rb") as f:
        parser = PDFParser(f)
        doc = PDFDocument(parser)
        pages = [page for page in PDFPage.create_pages(doc)]
        logging.getLogger("pdfminer").setLevel(original_level)
        return len(pages)


# alternative function to compute number of pages of pdf
"""
# using pypdf
def get_number_of_pages(pdf_file_path):
    import pypdf as pypdf

    with open(pdf_file_path, "rb") as f:
        reader = pypdf.PdfReader(f)
        return len(reader.pages)


# using fitz (PyMuPDF)
def get_number_of_pages(pdf_file_path):
    import fitz
    doc = fitz.open(pdf_file_path)
    num_pages = doc.page_count
    doc.close()
    return num_pages
"""


def page_selector_method_pdfminer_six(pdf_file_path, terms):
    logging.getLogger("pdfminer").setLevel(logging.ERROR)
    number_of_pages = get_number_of_pages(pdf_file_path)
    for page in range(number_of_pages):
        pageText = pdfminer.high_level.extract_text(pdf_file_path, page_numbers=[page])
        pageText = pageText.lower()
        # print(pageText)
        # if any(pageText.find(term.lower()) != -1 for term in terms):
        #    return page
        if find_terms_in_page(pageText, terms):
            return page
    return -1


# previous name: def page_selector_extractTextNew(input_file, terms):
# works only for text pdfs
def page_selector_method_pypdf(input_file, terms):
    pageDescriptionNum = -1
    with open(input_file, "rb") as pdfFileObj:
        pdfReader = pypdf.PdfReader(pdfFileObj)
        for pageNum in range(len(pdfReader.pages)):
            if pageDescriptionNum == -1:
                # print("extractin text from page ", pageNum)
                pageObj = pdfReader.pages[pageNum]
                try:
                    pageText = pageObj.extract_text()
                    # print(pageText)
                except:
                    # pass
                    print(f"could not use extractText on page {pageNum}")
                else:
                    # if any(pageText.find(term) != -1 for term in terms):
                    if find_terms_in_page(pageText, terms):
                        pageDescriptionNum = pageNum
                        print(f"Plan description found on page {pageNum}")
                        return pageDescriptionNum  # added this - no need to loop over remaining pages if find plan description

    return pageDescriptionNum


def page_selector_method_ocrmypdf(input_pdf, terms):
    # ocrmypdf
    # fitz (PyMuPDF)
    # Note: ocrmypdf imports pdfminer (should be able to work with pdfminer.six)
    # FIXME: maybe use "sidecar: str" argument to store text
    # ocrmypdf.configure_logging(verbosity: Verbosity, *, progress_bar_friendly: bool = True, manage_root_logger: bool = False, plugin_manager: pluggy.PluginManager | None = None)
    # instructions from:
    # Creating a child process to call ocrmypdf.ocr() is suggested.
    # That way your application will survive and remain interactive even if OCRmyPDF fails for any reason.
    # For example:
    """
    from multiprocessing import Process

    def ocrmypdf_process():
        ocrmypdf.ocr('input.pdf', 'output.pdf')

    def call_ocrmypdf_from_my_app():
        p = Process(target=ocrmypdf_process)
        p.start()
        p.join()
    """

    pageDescriptionNum = -1
    with open(input_pdf, "rb") as file:
        reader = pypdf.PdfReader(file)
        # Extract page 4 (0-indexed)
        for pageNum in range(len(reader.pages)):
            if pageDescriptionNum == -1:
                writer = pypdf.PdfWriter()
                temp_pdf = re.sub(
                    ".pdf",
                    "_temp_" + str(pageNum) + ".pdf",
                    os.path.basename(input_pdf),
                )
                pdf_page = os.path.join(TEMP, temp_pdf)
                writer.add_page(reader.pages[pageNum])
                # print(f"save file to: {pdf_page}")
                with open(pdf_page, "wb") as temp_file:
                    writer.write(temp_file)
                # OCR the file
                logging.basicConfig(level=logging.WARNING)
                ocrmypdf.ocr(pdf_page, pdf_page, force_ocr=True)
                # file reader
                doc = fitz.open(pdf_page)
                # FIXME: no need to loop over pages
                for page_num in range(doc.page_count):
                    page = doc.load_page(page_num)
                    pageText = page.get_text("text")
                    # if any(pageText.find(term) != -1 for term in terms):
                    if find_terms_in_page(pageText, terms):
                        pageDescriptionNum = pageNum
                        print(f"Plan description found on page {pageNum}")
                        # return pageDescriptionNum  # added this - no need to loop over remaining pages if find plan description
                # delete temporary page
                try:
                    # deleting temporary files
                    os.remove(pdf_page)
                except OSError:
                    pass

    return pageDescriptionNum


# adapted from George code
def page_selector_pytesseract(input_pdf, terms):
    import pytesseract
    from pdf2image import convert_from_path

    def ocr_pdf(file_name):
        # Convert the PDF to a series of images
        images = convert_from_path(file_name)
        # Perform OCR on each image
        texts = []
        for img in images:
            texts.append(pytesseract.image_to_string(img))
        # Join the OCR results from each page into a single string
        return "\n".join(texts)

    pageDescriptionNum = -1
    with open(input_pdf, "rb") as file:
        reader = pypdf.PdfReader(file)
        for pageNum in range(len(reader.pages)):
            if pageDescriptionNum == -1:
                writer = pypdf.PdfWriter()
                temp_pdf = re.sub(
                    ".pdf",
                    "_temp_" + str(pageNum) + ".pdf",
                    os.path.basename(input_pdf),
                )
                pdf_page_path = os.path.join(TEMP, temp_pdf)
                writer.add_page(reader.pages[pageNum])
                print(f"save file to: {pdf_page_path}")
                with open(pdf_page_path, "wb") as temp_file:
                    writer.write(temp_file)

                # Convert the PDF to a series of images
                pageText = ocr_pdf(pdf_page_path)
                # if any(pageText.find(term) != -1 for term in terms):
                if find_terms_in_page(pageText, terms):
                    pageDescriptionNum = pageNum
                    print(f"Plan description found on page {pageNum}")
                    # return pageDescriptionNum  # added this - no need to loop over remaining pages if find plan description
                # delete temporary file
                # os.remove(pdf_page_path)
                try:
                    os.remove(pdf_page_path)
                except OSError:
                    print(f"can't delete the file {pdf_page_path}")
                    pass
                # try:
                #     os.remove(os.path.join(temp_directory, tmpfile_txt))
                # except:
                #     pass
    return pageDescriptionNum

if __name__ == "__main__":
    pass