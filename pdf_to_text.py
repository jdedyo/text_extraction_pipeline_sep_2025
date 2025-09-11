# using pdfminer
import pdfminer.high_level
import logging
import pytesseract
from pdf2image import convert_from_path
import unicodedata

original_level = logging.getLogger("pdfminer").level
logging.getLogger("pdfminer").setLevel(logging.ERROR)


def pdf_to_text_method_pdfminer(pdf_file_path):
    """Convert pdf to text using pdfminer.six (no OCR).

    Args:
        pdf_file_path (str): Path to pdf file

    Returns:
        str: text extracted from pdf
    """
    text = pdfminer.high_level.extract_text(pdf_file_path)
    return text


def pdf_to_text_pytesseract(pdf_file_path):
    """Convert pdf to text using pytesseract (OCR).

    Args:
        pdf_file_path (str): Path to pdf file

    Returns:
        str: text extracted from pdf
    """
    # Convert the PDF to a series of images
    images = convert_from_path(pdf_file_path)
    # Perform OCR on each image
    texts = []
    for img in images:
        texts.append(pytesseract.image_to_string(img))
    # Join the OCR results from each page into a single string
    return "\n".join(texts)


def pdf_to_text(pdf_file_path, method=None):
    """Convert pdf to text using pdfminer or pytesseract (using OCR if needed).

    Args:
        pdf_file_path (str): Path to pdf file.
        method (_type_, optional): _description_. Defaults to None.

    Returns:
        str: text extracted from pdf
    """
    # try extracting text directly (without OCR)
    text = pdf_to_text_method_pdfminer(pdf_file_path)
    # if text is empty or doesn't contain "the", use OCR
    if "the" not in text.lower():
        text = pdf_to_text_pytesseract(pdf_file_path)
    
    text = unicodedata.normalize("NFKC", text)  # normalize Unicode characters
    text = text.encode("utf-8", errors="ignore").decode("utf-8")  # drop any bad bytes
    text = text.replace("\r\n", "\n").replace("\r", "\n")  # unify newlines
    text = text.strip()

    return text


if __name__ == "__main__":
    pass
