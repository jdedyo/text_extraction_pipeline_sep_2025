#!/bin/bash

# Run this to set up your conda environment. 
# Don't change the name or else you'll have to edit it in all bash scripts.

module purge
module load miniconda
module load poppler/22.12.0-GCC-12.2.0
conda create -n text_extraction_sep_2025 -c conda-forge python=3.11 pypdf pdfminer.six pymupdf pdf2image pytesseract pillow pandas cryptography pycryptodome cffi tesseract requests
pip install --upgrade ocrmypdf