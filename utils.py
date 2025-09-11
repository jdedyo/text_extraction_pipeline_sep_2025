import os
import subprocess
import pandas as pd
import argparse
import json
from requests import session
import random
from shutil import copy
import os
import pypdf
from tracker import Tracker
from SETTINGS import *

def normalize_slurm_time(s: str) -> str:
    s = s.strip()
    if "-" in s:  # D-HH:MM:SS -> "D days HH:MM:SS"
        d, rest = s.split("-", 1)
        return f"{int(d)} days {rest}"
    if s.count(":") == 1:  # MM:SS -> "0:MM:SS"
        mm, ss = s.split(":")
        return f"00:{int(mm):02d}:{int(ss):02d}"
    return s

def slurm_time_remaining():
    """
    Returns remaining walltime as a pandas.Timedelta,
    or None if not running inside a Slurm job.
    """
    job_id = os.environ.get("SLURM_JOB_ID")
    if job_id is None:
        return None

    # Query Slurm for remaining time (D-HH:MM:SS, HH:MM:SS, or MM:SS)
    result = subprocess.run(
        ["squeue", "-j", job_id, "--noheader", "--format=%L"],
        capture_output=True, text=True
    )
    time_left_str = result.stdout.strip()
    if not time_left_str:
        return None

    # Let pandas parse it into a Timedelta
    td = pd.to_timedelta(normalize_slurm_time(time_left_str))
    return td

# # Example usage
# td = slurm_time_remaining()
# if td is not None:
#     print(f"Remaining time: {td} ({td.total_seconds():.0f} seconds)")
# else:
#     print("Not running inside a Slurm job.")

def save_page_selection(pageNumber: int, inputFile: Path, outputFile: Path, number_pages_to_save: int=NUMBER_PAGES_TO_SAVE):
    """Extract pages from a PDF and save them in a new PDF.

    Args:
        pageNumber (int): Page index to start extraction from (0-based).
        inputFile (str | Path): Path to source PDF.
        outputFile (str | Path): Path to write new PDF.
        number_pages_to_save (int): Number of pages to save starting at pageNumber.

    Returns:
        None
    """
    inputFile = Path(inputFile)
    outputFile = Path(outputFile)
    outputFile.parent.mkdir(parents=True, exist_ok=True)

    pdfWriter = pypdf.PdfWriter()
    with inputFile.open("rb") as pdfFileObj, outputFile.open("wb") as pdfOutput:
        pdfReader = pypdf.PdfReader(pdfFileObj)
        last_page = min(pageNumber + number_pages_to_save - 1, len(pdfReader.pages) - 1)
        for i in range(pageNumber, last_page + 1):
            pdfWriter.add_page(pdfReader.pages[i])
        pdfWriter.write(pdfOutput)
    
    return


# check if file can be read
def corrupted_file_detector(input_file: Path):
    """The function checks if a file can be read as a pdf.

    Args:
        input_file (str): path of the file to be read as a pdf

    Returns:
        file_is_corrupted (bool): True if file is corrupted, i.e. cannot be read
    """
    file_is_corrupted = True
    try:
        with open(input_file, "rb") as pdfFileObj:
            pdfReader = pypdf.PdfReader(pdfFileObj)
            # try access number of page
            numtemp = len(pdfReader.pages)  # pdfReader.numPages
            # try access all pages # added 2 lines below, haven't tested it yet
            for i in range(numtemp):
                # pageObj not used, just to check if it works
                pageObj = pdfReader.pages[i]
            file_is_corrupted = False
    # except (pypdf.utils.PdfReadError, IOError):
    except (pypdf.errors.PdfReadError, IOError):
        file_is_corrupted = True
    return file_is_corrupted


# check if the pdf file is suspiciously too long
def long_file_detector(input_file: Path, limit_pages: int=100):
    """The function checks if a file is too long.

    Args:
        input_file (Path): path of the file to check
        limit_pages (int): minimum number of pages to be considered as too long

    Returns:
        file_is_long (bool): True if file is too long
    """
    file_is_long = False
    with open(input_file, "rb") as pdfFileObj:
        pdfReader = pypdf.PdfReader(pdfFileObj)
        # if too many pages, print message and return file_is_long as True
        if len(pdfReader.pages) > limit_pages:
            print(f"{input_file} -> file too long")
            file_is_long = True
    return file_is_long