import pandas as pd
import argparse
import requests
from requests import Session
import os
import random
import sys
import time
from pathlib import Path
from progress_tracking import * 
from credentials import USERNAME, PASSWORD
from SETTINGS import *
from tracker import Tracker
from utils import slurm_time_remaining, corrupted_file_detector, long_file_detector, save_page_selection
import traceback

from page_selectors import (
    page_selector_method_pdfminer_six, 
    page_selector_method_pypdf, 
    page_selector_method_ocrmypdf, 
    page_selector_pytesseract
)

from pdf_to_text import pdf_to_text

def check_valid_pdf(t: Tracker):
    pdf = t.get_pdf_path()
    if corrupted_file_detector(pdf):
        t.update_status(CORRUPTED_FILE)
        return False
    elif long_file_detector(pdf):
        t.update_status(FILE_TOO_LONG)
        return False
    return True
    
def get_plan_page_num(t: Tracker):
    print("page selection starts")

    raw_file_path = str(t.get_pdf_path()) # Antoine's old functions require string paths

    # look for plan description in pdf
    page_num = -1
    # pdfminer.sx
    page_num = page_selector_method_pdfminer_six(raw_file_path, TERMS)
    # method = (tack_id, "pdfminer.six")
    # pypdf
    if page_num == -1:
        page_num = page_selector_method_pypdf(raw_file_path, TERMS)
        # method = (ack_id, "pypdf")
    # print(page_num)
    # pytesseract
    if page_num == -1:
        page_num = page_selector_pytesseract(raw_file_path, TERMS)
        # method = (ack_id, "pytesseract")

    if page_num == -1:
        print(
            "cannot find the description of plan - record in error folder"
        )
        # record file in error folder
        t.update_status(PLAN_NOT_FOUND)
    return page_num

def save_selection(t: Tracker, page_num: int, number_pages_to_save: int=NUMBER_PAGES_TO_SAVE):
    pdf_path = t.get_pdf_path()

    selection_path = (SELECTION / t.year / f"{t.ack_id}_selection.pdf")

    save_page_selection(page_num, pdf_path, selection_path, number_pages_to_save)

    return selection_path


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Download PDFs for a given year.")
    parser.add_argument(
        "--year",
        type=int,
        required=True,
        help="Year to process (e.g. 2012)",
    )
    args = parser.parse_args()

    YEAR = args.year

    print(f"Processing PDFs for year {YEAR}...")

    while True:
        
        try:
            if slurm_time_remaining() < pd.Timedelta(minutes=PROCESS_BATCH_SIZE*1.5): # Rule of thumb: each pdf takes 90sec to process
                print(f"Slurm job nearly out of time. Aborting to avoid being cut off.")
                break
        except:
            pass
        
        files = claim_n(TO_PROCESS, CLAIMED, YEAR, PROCESS_BATCH_SIZE)
        print(files)

        if not files:
            year_dir = TO_PROCESS / str(YEAR)
            if not any(os.scandir(year_dir)):
                print(f"No items to claim for year {YEAR}. Aborting job.")
                break
            else:
                wait_time = random.uniform(0, 10)
                print(f"Sleeping for {wait_time:.1f} seconds before next batch...")
                time.sleep(wait_time)
                continue
        
        

        for p in files:
            t = Tracker(p)

            if not check_valid_pdf(t):
                t.get_pdf_path().unlink(missing_ok=True)
                continue
            
            try:
                page_num = get_plan_page_num(t)
            except:
                t.update_status(PDF_READER_FAILED, traceback.format_exc())
                t.get_pdf_path().unlink(missing_ok=True)
                continue

            if page_num == -1:
                t.get_pdf_path().unlink(missing_ok=True)
                continue
            
            selection_path = save_selection(t, page_num)

            text = pdf_to_text(selection_path)

            # check if text contains "the":
            if "the" not in text.lower():
                print("cannot find 'the' in the text")
                t.update_status(TEXT_EXTRACTION_EMPTY, text)
            # save txt file and overwrite anything already stored in it
            else:
                t.update_status(PROCESSED, text, overwrite=True)

            selection_path.unlink(missing_ok=True)
            t.get_pdf_path().unlink(missing_ok=True)

            print(text[:500])