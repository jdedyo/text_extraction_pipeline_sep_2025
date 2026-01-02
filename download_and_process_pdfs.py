# %%
import pandas as pd
import argparse
import requests
from requests import Session
import csv
import os
import random
import sys
import time
from pathlib import Path
from progress_tracking import * 
from credentials import USERNAME, PASSWORD
from SETTINGS import *
from tracker import Tracker
from utils import slurm_time_remaining
import traceback
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

PAYLOAD = {"action": "login", "username": USERNAME, "password": PASSWORD}

def myLogin(payload=PAYLOAD) -> Session:
    """
    Log in to DOL servers and return a live requests.Session.
    """
    s = requests.Session()
    s.post("https://www.askebsa.dol.gov/BulkFOIARequest/Account.aspx", data=payload)
    return s

def download_file(pdf_url: str, save_pdf_path: Path, c: Session):
    """
    Download pdf file from DOL and save on drive.
    """
    # refresh auth (cheap) then fetch
    c.post("https://www.askebsa.dol.gov/BulkFOIARequest/Account.aspx", data=PAYLOAD)
    resp = c.get(pdf_url)
    resp.raise_for_status()
    with open(save_pdf_path, "wb") as f:
        f.write(resp.content)

def download_file_with_retry(pdf_url: str, save_pdf_path: Path, c: Session, max_retries: int = 5, backoff: float = 2.0):
    """
    Download PDF file from DOL and save on drive, retrying on HTTP 5xx and connection errors.
    Raises the most recent exception if all attempts fail.
    """
    last_exception = None

    for attempt in range(1, max_retries + 1):
        try:
            download_file(pdf_url, save_pdf_path, c)
            return

        except (requests.exceptions.HTTPError, requests.exceptions.ConnectionError, requests.exceptions.Timeout) as e:
            last_exception = e
            status = getattr(e.response, "status_code", None)

            # Retry only on transient errors: 5xx server errors, timeouts, connection errors
            if isinstance(e, requests.exceptions.HTTPError) and (status is not None and status < 500):
                # Don't retry on 4xx errors (like 404 Not Found)
                raise

            if attempt < max_retries:
                wait_time = backoff * (2 ** (attempt - 1))
                print(f"[Attempt {attempt}/{max_retries}] Error {status or type(e).__name__} for {pdf_url}. Retrying in {wait_time:.1f}s...")
                time.sleep(wait_time)
            else:
                print(f"Failed after {max_retries} attempts for {pdf_url}")
                raise

    # In case the loop exits without raising
    if last_exception:
        raise last_exception

def download_and_sort_pdf(t: Tracker, c: Session, download_dir: Path=DOWNLOADING, save_dir: Path=DOWNLOADED):
    """
    Download pdf file from DOL and save on drive in proper folder as ack_id.pdf.
    Then move the tracker file from CLAIMED → TO_PROCESS.
    """

    tmp_dir = download_dir / t.year
    tmp_dir.mkdir(parents=True, exist_ok=True)

    tmp_path = tmp_dir / f"{t.ack_id}.pdf.temp"

    final_dir = save_dir / t.year
    final_dir.mkdir(parents=True, exist_ok=True)

    final_path = final_dir / f"{t.ack_id}.pdf"

    url = t.get_link()
    if not url.strip():
        url = t.get_facsimile_link()
    try:
        # 1) download to temp
        download_file_with_retry(url, tmp_path, c)

        # 2) atomically finalize on same FS
        tmp_path.rename(final_path)

        # 3) move tracker token CLAIMED -> TO_PROCESS (preserve year)
        q = t.update_status(TO_PROCESS)

        return Tracker(q)
    except Exception as e:
        print("Error:", e)
        print(f"[ERR] download failed for {t.ack_id}; moving token to ERROR.")
        t.update_status(DOWNLOAD_FAILED, traceback.format_exc())
        # cleanup best-effort
        tmp_path.unlink(missing_ok=True)
        final_path.unlink(missing_ok=True)
        return None


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

def process_and_sort_pdf(t: Tracker):
    if not check_valid_pdf(t):
        t.get_pdf_path().unlink(missing_ok=True)
        return False
    
    try:
        page_num = get_plan_page_num(t)
    except:
        t.update_status(PDF_READER_FAILED, traceback.format_exc())
        t.get_pdf_path().unlink(missing_ok=True)
        return False

    if page_num == -1:
        t.get_pdf_path().unlink(missing_ok=True)
        return False
    
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

    return True

# %%
if __name__ == '__main__':
    cxn = myLogin()
    parser = argparse.ArgumentParser(description="Download PDFs for a given year.")
    parser.add_argument(
        "--year",
        type=int,
        required=True,
        help="Year to process (e.g. 2012)",
    )
    args = parser.parse_args()

    YEAR = args.year
    print(f"Downloading PDFs for year {YEAR}...")

    while True:

        try: 
            if slurm_time_remaining() < pd.Timedelta(minutes=15):
                break
        except:
            pass

        files = claim_n(TO_DOWNLOAD, CLAIMED, YEAR, DOWNLOAD_AND_PROCESS_BATCH_SIZE)
        print(files)


        if not files:
            year_dir = TO_DOWNLOAD / str(YEAR)
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
            new_tracker = download_and_sort_pdf(t, cxn)
            
            print(new_tracker)

            if new_tracker:
                process_and_sort_pdf(new_tracker)