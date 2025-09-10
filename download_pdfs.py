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
from progress_tracking import *   # get_ack_id_year_from_path, unclaim_to, claim_n_any_year, etc.
from credentials import USERNAME, PASSWORD
from SETTINGS import *
from tracker import Tracker
from utils import slurm_time_remaining

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
        download_file(url, tmp_path, c)

        # 2) atomically finalize on same FS
        tmp_path.rename(final_path)

        # 3) move tracker token CLAIMED -> TO_PROCESS (preserve year)
        q = t.update_status(TO_PROCESS)

        return q
    except Exception as e:
        print("Error:", e)
        print(f"[ERR ] download failed for {t.ack_id}; moving token to ERROR.")
        t.update_status(DOWNLOAD_FAILED)
        # cleanup best-effort
        tmp_path.unlink(missing_ok=True)
        final_path.unlink(missing_ok=True)
        return None

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
    # print("Loading universe_all.csv ...")
    # # Load once
    # universe_all = (
    #     pd.read_csv(INDEX / "universe_all.csv",
    #                 dtype={"ack_id": str, "link": str},
    #                 low_memory=False)
    #       .drop_duplicates()
    # )
    # # normalize once
    # universe_all["ack_id"] = universe_all["ack_id"].astype(str).str.strip()
    # universe_all["link"]   = universe_all["link"].astype(str).str.strip()
    
    # get_link = lambda id: universe_all.loc[universe_all["ack_id"].str.strip() == id].link.iloc[0]

    # # If ack_id is unique, index it (fast path). Otherwise, keep as before.
    # if universe_all["ack_id"].is_unique:
    #     print("ack_id unique in universe_all. Setting it as index.")
    #     universe_all = universe_all.set_index("ack_id")
    #     get_link = lambda id: universe_all.at[id, "link"]
    while True:
        files = claim_n(TO_DOWNLOAD, CLAIMED, YEAR, DOWNLOAD_BATCH_SIZE)
        print(files)

        if not files:
            print("No items to claim. Aborting job.")
            break

        if slurm_time_remaining() < pd.Timedelta(minutes=15):
            break

        for p in files:
            t = Tracker(p)
            new_tracker = download_and_sort_pdf(t, cxn)
            
            print(new_tracker)