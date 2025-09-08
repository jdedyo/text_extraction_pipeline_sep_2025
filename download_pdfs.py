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

def download_and_sort_pdf(p: Path, url: str, download_dir: Path, save_dir: Path, c: Session):
    """
    Download pdf file from DOL and save on drive in proper folder as ack_id.pdf.
    Then move the tracker file from CLAIMED → TO_PROCESS.
    """
    ack_id, year = get_ack_id_year_from_path(p)

    tmp_dir = download_dir / year
    tmp_dir.mkdir(parents=True, exist_ok=True)
    tmp_path = tmp_dir / f"{ack_id}.pdf.temp"

    final_dir = save_dir / year
    final_dir.mkdir(parents=True, exist_ok=True)
    final_path = final_dir / f"{ack_id}.pdf"

    try:
        # 1) download to temp
        download_file(url, tmp_path, c)

        # 2) atomically finalize on same FS
        tmp_path.rename(final_path)

        # 3) move tracker token CLAIMED -> TO_PROCESS (preserve year)
        new_tracker_file = unclaim_to(p, TO_PROCESS)
        return new_tracker_file
    except Exception as e:
        print("Error:", e)
        print(f"[ERR ] download failed for {ack_id}; moving token to ERROR.")
        unclaim_to(p, ERROR / 'download_failed')
        # cleanup best-effort
        tmp_path.unlink(missing_ok=True)
        final_path.unlink(missing_ok=True)
        return None

# %%
if __name__ == '__main__':
    cxn = myLogin()

    print("Loading universe_all.csv ...")
    # Load once
    universe_all = (
        pd.read_csv(INDEX / "universe_all.csv",
                    dtype={"ack_id": str, "link": str},
                    low_memory=False)
          .drop_duplicates()
    )
    # normalize once
    universe_all["ack_id"] = universe_all["ack_id"].astype(str).str.strip()
    universe_all["link"]   = universe_all["link"].astype(str).str.strip()
    
    get_link = lambda id: universe_all.loc[universe_all["ack_id"].str.strip() == id].link.iloc[0]

    # If ack_id is unique, index it (fast path). Otherwise, keep as before.
    if universe_all["ack_id"].is_unique:
        print("ack_id unique in universe_all. Setting it as index.")
        universe_all = universe_all.set_index("ack_id")
        get_link = lambda id: universe_all.at[id, "link"]

    while True:
        tracker_paths = claim_n_any_year(TO_DOWNLOAD, CLAIMED, DOWNLOAD_BATCH_SIZE)
        print(tracker_paths)

        if not tracker_paths:
            print("No items to claim; sleeping 2s...")
            time.sleep(2)
            continue

        for p in tracker_paths:
            ack_id, year = get_ack_id_year_from_path(p)
            print(ack_id, year)

            # Find the link row(s) for this ack_id
            link_val = get_link(ack_id)
            if not isinstance(link_val, str) or not link_val.strip() or link_val.strip().lower() in {"nan","none","null"}:
                print(f"[WARN] ack_id {ack_id} has no usable link; sending to ERROR.")
                unclaim_to(p, ERROR / 'no_link_provided')
                continue

            # Do the download & handoff to TO_PROCESS
            new_tracker = download_and_sort_pdf(
                p=p,
                url=link_val.strip(),
                download_dir=DOWNLOADING,
                save_dir=DOWNLOADED,
                c=cxn
            )
            print(new_tracker)
        i+=1