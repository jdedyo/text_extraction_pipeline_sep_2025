# %%
import pandas as pd
import argparse
from requests import session
import csv
import os
import random
import sys
from pathlib import Path
from progress_tracker2 import *
from credentials import USERNAME, PASSWORD
from SETTINGS import *
# %%

PAYLOAD = {"action": "login", "username": USERNAME, "password": PASSWORD}


def myLogin(payload=PAYLOAD):
    """
    log in to DOL servers
    """
    with session() as c:
        c.post("https://www.askebsa.dol.gov/BulkFOIARequest/Account.aspx", data=payload)
    return c


def download_file(pdf_url, save_pdf_path, c):
    """
    download pdf file from DOL and save on drive
    """
    c.post("https://www.askebsa.dol.gov/BulkFOIARequest/Account.aspx", data=PAYLOAD)
    response = c.get(pdf_url)
    with open(save_pdf_path, "wb") as f:
        f.write(response.content)

def download_and_sort_pdf(ack_id: str, url: str, save_dir: Path, c):
    """
    Download pdf file from DOL and save on drive in proper folder as ack_id.pdf
    """
    save_dir.mkdir(parents=True, exist_ok=True)

    save_path = save_dir / f"{ack_id}.pdf"
    try:
        download_file(url, save_path, c)
    except Exception as e:
        print('Error: ', e)
        return ack_id, -1 # pdf_downloaded = -1 indicates error in the download process!

    return ack_id, 1

# # %%
# myLogin()
# # %%
# testpath = Path('./downloaded_pdfs/test.pdf')
# testurl = 'https://www.askebsa.dol.gov/BulkFOIARequest/Listings.aspx/GetImage?ack_id=20130212125301P030028636741001&year=2012'
# # %%
# download_file(testurl, testpath, myLogin())
#  # %%
# test_dir = Path('./downloaded_pdfs/test')
# df = pd.read_csv('index_files/universe/universe_2012.csv')


# download_and_sort_pdf(df.iloc[0].ack_id, df.iloc[0].link, test_dir, myLogin())

# %%
if __name__ == '__main__':
    cxn = myLogin()

    while True:
        ack_ids = claim_many_with_link(DOWNLOAD_BATCH_SIZE)
        print('ack_ids: ', ack_ids)
        df = get_df_by_ack_ids(ack_ids)
        print('df: ', df)

        tracker = []
        for _, row in df.iterrows():
            status = download_and_sort_pdf(str(row.ack_id), str(row.link), DOWNLOAD_PATH / f'universe_{row.year}', cxn)
            print(status)
            tracker.append(status)

        update_pdf_downloaded(tracker)
        unclaim(ack_ids)