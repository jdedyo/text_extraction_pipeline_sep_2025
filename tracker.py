from pathlib import Path
from SETTINGS import *
import pandas as pd
from functools import lru_cache

@lru_cache
def _load_csv(year: [int|str]):
    return pd.read_csv(INDEX / f"universe_{year}.csv", dtype={"ack_id": "string"}).drop_duplicates()

class Tracker():
    def __init__(self, p: Path):
        self.ack_id = p.stem
        self.status = p.parent.parent.stem
        self.year = p.parent.stem
        self.path = p
        self.filename = p.name
    
    def __repr__(self):
        return f"Tracker({self.ack_id}, {self.year}, {self.status}, {self.path})"

    def get_csv(self, index_on_id: bool=True):
        if index_on_id:
            df = _load_csv(self.year).set_index("ack_id")
        else:
            df = _load_csv(self.year)
        
        return df

    def update_status(self, status: Path, msg: str="", overwrite=False):
        dest = status / self.year / self.filename
        dest.parent.mkdir(parents=True, exist_ok=True)

        self.path.rename(dest)

        self.path = dest

        self.status = status.stem

        if msg:
            text = str(msg)
            if overwrite:
                with open(self.path, "w", encoding="utf-8") as f:
                    f.write(text + "\n")
            else:
                with open(self.path, "a", encoding="utf-8") as f:
                    f.write(text + "\n")

        return dest

    def get_link(self):
        df = self.get_csv()
        link = df.at[self.ack_id, "link"]
        if isinstance(link, str):
            return link
        return ""
    
    def get_facsimile_link(self):
        df = self.get_csv()
        link = df.at[self.ack_id, "facsimile_link"]
        if isinstance(link, str):
            return link
        return None
    
    def get_sponsor_name(self):
        df = self.get_csv()
        name = df.at[self.ack_id, "sponsor_name"]
        return name
    
    # def write(self, msg: str):
    #     text = str(msg)
    #     with open(self.path, "a") as f:
    #         f.write(text + "\n")
    # return text

    def get_pdf_path(self):
        p = (DOWNLOADED / self.year / self.ack_id).with_suffix(".pdf")
        if p.exists():
            return p
        else:
            raise FileNotFoundError(f"No pdf associated with ack_id {self.ack_id} found in {DOWNLOADED / self.year}.")
    
    def exists(self):
        return self.path.exists()