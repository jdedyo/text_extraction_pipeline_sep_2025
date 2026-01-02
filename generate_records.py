from SETTINGS import *
from pathlib import Path
import glob
import random
import pandas as pd
import argparse
from typing import List
import numpy as np

def create_df(files_lst: List[Path|str]):
    # Convert Path objects to strings
    s = pd.Series(map(str, files_lst), dtype="string")

    # Normalize separators (handles Windows)
    s_norm = s.str.replace("\\", "/", regex=False)

    # Split from the right into: root / status / year / filename
    parts = s_norm.str.rsplit("/", n=3, expand=True)
    parts.columns = ["_root", "status", "year", "filename"]

    # Build df
    df_paths = pd.DataFrame({
        # strip extension robustly: remove the final dot-suffix
        "ack_id": parts["filename"].str.replace(r"\.[^.]+$", "", regex=True).astype("string"),
        "year":   pd.to_numeric(parts["year"], errors="coerce").astype("Int32"),
        "status": parts["status"].astype("string"),
        "path":   s,  # original string path
    })

    # Keep only rows with a valid numeric year
    df_paths = df_paths[df_paths["year"].notna()].reset_index(drop=True)

    # Make status a category to save memory
    df_paths["status"] = df_paths["status"].astype("category")

    return df_paths

def get_ocr_text(row):
    if row['status'] == 'processed':
        try:
            return Path(row['path']).read_text()
        except Exception as e:
            return f"[Error reading file: {e}]"
    else:
        return np.nan

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Aggregate metadata and create all_ocr_YYYY.csv for a given year.")
    parser.add_argument(
        "--year",
        type=int,
        help="Optional year to process (e.g. 2012)",
    )
    args = parser.parse_args()

    years = [args.year] if args.year else YEARS

    for y in years:
        RECORDS_SAVE_DIR = RECORDS / f"records_{y}.csv"
        ALL_OCR_SAVE_DIR = ALL_OCR / f"all_ocr_{y}.csv"
        
        pattern = str(TRACKER / "**" / str(y) / "*.txt")
        
        print(f"Creating records csv for {y} and saving to {RECORDS_SAVE_DIR}...")

        print("Globbing all files...")
        all_files = [Path(f) for f in glob.glob(pattern, recursive=True)]
        print("Globbed all files!")

        sample_files = random.sample(all_files, k=min(5, len(all_files)))
        print("Sample files:")
        for f in sample_files:
            print(f)

        if all_files:
            print("Creating df")
            df = create_df(all_files)
        else:
            print(f"No files found for year {y}. Moving on...")
            continue

        print(f"Saving df to {RECORDS_SAVE_DIR}...")
        df.to_csv(RECORDS_SAVE_DIR, index=False)

        print(f"Creating all_ocr csv for {y} and saving to {ALL_OCR_SAVE_DIR}...")

        df[OCR_TEXT_COL] = df.apply(get_ocr_text, axis=1)
        df2 = df[~df[OCR_TEXT_COL].isna()].copy()
        df2[OCR_TEXT_COL] = (
            df2[OCR_TEXT_COL]
        .astype(str)
        .str.encode("latin1", errors="ignore")
        .str.decode("utf-8", errors="ignore")
    )

        df2.to_csv(ALL_OCR_SAVE_DIR, index=False)