from SETTINGS import *
from pathlib import Path
import glob
import random
import pandas as pd
import argparse
from typing import List

def create_df(files_lst: List[Path|str]):
    # Convert Path objects -> strings once
    s = pd.Series(map(str, files_lst), dtype="string")

    # Normalize separators (handles Windows too)
    s_norm = s.str.replace("\\", "/", regex=False)

    # Split from the right into: root / status / year / filename
    parts = s_norm.str.rsplit("/", n=3, expand=True)
    parts.columns = ["_root", "status", "year", "filename"]

    # Build the DataFrame
    df_paths = pd.DataFrame({
        # strip extension robustly: remove the final dot-suffix
        "ack_id": parts["filename"].str.replace(r"\.[^.]+$", "", regex=True).astype("string"),
        "year":   pd.to_numeric(parts["year"], errors="coerce").astype("Int32"),
        "status": parts["status"].astype("string"),
        "path":   s,  # original string path
    })

    # Keep only rows with a valid numeric year (optional)
    df_paths = df_paths[df_paths["year"].notna()].reset_index(drop=True)

    # (Optional) Make status a category to save memory
    df_paths["status"] = df_paths["status"].astype("category")

    return df_paths

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
    SAVE_DIR = RECORDS / f"records_{YEAR}.csv"
    
    pattern = str(TRACKER / "**" / str(YEAR) / "*.txt")
    
    print(f"Creating records csv for {YEAR} and saving to {SAVE_DIR}...")

    print("Globbing all files...")
    all_files = [Path(f) for f in glob.glob(pattern, recursive=True)]
    print("Globbed all files!")

    sample_files = random.sample(all_files, k=min(5, len(all_files)))
    print("Sample files:")
    for f in sample_files:
        print(f)

    print("Creating df")
    df = create_df(all_files)

    print(f"Saving df to {SAVE_DIR}...")
    df.to_csv(SAVE_DIR, index=False)
