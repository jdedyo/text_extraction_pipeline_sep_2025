# %% 
import pandas as pd
import csv
from pathlib import Path
from tqdm import tqdm

# %%
def load_dol_file_to_df(year: int, index_files_path: Path):
# path to your file
    file_path = index_files_path / f"{year}.txt"

    # df = pd.read_csv(file_path, sep="|", engine="python", dtype=str, skiprows=3)

    df = pd.read_csv(
        file_path,
        sep="|",
        skiprows=3,
        engine="python",
        dtype=str,
        quoting=csv.QUOTE_NONE,  # don't treat " as special
        escapechar="\\",         # required with QUOTE_NONE
        on_bad_lines="warn"      # or "skip"
    )

    df = df[~df.iloc[:,0].str.fullmatch("-+", na=False)]  # drop the dashed row
    df.columns = df.columns.str.strip()

    df.rename(columns={'id': 'ack_id'}, inplace=True)
    df["ack_id"] = df["ack_id"].astype(str)
    df["filing_year"] = df["filing_year"].astype(int)
    df = df.apply(lambda col: col.map(lambda x: x.strip() if isinstance(x, str) else x))
    df["sponsor_name"] = df["sponsor_name"].str.strip('"')
    return df

def get_acceptable_ack_ids_from_index_file(filepath: Path):
    print("Loading index file...\n")

    df = pd.read_stata(filepath, columns=["ack_id", "pension_benefit_code"])

    df = df.loc[df.pension_benefit_code.str.contains("2J|2K", na=False)]
    df["ack_id"] = df["ack_id"].astype(str)
    df["ack_id"] = df["ack_id"].map(lambda x: x.strip())

    good_ack_ids = set(df.ack_id.tolist())

    print("Finished loading index file...\n")

    return good_ack_ids


# %%
YEARS = list(range(1999, 2025))
INDEX_FILES_PATH = Path("./index_files")

SAVEPATH = INDEX_FILES_PATH / 'universe'
SAVEPATH.mkdir(parents=True, exist_ok=True)

if __name__ == '__main__':
    
    good_ack_ids = get_acceptable_ack_ids_from_index_file(INDEX_FILES_PATH / 'merged_sh_h.dta')

    for year in tqdm(
        YEARS,
        total=len(YEARS),
        desc=f"Creating universe_all.csv"
    ):
        print(f"Reading in {year}.txt...\n")

        dol_df = load_dol_file_to_df(year, INDEX_FILES_PATH / 'dol_index_files')
        filtered_df = dol_df.loc[dol_df.ack_id.isin(good_ack_ids)]

        print(f"Concatenating {year} to universe_all...\n")
        try:
            universe = pd.concat([universe, filtered_df], ignore_index=True)
        except NameError:
            universe = filtered_df
        print(f"Concatenated {year} to universe_all!\n")
    
    print(f"Saving universe_all.csv")
    universe = universe.drop_duplicates()
    universe["filing_year"] = pd.to_numeric(universe["filing_year"], errors="coerce")
    universe.to_csv(SAVEPATH / 'universe_all.csv', index=False)
    print("Saved universe_all.csv!\n")

    all_valid = universe["filing_year"].notna() & universe["filing_year"].astype(str).str.strip().ne("")
    all_valid = all_valid.all()

    print(f"Are all filing_year entries filled in? {all_valid}")

    for year in tqdm(
        YEARS,
        total=len(YEARS),
        desc=f"Setting up universe_YYYY.csv files"
    ):

        mask = universe["filing_year"].eq(year)  # boolean Series
        df_year = universe.loc[mask]

        df_year.to_csv(SAVEPATH / f'universe_{year}.csv', index=False)
        
        print(f"Saved universe_{year}.csv!\n")