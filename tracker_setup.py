import logging
from SETTINGS import *
import argparse
import pandas as pd
from tqdm import tqdm
import logging
import argparse
from SETTINGS import *
import pandas as pd
from tqdm import tqdm

# --------------------------
# Configure logging
# --------------------------
logging.basicConfig(
    filename="tracker_setup.log",      # file to write logs
    filemode="a",                      # "a" = append, "w" = overwrite
    level=logging.INFO,                # log INFO and above
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

# --------------------------
# Parse arguments
# --------------------------
parser = argparse.ArgumentParser(description="Set up tracker folders")
parser.add_argument(
    "--year",
    type=int,
    help="Optional: a single year to run (e.g., --year 2010)"
)
args = parser.parse_args()

years = [args.year] if args.year else YEARS

for year in years:
    logging.info(f"Loading universe_{year}.csv ...")
    universe_year = pd.read_csv(INDEX / f"universe_{year}.csv", low_memory=False)
    logging.info(f"Loaded universe_{year}.csv!")

    for ack_id in tqdm(
        universe_year.ack_id.tolist(),
        total=len(universe_year),
        desc=f"Seeding tracker folder {year}"
    ):
        save_dir = TO_DOWNLOAD / f"{year}"
        save_dir.mkdir(parents=True, exist_ok=True)

        save_path = save_dir / f"{ack_id}.txt"
        save_path.touch(exist_ok=True)

# preview first 10 files
x = list(TO_DOWNLOAD.glob("*/*"))[:10]
logging.info(f"Sample tracker files: {x}")
logging.info(f"All tracker files created!")

# print("Loading universe_all.csv")
# universe_all = pd.read_csv(INDEX / "universe_all.csv")
# print("Loaded universe_all.csv")

# id_years = zip(universe_all.ack_id.tolist(), universe_all.filing_year.tolist())

# for ack_id, year in tqdm(id_years, total=len(universe_all), desc="Seeding the tracker folder"):
#     year = int(year)
#     save_dir = TO_DOWNLOAD / f"{year}"
#     save_dir.mkdir(parents=True, exist_ok=True)

#     save_path = save_dir / f"{ack_id}.txt"
#     save_path.touch(exist_ok=True)

# x = list(TO_DOWNLOAD.glob("*/*"))[:10]
# print(x)