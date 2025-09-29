from pathlib import Path

DOL_INDEX_FILES = Path("./index_files/dol_index_files")
YEARS = list(range(1999, 2025))

INDEX = Path("/nfs/roberts/project/pi_co337/jmd324/text_extraction_pipeline_sep_2025/index_files/universe")
if not INDEX.exists():
    raise FileNotFoundError(f"Index files path does not exist: {INDEX}")

MERGE_INDEX = INDEX.parent / 'merged_ppp_5500_data_compressed_2025.dta'

TRACKER = Path.home() / "scratch_pi_co337/jmd324/txt_extraction_pipeline_tracker"
TRACKER.mkdir(parents=True, exist_ok=True)

QUEUE = TRACKER / "queue"
QUEUE.mkdir(parents=True, exist_ok=True)

CLAIMED = TRACKER / "claimed"
CLAIMED.mkdir(parents=True, exist_ok=True)

TO_DOWNLOAD = QUEUE / "to_download"
TO_DOWNLOAD.mkdir(parents=True, exist_ok=True)

TO_PROCESS = QUEUE / "to_process"
TO_PROCESS.mkdir(parents=True, exist_ok=True)

DOWNLOADING = TRACKER / "downloading"
DOWNLOADING.mkdir(parents=True, exist_ok=True)

DOWNLOADED = TRACKER / "downloaded"
DOWNLOADED.mkdir(parents=True, exist_ok=True)

DOWNLOAD_BATCH_SIZE = 500

PROCESSING = TRACKER / "processing"
PROCESSING.mkdir(parents=True, exist_ok=True)

PROCESSED = TRACKER / "processed"
PROCESSED.mkdir(parents=True, exist_ok=True)

SELECTION = TRACKER / "selection"
SELECTION.mkdir(parents=True, exist_ok=True)

PROCESS_BATCH_SIZE = 20

TEMP = TRACKER / "temp"
TEMP.mkdir(parents=True, exist_ok=True)

ERROR = TRACKER / "error"
ERROR.mkdir(parents=True, exist_ok=True)

DOWNLOAD_FAILED = ERROR / "download_failed"
DOWNLOAD_FAILED.mkdir(parents=True, exist_ok=True)

PLAN_NOT_FOUND = ERROR / "plan_not_found"
PLAN_NOT_FOUND.mkdir(parents=True, exist_ok=True)

CORRUPTED_FILE = ERROR / "corrupted_file"
CORRUPTED_FILE.mkdir(parents=True, exist_ok=True)

FILE_TOO_LONG = ERROR / "file_too_long"
FILE_TOO_LONG.mkdir(parents=True, exist_ok=True)

TEXT_EXTRACTION_EMPTY = ERROR / "text_extraction_empty"
TEXT_EXTRACTION_EMPTY.mkdir(parents=True, exist_ok=True)

PDF_READER_FAILED = ERROR / "pdf_reader_failed"
PDF_READER_FAILED.mkdir(parents=True, exist_ok=True)

NUMBER_PAGES_TO_SAVE = 4

TERMS = [
    "Description of Plan",
    "Description Of Plan",
    "DESCRIPTION OF PLAN",
    "Description of the Plan",
    "Description Of The Plan",
    "DESCRIPTION OF THE PLAN",
    "Description of Plans",
    "Description of the Plans",
    "Plan Description",
    "PLAN DESCRIPTION",
    "SUMMARY OF SIGNIFICANT ACCOUNTING POLICIES",
    "DESCRIPTION OF THE 401(k) PENSION PLAN",
]

RECORDS = INDEX.parent / "records"
RECORDS.mkdir(parents=True, exist_ok=True)