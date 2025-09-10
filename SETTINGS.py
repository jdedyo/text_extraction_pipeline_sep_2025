from pathlib import Path

INDEX = Path("/nfs/roberts/project/pi_co337/jmd324/text_extraction_pipeline_sep_2025/index_files/universe")
if not INDEX.exists():
    raise FileNotFoundError(f"Index files path does not exist: {INDEX}")


TRACKER = Path.home() / "scratch_pi_co337/jmd324/text_pipeline_tracker"
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

PROCESS_BATCH_SIZE = 900


ERROR = TRACKER / "error"
ERROR.mkdir(parents=True, exist_ok=True)

DOWNLOAD_FAILED = ERROR / "download_failed"