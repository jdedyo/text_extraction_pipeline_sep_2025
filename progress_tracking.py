import os
from pathlib import Path
from itertools import islice
import errno
import random
from SETTINGS import *

def same_mount(path1: str | Path, path2: str | Path) -> bool:
    """
    Return True if path1 and path2 are on the same mount (same st_dev).
    """
    path1, path2 = Path(path1), Path(path2)
    return os.stat(path1).st_dev == os.stat(path2).st_dev

def _safe_claim_then_move(src: Path, dst: Path) -> bool:
    """
    Atomically claim by renaming within the source dir to a unique temp name,
    then move to destination. Returns True on success, False if raced.
    """

    # 1) Claim inside source dir
    temp = src.with_suffix(src.suffix + f".claiming.{os.getpid()}.{random.randint(0, 1_000_000)}")
    try:
        os.rename(src, temp)
    except FileNotFoundError:
        return False  # raced
    except OSError as e:
        if e.errno in (errno.ENOENT, errno.EEXIST, getattr(errno, "ESTALE", -1)):
            return False
        raise

    # 2) Move claimed token to destination
    try:
        try:
            os.rename(temp, dst)
        except OSError as e:
            if e.errno == errno.EXDEV:
                with open(temp, "rb") as f:
                    data = f.read()
                with open(dst, "wb") as f:
                    f.write(data)
                os.unlink(temp)
            else:
                raise
    except Exception:
        # Try to put it back so it can be reclaimed later
        try:
            os.rename(temp, src)
        except Exception:
            # If this also fails, leave temp
            pass
        raise

    return True

def claim_n(src_root: Path, dst_root: Path, year: str, n: int):
    """
    Claim up to n files from the given year subfolder under src_root,
    moving each into the matching year subfolder under dst_root
    via _safe_claim_then_move (atomic same-dir claim; cross-FS safe move).
    Returns list of destination Paths.
    """
    claimed: list[Path] = []

    year = str(year)

    # Locate year directory
    yd = src_root / year
    if not yd.is_dir():
        return claimed  # nothing to claim

    # Ensure destination year dir exists
    dstdir = dst_root / year
    dstdir.mkdir(parents=True, exist_ok=True)

    try:
        # Stream entries; do not build a list
        with os.scandir(yd) as entries:
            for fe in entries:
                if len(claimed) >= n:
                    break

                # only regular files; ignore dirs
                if not fe.is_file(follow_symlinks=False):
                    continue
                # skip temporary in-progress markers
                if ".claiming." in fe.name:
                    continue

                src = Path(fe.path)
                dst = dstdir / fe.name

                # Try to claim+move
                if _safe_claim_then_move(src, dst):
                    claimed.append(dst)

    except FileNotFoundError:
        # The year dir vanished. Treat as nothing to claim
        pass

    return claimed

def get_ack_id_year_from_path(p: Path):
    """
    Extracts (ack_id, year) as strings from the .txt tracker filepath.
    """
    ack_id, year = p.stem, p.parent.name
    return ack_id, year

def unclaim_all(src_root: Path, dst_root: Path):
    unclaimed: list[Path] = []

    if not src_root.is_dir():
        return unclaimed

    # Walk only one level of year dirs under src_root
    for year_dir in src_root.iterdir():
        if not year_dir.is_dir():
            continue

        year = year_dir.name
        if not (len(year_dir.name) == 4 and year_dir.name.isdigit()):
            raise Exception(f"Non-year directory found at {year_dir}.")

        dstdir = dst_root / year
        dstdir.mkdir(parents=True, exist_ok=True)

        with os.scandir(year_dir) as entries:
            for fe in entries:
                if not fe.is_file(follow_symlinks=False):
                    continue
                if ".claiming." in fe.name:  # skip temp markers
                    continue

                src = Path(fe.path)
                dst = dstdir / fe.name

                if dst.exists():
                    raise FileExistsError(f"Destination already exists: {dst}")

                src.rename(dst)
                unclaimed.append(dst)
        
        try:
            year_dir.rmdir()
        except OSError:
            # Not empty or some other issue. Ignore
            pass

    return unclaimed

def clean_queue_filenames(queue: Path):
    """
    Recursively finds all files in the given folder whose names contain `.txt`
    followed by extra text, and renames them to stop at `.txt`.

    Parameters
    ----------
    folder : str
        Path to the root folder to scan.
    """
    folder_path = Path(queue)

    for file_path in folder_path.rglob("*"):
        if file_path.is_file() and ".txt" in file_path.name:
            name_before_txt, sep, _ = file_path.name.partition(".txt")
            new_name = f"{name_before_txt}{sep}"  # keep '.txt'
            new_path = file_path.with_name(new_name)

            if new_path.exists():
                # print(f"Skipping {file_path} (target {new_name} already exists)")
                pass
            else:
                print(f"Renaming {file_path} -> {new_name}")
                file_path.rename(new_path)