# %%
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

def _safe_claim_then_move(src_path: str, dst_path: str) -> bool:
    """
    Atomically claim by renaming within the source dir to a unique temp name,
    then move to destination. Returns True on success, False if raced.
    """
    src = Path(src_path)
    dst = Path(dst_path)
    # 1) Claim inside source dir (always same-dir, atomic)
    temp = src.with_suffix(src.suffix + f".claiming.{os.getpid()}.{random.randint(0, 1_000_000)}")
    try:
        os.rename(src, temp)  # atomic: only one worker wins
    except FileNotFoundError:
        return False  # raced
    except OSError as e:
        if e.errno in (errno.ENOENT, errno.EEXIST, getattr(errno, "ESTALE", -1)):
            return False
        raise

    # 2) Move claimed token to destination (may cross dirs; handle EXDEV)
    try:
        try:
            os.rename(temp, dst)   # atomic if same FS
        except OSError as e:
            if e.errno == errno.EXDEV:
                # cross-device: fall back to replace semantics
                # (token files are tiny, so copy+unlink is fine)
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
            # If this also fails, leave temp; a janitor can clean *.claiming.*
            pass
        raise

    return True

def claim_n_any_year(src_root: Path, dst_root: Path, n: int, shuffle: bool = True):
    """
    Claim up to n files from any year subfolder under src_root,
    moving each into the matching year subfolder under dst_root
    via _safe_claim_then_move (atomic same-dir claim; cross-FS safe move).
    Returns list of destination Paths.
    """
    dst_root.mkdir(parents=True, exist_ok=True)
    claimed = []

    # Enumerate year directories quickly
    with os.scandir(src_root) as it:
        year_dirs = [e for e in it if e.is_dir(follow_symlinks=False)]

    if not year_dirs:
        return claimed
    if shuffle:
        random.shuffle(year_dirs)

    dst_cache: dict[str, Path] = {}

    while len(claimed) < n and year_dirs:
        progress = False

        for yd in year_dirs:
            if len(claimed) >= n:
                break

            year = yd.name

            # Ensure destination year dir exists (cached)
            dstdir = dst_cache.get(year)
            if dstdir is None:
                dstdir = dst_root / year
                dstdir.mkdir(parents=True, exist_ok=True)
                dst_cache[year] = dstdir

            # Scan this year directory for one claimable file
            try:
                with os.scandir(yd.path) as files:
                    for fe in files:
                        # only regular files; ignore dirs/symlinks
                        if not fe.is_file(follow_symlinks=False):
                            continue
                        # skip temporary in-progress markers
                        if ".claiming." in fe.name:
                            continue

                        dst = dstdir / fe.name
                        # Try to claim+move; returns True if we won the race
                        if _safe_claim_then_move(fe.path, str(dst)):
                            claimed.append(dst)
                            progress = True
                            break  # round-robin: move to next year
            except FileNotFoundError:
                # The year dir vanished; skip it
                continue

        if not progress:
            # Nothing left to claim across all year dirs
            break

    return claimed

def get_ack_id_year_from_path(p: Path):
    """
    Extracts (ack_id, year) as strings from the .txt tracker filepath.
    """
    ack_id, year = p.stem, p.parent.name
    return ack_id, year


# def unclaim_to(src_file: Path, dst_dir: Path):
#     """
#     Move an individual ack_id file from src_file into dst_dir.
#     Keeps the filename (e.g. 1234567.txt).
#     """
#     dst_dir.mkdir(parents=True, exist_ok=True)  # make sure the folder exists
#     dst = dst_dir / src_file.name
#     src_file.rename(dst)
#     return dst

def unclaim_to(src_file: Path, dst_root: Path):
    """
    Move an ack_id file from one root (e.g. CLAIMED) to another (e.g. TO_PROCESS, ERROR, PROCESSED),
    preserving the year subfolder.
    """
    year = src_file.parent.name        # e.g. "2003"
    dst_dir = dst_root / year
    dst_dir.mkdir(parents=True, exist_ok=True)

    dst_file = dst_dir / src_file.name
    src_file.rename(dst_file)          # atomic on same filesystem
    return dst_file

# %%
