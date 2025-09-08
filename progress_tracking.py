# %%
import os
from pathlib import Path
from SETTINGS import *

def same_mount(path1: str | Path, path2: str | Path) -> bool:
    """
    Return True if path1 and path2 are on the same mount (same st_dev).
    """
    path1, path2 = Path(path1), Path(path2)
    return os.stat(path1).st_dev == os.stat(path2).st_dev
