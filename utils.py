import os
import subprocess
import pandas as pd

def slurm_time_remaining():
    """
    Returns remaining walltime as a pandas.Timedelta,
    or None if not running inside a Slurm job.
    """
    job_id = os.environ.get("SLURM_JOB_ID")
    if job_id is None:
        return None

    # Query Slurm for remaining time (D-HH:MM:SS, HH:MM:SS, or MM:SS)
    result = subprocess.run(
        ["squeue", "-j", job_id, "--noheader", "--format=%L"],
        capture_output=True, text=True
    )
    time_left_str = result.stdout.strip()
    if not time_left_str:
        return None

    # Let pandas parse it into a Timedelta
    td = pd.to_timedelta(time_left_str)
    return td

# Example usage
td = slurm_time_remaining()
if td is not None:
    print(f"Remaining time: {td} ({td.total_seconds():.0f} seconds)")
else:
    print("Not running inside a Slurm job.")