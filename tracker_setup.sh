#!/bin/bash
# run this file to set up the progress tracking system

echo "Running tracker setup"

sbatch_script="tracker_setup.sbatch"  # Change this to the name of your SBATCH script template; will overwrite any job name in the sbatch file

job_name="tracker_setup"  # Change this to your desired job name

sbatch --job-name="$job_name" "$sbatch_script"

echo "tracker setup job submitted successfully."