#!/bin/bash
# run this file to set up the index files and progress tracking system

echo "Setting up index files and tracker..."

sbatch_script="setup.sbatch"  # Change this to the name of your SBATCH script template; will overwrite any job name in the sbatch file

job_name="setup"  # Change this to your desired job name

sbatch --job-name="$job_name" "$sbatch_script"

echo "setup job submitted successfully."