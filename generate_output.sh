#!/bin/bash
# run this file to set up the index files and progress tracking system

echo "Creating final pipeline output files..."

sbatch_script="generate_output.sbatch"  # Change this to the name of your SBATCH script template; will overwrite any job name in the sbatch file

job_name="generate_output"  # Change this to your desired job name

sbatch --job-name="$job_name" "$sbatch_script"

echo "generate_output job submitted successfully."