#!/bin/bash
# run this file to download pdfs
# bash grace_run_jobs_pdf_download.sh NB_CPUS 
# NB_CPUS can be e.g. 200 or more
# e.g. bash download_pdfs.sh 200
# IF NB_CPUS > 50, it will submit using --array directive to avoid being killed by the scheduler

#cpus=1

# Check if at least one argument is provided
if [ $# -lt 2 ]; then
    echo "Error: Please provide number of cpus in the first argument and year (YYYY) to run in the second."
    exit 1
fi

if [ $# -gt 1 ]; then
    cpus=$1
    year=$2
    # $1 means first argument passed in the command line
fi

echo ">> Running $cpus jobs for year $year"

sbatch_script="download_pdfs.sbatch"  # Change this to the name of your SBATCH script template; will overwrite any job name in the sbatch file

if [ "$cpus" -gt 50 ]; then
    # submit using --array directive to avoid being killed by the scheduler if more than 50 jobs
    sbatch --array=1-$cpus --job-name="download_pdfs_$year" "$sbatch_script" $year
else
    # loop over number of cpus
    for i in $(seq 1 $cpus);
        do
        # this should be submitted from a "clean" environment; no loading of environment
        #sbatch grace_task_pdf_download.sbatch $sample
        #sbatch --export=SAMPLE=$sample grace_task_pdf_download.sbatch

        job_name="download_pdfs_$i"  # Change this to your desired job name

        # Use sbatch to submit the job
        sbatch --job-name="$job_name" "$sbatch_script" $year

        done
fi

echo "$cpus jobs submitted successfully."