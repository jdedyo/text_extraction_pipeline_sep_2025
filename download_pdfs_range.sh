#!/bin/bash
# run this file to download pdfs
# bash download_pdfs_range.sh NB_CPUS MIN_YEAR MAX_YEAR
# NB_CPUS can be e.g. 200 or more
# e.g. bash download_pdfs_range.sh 200 2010 2015
# IF NB_CPUS > 50, it will submit using --array directive to avoid being killed by the scheduler

# Check if at least three arguments are provided
if [ $# -lt 3 ]; then
    echo "Error: Please provide number of cpus in the first argument, min year in second, and max year in third (YYYY)."
    exit 1
fi

cpus=$1
minyear=$2
maxyear=$3

echo ">> Running $cpus jobs for years between $minyear and $maxyear"

# Loop over years
for (( y=$minyear; y<=$maxyear; y++ ))
do
    echo "Submitting job for year $y"
    bash download_pdfs_one_year.sh "$cpus" "$y"
done