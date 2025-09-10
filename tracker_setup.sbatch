#!/bin/bash
#SBATCH --job-name=tracker_setup
#SBATCH --mem-per-cpu=8G
#SBATCH --cpus-per-task=2
#SBATCH --time=8:00:00
#SBATCH --output=logs/tracker_setup/%x-%j.log
#SBATCH --partition=day

module purge
source .venv/bin/activate

python tracker_setup.py