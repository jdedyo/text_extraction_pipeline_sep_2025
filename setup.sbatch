#!/bin/bash
#SBATCH --job-name=setup
#SBATCH --mem-per-cpu=16G
#SBATCH --cpus-per-task=2
#SBATCH --time=2:00:00
#SBATCH --output=logs/setup/%x-%j.log
#SBATCH --partition=day

module purge
module load miniconda
conda activate text_extraction_sep_2025

echo "Setting up index files..."
export PYTHONUNBUFFERED=1
python create_index_files.py
echo "Index files created!"

echo "Submitting tracker setup job..."
bash tracker_setup.sh
echo "Tracker setup job submitted!"