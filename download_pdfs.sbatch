#!/bin/bash
#SBATCH --job-name=download_pdfs
#SBATCH --mem-per-cpu=4G
#SBATCH --time=8:00:00
#SBATCH --output=logs/download_pdfs/%x-%j.log

module purge
source .venv/bin/activate


python download_pdfs.py --year $1