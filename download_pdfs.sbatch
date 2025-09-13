#!/bin/bash
#SBATCH --job-name=download_pdfs
#SBATCH --mem-per-cpu=4G
#SBATCH --time=8:00:00
#SBATCH --output=logs/download_pdfs/%x-%j.log
#SBATCH --open-mode=append

module purge
module load miniconda
conda activate text_extraction_sep_2025

export PYTHONUNBUFFERED=1
python download_pdfs.py --year $1