#!/bin/bash
#SBATCH --job-name=process_pdfs
#SBATCH --mem-per-cpu=8G
#SBATCH --time=8:00:00
#SBATCH --output=logs/process_pdfs/%x-%j.log
#SBATCH --open-mode=append

module purge
module load miniconda
module load poppler/22.12.0-GCC-12.2.0
conda activate text_extraction_sep_2025

export PYTHONUNBUFFERED=1
python process_pdfs.py --year "$1"