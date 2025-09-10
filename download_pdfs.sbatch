#!/bin/bash
#SBATCH --job-name=download_pdfs
#SBATCH --mem-per-cpu=4G
#SBATCH --time=8:00:00
#SBATCH --output=logs/%x-%j.log

module purge
source /nfs/roberts/project/pi_co337/jmd324/rebuilding_text_extraction_pipeline_sep_2025/.venv/bin/activate


python download_pdfs.py --year $1