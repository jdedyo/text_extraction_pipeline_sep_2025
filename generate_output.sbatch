#!/bin/bash
#SBATCH --job-name=generate_output
#SBATCH --mem-per-cpu=16G
#SBATCH --cpus-per-task=2
#SBATCH --time=6:00:00
#SBATCH --output=logs/generate_output/%x-%j.log
#SBATCH --partition=day

module purge
module load miniconda
conda activate text_extraction_sep_2025

echo "Creating metadata (waterfall) files and OCR csvs..."
export PYTHONUNBUFFERED=1
python generate_records.py
echo "Metadata and OCR csv files created!"

echo "Creating OOS chunks and training data..."
export PYTHONUNBUFFERED=1
python generate_all_ocr_train_and_oos.py
echo "OOS chunks and training data created!"