#!/bin/bash
#SBATCH --job-name=generate_output
#SBATCH --mem-per-cpu=16G
#SBATCH --cpus-per-task=2
#SBATCH --time=2:00:00
#SBATCH --output=logs/generate_output/%x-%j.log
#SBATCH --partition=day