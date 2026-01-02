# Text extraction pipeline September 2025
## Text extraction pipeline built by John M Dedyo in 2025, based heavily on work by Antoine Arnoud available [here](https://github.com/antoinearnoud/pensions401k).

Progress is tracked by the "Tracker()" class, which keeps track of where the .txt files are corresponding to each ack_id to record their status at any point in the pipeline.

The user should only have to edit SETTINGS.py and fill the index files folder with the text files from the [DOL](https://www.askebsa.dol.gov/BulkFOIARequest/Listings.aspx/Index).

### TODO:

* Create shell script to generate records.
* Clean up index file creation.
* Create python and shell script to merge OCR text into universe_all.csv.

### Usage (Bouchet cluster)

1. Clone repo into your project directory.
2. Run the commands in `requirements.txt` to set up the conda environment.
3. Add an `index_files/dol_index_files` folder to the repo that contains the DoL index files. Include `merged_sh_h.dta` in the folder. Then run `python create_index_files.py`.
4. Edit `SETTINGS.py` so that all paths are correct for your device.
5. Open a terminal and navigate to the repo.
6. Run `bash tracker_setup.sh` to set up the progress tracking system (*make sure this is done in a scratch directory*).
7. Start running download jobs with `bash download_pdfs_range.sh NB_CPUS MIN_YEAR MAX_YEAR`.
8. Once many pdfs are downloaded, run processing jobs with `bash process_pdfs_range.sh NB_CPUS MIN_YEAR MAX_YEAR`.
9. After all pdfs are processed (check that no files remain in the queue by navigating to the tracker folder and running `ls queue/*/to_download | wc -l`) create the records csv files with `bash generate_records_range.sh MIN_YEAR MAX_YEAR`.
10. Finally, create csv files with all OCR-extracted text with `bash generate_ocr_csvs.sh MIN_YEAR MAX_YEAR`.