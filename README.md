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
2. *On a compute node* `conda_setup.sh` to set up the conda environment. You may need to edit this for your platform.
3. Edit `SETTINGS.py` so that all paths are correct for your device.
4. Add an `index_files/dol_index_files` folder to the repo that contains the DoL index files. Include your own index file in the folder (`MERGE_INDEX` in `SETTINGS.py`). Then run `bash setup.sh`.
5. Open a terminal and navigate to the repo.
<!-- 6. Run `bash tracker_setup.sh` to set up the progress tracking system (*make sure this is done in a scratch directory*). -->
6. Start running download jobs with `bash download_pdfs_range.sh NB_CPUS MIN_YEAR MAX_YEAR`.
7. Once many pdfs are downloaded, run processing jobs with `bash process_pdfs_range.sh NB_CPUS MIN_YEAR MAX_YEAR`.
8. After all pdfs are processed (check that no files remain in the queue by navigating to the tracker folder and running `ls queue/*/to_download | wc -l`) create the metadata and final ocr csvs by running `bash generate_output.sh`.



### Remarks

1. Make sure to run enough CPUs that the downloading/processing for the year is done before the jobs are cancelled, or else some manual cleanup is required.
2. You can create csv files with all OCR-extracted text for a subset of years with `bash generate_ocr_csvs.sh MIN_YEAR MAX_YEAR`.
3. `tracker_setup.py` takes an optional `--year` argument, so it can be run for a single year (if the dataset expands, e.g.).
4.  with `bash generate_records_range.sh MIN_YEAR MAX_YEAR`.