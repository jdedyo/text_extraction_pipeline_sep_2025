# Text extraction pipeline September 2025
## Text extraction pipeline built by John M Dedyo in 2025, based heavily on work by Antoine Arnoud available [here](https://github.com/antoinearnoud/pensions401k).

Progress is tracked by the "Tracker()" class, which keeps track of where the .txt files are corresponding to each ack_id to record their status at any point in the pipeline.

The user should only have to edit SETTINGS.py and fill the index files folder with the text files from the [DOL](https://www.askebsa.dol.gov/BulkFOIARequest/Listings.aspx/Index).

### TODO:

* Create full setup script
* Implement pdf processing
* Create "crawler" script that will create a database based on the .txt file locations
