# new_scripts
These scripts are for processing more recent (2013-14 forward) iterations of the IRS county to county and state to state migration files. 

- Place and unzip new files in the source_data folder for processing. Remove any existing data files (i.e. don't keep older stuff in git folders)
- Files are processed one year and geography at a time
- Output is written to a test database where output can be inspected
- If output looks good, the sqlite_to_sqlite_irs python script in the sources folder can be run to transfer tables out of the test database and into the permanent one



