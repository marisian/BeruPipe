# BeruPipe
An upstream data parsing pipeline designed to remove XML structure and boilerplate from raw BERUFENET database extracts. Creates a simple data model that links description texts of multiple occupational dimensions to occupational and temporal metadata. Downstream, the data model can be used for text mining and feature extraction of BERUFENET data.

## Settings
Set paths and parameters in src/config.py. It is usually enough to only set the 'base_dir' and provide 
the correct input data in the respective directories.

## Input data
The modules of this program create several data objects from raw input data. The following input data is required for 
the scripts to run:
* XML files extracted from the BERUFENET database (1 file = 1 occupation-year)
* An XML file with occupation meta-attributes

## Scripts
* *parse_raw_data.py* is the core XML parsing script that is applied to the single-occupation XML files containing the BERUFENET information fields
* *parse_raw_meta.py* is the core XML parsing script that is applied to the cross-sectional files containing dkz-level attributes
* *transform_data.py* is the main preprocessing script that applies all text-level transformations to the data
* *run_sanity_checks.py* runs all plausibility checks on intermediate and final data object content in one go

## Output data
The main outputs of this program are data objects with transformed BERUFENET texts
of config-specified information fields. Additionally, a metadata file with codes and other attributes at constant occupation level 
is produced.

## Setup and use
Run *main.py* from console to run all relevant preprocessing scripts automatically.

Each script can be run on its own. 

The script *run_sanity_checks.py* is not included in *main.py* (but sanity checks are run 
automatically in each preprocessing step).
