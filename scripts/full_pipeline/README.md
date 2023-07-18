# SPENSER: full pipeline on single machine

A set of scripts for running the entire SPENSER pipeline on a single machine.

## Install
### Prerequisites
The following scripts assume the following have been installed:
- [conda](https://docs.conda.io/en/latest/miniconda.html): for installation and
  environments
- [pueue](https://github.com/Nukesor/pueue): a process queue for running all
  LADs
- [Nomisweb API key](../../README.md#setting-up-your-nomis-api-key): a Nomisweb
  API key is required as an environment variable `$API_KEY` for successful
  installation

### Submodule and environment set-up
Run [install_script.sh](install.sh) to set-up submodules and environment to run
pipeline from repo root.
```bash
./scripts/full_pipeline/install.sh
```
Installation assumes conda is installed for creating the new virtual
environment.

## Pipeline
### SPENSER with 2011 LAD codes
A single region can be run from the repo root with
[single_lad.sh](single_lad.sh):
```bash
./scripts/full_pipeline/single_lad.sh <LAD>
```

All LADs can be run with [run_all_lads.sh](run_all_lads.sh) from the
repo root:
```bash
./scripts/full_pipeline/run_all_lads.sh
```
This script requires [pueue](https://github.com/Nukesor/pueue) to be installed.
Running for all 380 LADs on a single core will take several weeks.


### Postprocessing
Run the postprocessing script to merge 2011 LADs into 2020 LADs:
```bash
python scripts/postprocessing/spenser_to_2020_lads.py \
    --data_in <MICROSIMULATION_DATA_PATH> \
    --data_out <MICROSIMULATION_DATA_PATH>
```

### Collation
Finally run [collation.sh](collation.sh) to reorganise the outputs for the SPC:
```bash
./scripts/full_pipeline/collation.sh \
    -s <MICROSIMULATION_DATA_PATH> \
    -t <COLLATED_SPENSER_PATH> \
    -d # dry-run flag
```

### Test
A final check that all regions are covered may be performed with:
```bash
python scripts/postprocessing/final_check.py \
    --paths <COLLATED_SPENSER_PATHS>
```