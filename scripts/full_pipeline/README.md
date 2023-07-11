# SPENSER: full pipeline on single machine

A set of scripts for running the entire SPENSER pipeline on a single machine.

## Install
Run [install_script.sh](install.sh) to set-up submodules
and environment to run pipeline from repo root.to run pipeline from repo root.
```bash
./scripts/full_pipeline/install.sh
```
Installation assumes conda is installed for creating the new virtual
environment.

## Pipeline
### SPENSER with 2011 LAD codes
A single region can be run from the repo root with
[single_region.sh](single_region.sh):
```bash
./scripts/full_pipeline/single_region.sh <A_SINGLE_LAD>
```

All regions can be run with [run_all_regions.sh](run_all_regions.sh) from the
repo root:
```bash
./scripts/full_pipeline/run_all_regions.sh
```
This script requires [pueue](https://github.com/Nukesor/pueue) to be installed.
Running for all regions on a single core will take several weeks.


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
    <MICROSIMULATION_DATA_PATH> \
    <COLLATED_SPENSER_PATH>
```

### Test
A final check that all regions are covered may be performed with:
```bash
python scripts/postprocessing/final_check.py \
    <LIST_COLLATED_SPENSER_PATHS>
```