# SPENSER: full pipeline on single machine

A set of scripts for running the entire SPENSE pipeline on a single machine.

## Install
Run [install_script.sh](../../submodules/install_script.sh) to set-up submodules and environment
to run pipeline.
```
./install_script.sh
```

## Pipeline
### SPENSER with 2011 LAD codes
- TODO: A single region can be run with [single_region.sh](single_region.sh)
- TODO: All regions can be run with [run_all_regions.sh](run_all_regions.sh):
```
./run_all_regions.sh
```


### Postprocessing
Run the postprocessing script to merge 2016 LADs into 2020 LADs:
```
python spenser_to_2020_lads.py --data_in <MICROSIMULATION_DATA_PATH> --data_out <MICROSIMULATION_DATA_PATH>
```

### Collation
Finally run [collation.sh](collation.sh) to reorganise the outputs for the SPC pipeline