#!/bin/bash

set -e

# Check inputs are provided
if [[ -z $1 ]]; then
    echo "Please enter a conda environment name to complete installation."
fi
if [[ -z $2 ]]; then
    echo "Please enter a LAD."
fi

CONDA_SPENSER=$1
var=$2

eval "$(conda shell.bash hook)"
conda activate $CONDA_SPENSER
conda info --env


# Have to run household_microsynth for LAD to produce data for
# microsimulation tests to pass
cd submodules/household_microsynth

# NO_CACHE is used to ensure the correct resolution is retrieved for Scotland.
# See [UKCensusAPI](https://github.com/alan-turing-institute/UKCensusAPI/commit/61bf3ee667a5be4c6a91afe4d13415f85d35ca0b).
#
# For household microsimulation it can be set to false, while for individual
# microsimulation (Step 1 and Step 2), it needs to be set to NO_CACHE=true for
# successful run for Scotland.

export NO_CACHE=false
python -W ignore scripts/run_microsynth.py $var OA11

echo 'Moving to run microsimulation'
cd ..

echo 'Step 1'
cd microsimulation

# Set NO_CACHE=true so given above comment.
# England and Wales may use NO_CACHE=false throughout but no difference occurs
# with setting as NO_CACHE=true. So it is set here so that all England, Wales and
# Scotland may run with single script.
export NO_CACHE=true
python -W ignore scripts/run_ssm.py -c config/ssm_current.json $var

echo 'Step 2'
python -W ignore scripts/run_ssm_h.py -c config/ssm_h_current.json $var

echo 'Running assigment for 2012'
python -W ignore scripts/run_assignment.py -c config/ass_current_2012.json $var

echo 'Running assigment for 2020'
python -W ignore  scripts/run_assignment.py -c config/ass_current_2020.json $var

echo 'Running assigment for 2022'
python -W ignore scripts/run_assignment.py -c config/ass_current_2022.json $var

echo 'Running assigment for 2032'
python -W ignore scripts/run_assignment.py -c config/ass_current_2032.json $var

echo 'Running assigment for 2039'
python -W ignore scripts/run_assignment.py -c config/ass_current_2039.json $var

echo "Done with: $var"

now=$(date +"%T")
echo "Current time : $now"
cd .. 
