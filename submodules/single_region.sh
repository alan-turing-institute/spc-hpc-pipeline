#!/bin/bash
eval "$(conda shell.bash hook)"
conda activate spenser_run
conda info --env

var=$1

set -e

# Have to run household_microsynth for LAD to produce data for
# microsimulation tests to pass
cd household_microsynth
NO_CACHE=false python -W ignore scripts/run_microsynth.py $var OA11

echo 'Moving to run microsimulation'
cd ..

echo 'Step 1'
cd microsimulation
NO_CACHE=true python -W ignore scripts/run_ssm.py -c config/ssm_current.json $var

echo 'Step 2'
NO_CACHE=true python -W ignore scripts/run_ssm_h.py -c config/ssm_h_current.json $var

echo 'Running assigment for 2012'
NO_CACHE=true python -W ignore scripts/run_assignment.py -c config/ass_current_2012.json $var

echo 'Running assigment for 2020'
NO_CACHE=true python -W ignore  scripts/run_assignment.py -c config/ass_current_2020.json $var

echo 'Running assigment for 2022'
NO_CACHE=true python -W ignore scripts/run_assignment.py -c config/ass_current_2022.json $var

echo 'Running assigment for 2032'
NO_CACHE=true python -W ignore scripts/run_assignment.py -c config/ass_current_2032.json $var

echo 'Running assigment for 2039'
NO_CACHE=true python -W ignore scripts/run_assignment.py -c config/ass_current_2039.json $var

echo "Done with: $var"

now=$(date +"%T")
echo "Current time : $now"
cd .. 
