#!/bin/bash

if [[ -z $1 ]]; then
    echo "Please enter a conda environment to use."
fi

CONDA_SPENSER=$1

# List of all GB LADs from: submodules/microsimulation/lad_array_grouped10.sh
while read lad; do
  lads+=("${lad}")
done < <(tail -n +2 data/spenser_lad_list.csv)

for lad in "${lads[@]}"; do
    pueue add -- ./scripts/full_pipeline/single_lad.sh $CONDA_SPENSER $lad
done
