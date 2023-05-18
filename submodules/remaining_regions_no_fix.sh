#!/bin/bash
eval "$(conda shell.bash hook)"
conda activate spenser_run
regions=(
    "E09000001" # City of London (E09000001)
    "E06000053" # Isle of Scilly (E06000053)
)

for region in "${regions[@]}"; do
    pueue add ./single_region.sh $region
done
