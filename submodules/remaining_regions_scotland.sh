#!/bin/bash
eval "$(conda shell.bash hook)"
conda activate spenser_run
regions=(
    "S12000005" "S12000006" "S12000008" "S12000010" "S12000011"
    "S12000013" "S12000014" "S12000017" "S12000018" "S12000019"
    "S12000020" "S12000021" "S12000023" "S12000026" "S12000027"
    "S12000028" "S12000029" "S12000030" "S12000033" "S12000034"
    "S12000035" "S12000036" "S12000038" "S12000039" "S12000040"
    "S12000041" "S12000042" "S12000045"
    "S12000015" "S12000024" # New regions: "S12000047", "S12000048"
    "S12000046"             # New region : "S12000049"
    "S12000044"             # New region : "S12000050"
)

for region in "${regions[@]}"; do
    pueue add ./single_region.sh $region
done
