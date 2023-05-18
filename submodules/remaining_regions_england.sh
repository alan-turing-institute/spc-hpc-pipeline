#!/bin/bash
eval "$(conda shell.bash hook)"
conda activate spenser_run
regions=(
    "E07000048" "E06000028" "E06000029" # Dorset: "E06000058"
    "E07000049" "E07000050" "E07000051" "E07000052" # Dorset: "E06000059"
    "E07000004" "E07000005" "E07000006" "E07000007" # Buckinghamshire incomplete (E06000060)
    "E07000135" # Leicestershire incomplete (E07000135) - fixed with larger maxiter in IPF.h of human league
    "E07000205" "E07000206" # Suffolk incomplete (E07000244 & E07000245): E07000244
    "E07000201" "E07000204" # Suffolk incomplete (E07000244 & E07000245): E07000245
    "E07000190" "E07000191" # Somerset incomplete (E07000246)
)

for region in "${regions[@]}"; do
    pueue add ./single_region.sh $region
done
