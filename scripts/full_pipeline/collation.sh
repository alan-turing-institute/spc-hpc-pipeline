#!/bin/bash

# Copies microsimulation output to region/year/./ file structure for SPENSER
# outputs.

# Args:
#  -s <PATH>: Source path of microsimulation data to collate
#  -t <PATH>: Destination (target) path to copy data to
#  -d       : Dry run flag.

dryrun='false'
while getopts 'ds:t:' flag; do
  case "${flag}" in
    d) dryrun='true' ;;
	s) SOURCE="${OPTARG}" ;;
	t) DESTINATION="${OPTARG}" ;;
    *) error "Unexpected option ${flag}" ;;
  esac
done

for region in "England" "Wales" "Scotland"; do
    for year in "2012" "2020" "2022" "2032" "2039"; do
	path=$DESTINATION/$region/$year/
	echo $path
	if [[ $dryrun == 'true' ]]; then
		rsync --dry-run -avu --progress \
			$SOURCE/microsimulation/data/*_${region:0:1}*_$year.csv \
			$path/./
	else
		mkdir -p $path
		rsync -vu --progress \
			$SOURCE/microsimulation/data/*_${region:0:1}*_$year.csv \
			$path/./
	fi
    done
done
