#!/bin/bash

# set -e

SOURCE=$1
DESTINATION=$2


for region in "England" "Wales" "Scotland"; do
    for year in "2012" "2020" "2022" "2032" "2039"; do
	path=$DESTINATION/$region/$year/
	echo $path
	mkdir -p $path
	# rsync --dry-run -avu --progress $SOURCE/microsimulation/data/*_${region:0:1}*_$year.csv $path/./
	rsync -vu --progress $SOURCE/microsimulation/data/*_${region:0:1}*_$year.csv $path/./
    done
done
