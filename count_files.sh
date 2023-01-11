#!/bin/bash

# This script recursively counts files under each folder in the current directory,
# and throws a warning if the count is different than the input argument.
arg=$1
for directory in `ls -d */`; do
    count=$(find $directory -type f | wc -l)
    if [ $count -ne $arg ]; then
        echo "WARNING: $directory has $count files, which is different than $arg"
    fi
done