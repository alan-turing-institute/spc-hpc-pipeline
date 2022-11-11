#!/bin/sh
# Download (and install) all git repo's

git clone -b arc --single-branch https://github.com/nismod/household_microsynth.git


cd household_microsynth
cat README.md > output.txt
