#!/bin/bash

set -e

if [[ -z $1 ]]; then
    echo "Please enter a conda environment name to complete installation."
fi

CONDA_SPENSER=$1

# Set-up conda env
eval "$(conda shell.bash hook)"
git submodule update --init --recursive
conda create --name $CONDA_SPENSER python=3.9 -y
conda activate $CONDA_SPENSER
if [[ "$(conda info --json | jq -r ".active_prefix_name")" != "$CONDA_SPENSER" ]]; then
    echo "${CONDA_SPENSER} is not activated. Please retry."
    exit 1
fi

# Installs for all OS
conda update -n base -c defaults conda -y
conda install python=3.9 pip cython matplotlib pandas -y
conda install -c conda-forge p7zip -y

# OS specific installs
os_name=`uname -s`
echo "OS name: ${os_name}"
if [[ $os_name == 'Linux' ]]; then
    conda install -c conda-forge gxx -y
elif [[ $os_name == 'Darwin' ]]; then
    # MacOS install in specific order, otherwise install fails
    pip install pandas==1.2.4
    pip install msgpack==0.6.2
    STATIC_DEPS=true pip install lxml==4.6.5
else
    echo "${os_name} not recognised."
    exit 1
fi

# Copy config
mkdir -p microsimulation/config
cp -a ../scripts/scp/ssm_current.json microsimulation/config/
cp -a ../scripts/scp/ssm_h_current.json microsimulation/config/
cp -a ../scripts/scp/ass_current*.json microsimulation/config/

# Make output data paths
mkdir -p household_microsynth/data/
mkdir -p microsimulation/data/

# Set up NOMIS_API_KEY files
if [[ -z $NOMIS_API_KEY ]]; then 
    echo "Environment variable NOMIS_API_KEY is not set. Set NOMIS_API_KEY and retry."
else 
    echo $NOMIS_API_KEY > NOMIS_API_KEY.txt
    mkdir -p cache/
    touch cache/NOMIS_API_KEY
    echo $API_KEY > cache/NOMIS_API_KEY
    mkdir -p household_microsynth/cache/
    echo $API_KEY > household_microsynth/cache/NOMIS_API_KEY
    mkdir -p microsimulation/cache/
    touch microsimulation/cache/NOMIS_API_KEY
    echo $API_KEY > microsimulation/cache/NOMIS_API_KEY
fi

# Install submodules
cd UKCensusAPI
pip install -e .
cd ../ukpopulation
./setup.py install
cd ../humanleague
pip install -e .
cd ../household_microsynth
./setup.py install
mkdir -p data/
cd ../microsimulation
./setup.py install
cd ..

# Test
# ./setup.py test
