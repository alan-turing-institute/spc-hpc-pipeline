#!/bin/sh
# Download (and install) all git repo's
sudo apt-get update
sudo apt install -y build-essential manpages-dev zip unzip

# install and setup miniconda
wget https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh -O ~/miniconda.sh
bash ~/miniconda.sh -b -p ~/miniconda
export PATH=~/miniconda/bin:$PATH
conda update -n base -c defaults conda
conda create -n spc_env -y python=3.9
conda init bash
source ~/.bashrc
conda activate spc_env

echo "Conda version $(conda --version)"

# Check if in conda env
if [ "$CONDA_DEFAULT_ENV" == "" ]; then
  echo "Error, no conda env activated"
  echo "This script will not work unless you are working in a conda environment"
  echo "Please set this up and retry"
  exit 1
fi

# We need this for UKCensusAPI and ukpopulation to work with Scottish census data
echo
echo -e "\e[31mInstalling 7zip\e[0m"
conda install --channel=conda-forge -y p7zip
echo
echo -e "\e[31mInstalling matplotlib\e[0m"
conda install -y matplotlib
echo

echo -e "\e[31mDownloading all SPENSER repo's from github and installing...\e[0m"

git clone -b master --single-branch https://github.com/ld-archer/UKCensusAPI.git

git clone -b master --single-branch https://github.com/ld-archer/ukpopulation.git

git clone https://github.com/virgesmith/humanleague.git

git clone -b arc --single-branch https://github.com/nismod/household_microsynth.git

git clone -b arc --single-branch https://github.com/nismod/microsimulation.git

export API_KEY="0x19c22fbcecbb026b54ad2a12192a4ad268fe8aae"

# Create caches and write the API key to where it is required
mkdir -p cache/
touch cache/NOMIS_API_KEY
echo $API_KEY > cache/NOMIS_API_KEY

mkdir -p household_microsynth/cache/
touch household_microsynth/cache/NOMIS_API_KEY
echo $API_KEY > household_microsynth/cache/NOMIS_API_KEY

mkdir -p microsimulation/cache/
touch microsimulation/cache/NOMIS_API_KEY
echo $API_KEY > microsimulation/cache/NOMIS_API_KEY

# Now create apikey.sh script for batch runs, and write in NOMIS_API_KEY
touch ~/apikey.sh
echo "export NOMIS_API_KEY=$API_KEY" > ~/apikey.sh
bash ~/apikey.sh

echo
echo -e "\e[31mAPI key copied to all the relevant directories\e[0m"
echo
echo -e "\e[31mCreating directories for successfully running the tests\e[0m"
echo

mkdir -p household_microsynth/data/
mkdir -p microsimulation/data/

echo
echo -e "\e[31mInstalling UKCensusAPI...\e[0m"
echo

cd UKCensusAPI
pip install -e .

echo
echo -e "\e[31mInstalling ukpopulation...\e[0m"
echo

cd ../ukpopulation
./setup.py install

echo
echo -e "\e[31mInstalling humanleague...\e[0m"
echo

cd ../humanleague
pip install -e .

echo
echo -e "\e[31mInstalling household_microsynth...\e[0m"
echo

cd ../household_microsynth
./setup.py install
# Make data directory if not already exists
mkdir -p data/

echo
echo -e "\e[31mInstalling microsimulation...\e[0m"
echo

cd ../microsimulation
./setup.py install
# Make data directory if not already exists
mkdir -p data/

echo
echo "SPENSER packages pulled and installed."


# Have to run household_microsynth for E09000001 (Ealing?) to produce data for
# microsimulation tests to pass
cd ../household_microsynth
scripts/run_microsynth.py $1 OA11
#
#echo
#echo "Now testing all packages"
#echo
#
#echo
#echo -e "\e[31mTesting UKCensusAPI...\e[0m"
#echo
#
#cd ../UKCensusAPI
#pytest
#
#echo
#echo -e "\e[31mTesting ukpopulation...\e[0m"
#echo
#
#cd ../ukpopulation
#./setup.py test
#
#echo
#echo -e "\e[31mTesting humanleague...\e[0m"
#echo
#
#cd ../humanleague
#pytest
#
#echo
#echo -e "\e[31mTesting household_microsynth...\e[0m"
#echo -e "\e[31mHave to run tests once first (that will fail) to download the correct zip file,\e[0m"
#echo -e "\e[31mthen we can unzip it and run tests again\e[0m"
#echo
#
#cd ../household_microsynth
#./setup.py test
#cd cache
#unzip Output_Area_blk.zip
#cd ..
#./setup.py test
#
#echo
#echo -e "\e[31mGenerating test data for microsimulation testing...\e[0m"
#echo
#
## Have to run household_microsynth for E09000001 (Ealing?) to produce data for
## microsimulation tests to pass
#scripts/run_microsynth.py $1 OA11
#
#echo
#echo -e "\e[31mTesting microsimulation...\e[0m"
#echo -e "\e[31mThis is weird because we have to run the tests twice for them to pass\e[0m"
#echo -e "\e[31mPossibly a timeout?\e[0m"
#echo
#
#cd ../microsimulation
#./setup.py test
#
#cd ..
#echo
#echo -e "\e[31mSetup complete!\e[0m"
#
