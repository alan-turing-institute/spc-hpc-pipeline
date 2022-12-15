#!/bin/sh

now=$(date +"%T")
echo "Current time : $now"

# Download (and install) all git repo's
sudo apt-get update
sudo apt install -y build-essential manpages-dev zip unzip

if [! -d "~/miniconda"]
then
  # install and setup miniconda
  wget https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh -O ~/miniconda.sh
  bash ~/miniconda.sh -b -p ~/miniconda
fi

export PATH=~/miniconda/bin:$PATH
conda update -n base -c defaults conda -y
conda install python=3.9 pip -y
conda install cython -y 
conda init bash
source ~/.bashrc

# We need this for UKCensusAPI and ukpopulation to work with Scottish census data
echo
echo -e "\e[31mInstalling 7zip\e[0m"
conda install --channel=conda-forge -y p7zip
echo
echo -e "\e[31mInstalling matplotlib\e[0m"
conda install -y matplotlib
echo

echo -e "\e[31mDownloading all SPENSER repo's from github and installing...\e[0m"

git clone -b master --single-branch https://github.com/alan-turing-institute/UKCensusAPI

git clone -b master --single-branch https://github.com/ld-archer/ukpopulation.git

git clone https://github.com/virgesmith/humanleague.git

git clone -b fix/NoneType --single-branch https://github.com/alan-turing-institute/household_microsynth/

git clone -b fix/double_run --single-branch https://github.com/alan-turing-institute/microsimulation.git

export API_KEY=`cat NOMIS_API_KEY.txt`

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
echo -e "\e[31mTesting household_microsynth...\e[0m"
echo

cd ../household_microsynth
./setup.py test
echo
echo "SPENSER packages pulled and installed."


# Have to run household_microsynth for LAD to produce data for
# microsimulation tests to pass
cd ../household_microsynth
scripts/run_microsynth.py $1 OA11

echo 'Moving to run microsimulation'
cd ..
mv ssm_current.json microsimulation/config/
mv ssm_h_current.json microsimulation/config/
mv ass_current*.json microsimulation/config/

echo 'Step 1'
cd microsimulation
scripts/run_ssm.py -c config/ssm_current.json $1

echo 'Step 2'
scripts/run_ssm_h.py -c config/ssm_h_current.json $1


echo 'Running assigment for 2012'
scripts/run_assignment.py -c config/ass_current_2012.json $1

echo 'Running assigment for 2020'
scripts/run_assignment.py -c config/ass_current_2020.json $1

echo 'Running assigment for 2022'
scripts/run_assignment.py -c config/ass_current_2022.json $1

echo 'Running assigment for 2032'
scripts/run_assignment.py -c config/ass_current_2032.json $1

echo 'Running assigment for 2039'
scripts/run_assignment.py -c config/ass_current_2039.json $1

echo 'Done!'

now=$(date +"%T")
echo "Current time : $now"

