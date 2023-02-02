# Running `SPC pipeline` on the cloud - Microsoft Azure

It is possible to make use of Azure cloud infrastructure to run the SPC pipeline in two ways:
* Using Azure blob storage to store the microsimulation outputs.
* Using Azure batch to parallelize the running of microsimulation at the LAD (Local Authority Districts) level.  This can vastly speed up the running time of your job.

In order to do these, you will need the following:
* An Azure account and an active subscription.
* An Azure "Blob Storage Account" - follow instructions on https://docs.microsoft.com/en-us/azure/storage/blobs/storage-blob-create-account-block-blob?tabs=azure-portal
* An Azure "Batch Account" - see https://docs.microsoft.com/en-us/azure/batch/batch-technical-overview for a description.  When setting up the batch account, it will ask you to link it to a Storage Account, so use the one above (and ensure that you select the same "Region" for both.
* You will probably need to increase the "quota" on your Batch Account - I believe that by default the quota for different types of VM are all set to zero.  There are instructions on how to do this at: https://docs.microsoft.com/en-us/azure/batch/batch-quota-limit - for our workflow we are using 100 dedicated cores of A1v2 VMs.

## Setting up your Azure `config.py`

In this repo there is a file `config.py` with place-holders that you must fill in the various fields.  The necessary info can be found in the Azure portal [https://portal.azure.com/#home] - perhaps the easiest way is to navigate via the "Subscriptions" icon at the top of the portal, then find the "Resources" (i.e. the Storage Account and the Batch Account).
* For the Storage Account, look on the left sidebar under "Settings" for the "Access keys" - then copy/paste one of the keys into the relevant field in `config.py`.
* For the Batch Account, similarly there is a "Keys" icon under "Settings" which will lead to the relevant info.
For the "batch_pool_id" you can put any name you like - if there will be multiple people using the same batch and storage accounts, you might want to use your initials or something to identify you (and in this case, you should be careful that the sum of everyone's "node_count"s don't exceed the quota for the batch account.

Once you have populated the fields in `config.py`, then do
```
pip install -r requirements.txt
```
from your preferred environment in the top level directory of this repo.

## Preparing submodules 

Run
```
git submodule update --init --recursive
```
to pull submodules required for these scripts. After this you will need to
prepare a zip file with the required modules: 

```
zip -r submodules.zip submodules/
```



## Setting up your NOMIS API key

Ths SCP pipeline uses an API by Nomisweb which allows relatively easy programmatic access the to data.
Nomisweb currently hosts the ONS principal NPP data for the UK, the SNPP data for England, and all of the MYE data.

You need to obtain a NOMIS API key and add it to the `scripts/scp/NOMIS_API_KEY.txt` file before running any SCP jobs.


## Running the SPC pipeline on batch

The script `spc-hpc-client.py` is designed to create a batch Pool and assigng parallel tasks that run
a given script individually for different LADs. The script has several options that can be  understood by 
running:

``python spc-hpc-client.py --help``

which returns:

```
options:
  -h, --help            show this help message and exit
  --upload_files UPLOAD_FILES
                        Path to files to be uploaded to batch container and used to run the script.
  --submodules SUBMODULES
                        Path where submodules are stored which are used by scripts
  --script_file_name SCRIPT_FILE_NAME
                        Name of bash script to be ran on jobs, should exist in the path provided by '--upload_files'
  --lads [ALIST ...]    LADs codes to be ran in parallel, one code per task. Examples: --lads E06000001 E06000002 E06000003 E06000004
  --lads_file LADS_FILE
                        Path to CSV file containing the LAD codes to be used, under a column names "LAD20CD"
```


### Quickstart 

1. For example, to run the SPC pipeline on 4 LADS in parallel you can run the following.

```python spc-hpc-client.py --upload_files scripts/scp --script_file_name SPENSER_HPC_setup.sh --submodules submodules --lads E06000001 E06000002 E06000003 E06000004```

2. If you want to run the SPC pipeline on all the LADS in parallel you can run the following.

```python spc-hpc-client.py --upload_files scripts/scp --script_file_name SPENSER_HPC_setup.sh --submodules submodules --lads_file data/new_lad_list.csv```

For each case you have to make sure your `POOL_NODE_COUNT` variable in the `config.py` file is 
at least the number of LADs you plan to run in parallel and that your quota allows it ( in case 1.  `POOL_NODE_COUNT=4`). 

### SPC pipeline output

Note that using Azure storage as detailed above is a prerequisite for using Azure batch.

For each job a time-stamped named container is created, in there you can find the following;
    - The input files used to run the job.
    - The output data generated by each task, saved under a directory named with the task name (e.g `E06000001`).

All task of one job save their outputs to the same container.

### Checking the status of your job on Azure batch

To be added.

### Downloading data from Azure storage when it is ready

For a given submission all files created by each task will be stored in the storage container belonging to the storage account
defined in the [config.py](config.py) file. The name of the container is named something like `scp-TIMESTAMP-JOB-SUBMISSION`. The files 
produced for each LAD are stored as subdirectories named with the LAD code as described above. 

To download the content of a container we use [AzCopy](https://learn.microsoft.com/en-us/azure/storage/common/storage-ref-azcopy), is a command-line tool that moves data into and out of Azure Storage.
First you must donwload the tool to your machine as described [here](https://learn.microsoft.com/en-us/azure/storage/common/storage-use-azcopy-v10#download-azcopy) and authentify using the command `AzCopy login`.

Once you have logged in you can Download a directory in the following way:

`azcopy copy 'https://<storage-account-name>.core.windows.net/<container-name>/<directory-path>' '<local-directory-path>' --recursive`

Example in our case:
                              
`azcopy copy 'https://scpoutputs.blob.core.windows.net/scp-2022-12-08-13-04-44/' 'WalesMicrosimulation/' --recursive`
                   
You can check if the needed files for every LAD have been produced and downloaded by running a simple file counting script on the
downloaded container directory. With the current version of this code 69 files have to be produced.

In the example above you can do:

```
cd WalesMicrosimulation/scp-2022-12-08-13-04-44/
source count_files.sh 69 
```

the script will return a warning for any subdirectory (LAD code name e.g. `W06000002`) that has a different number of files to the one given as an
argument (69 in the example). As LAD subdirectory with a different file count than expected means that there
was an issue on the microsimulation run for that LAD and needs to be investigated.

## What is going on "under the hood" when running on Azure batch?


(This section is only necessary if you are interested in knowing more about how this works - if you just want to run the jobs, the instructions above should suffice.)

When you run the command

```python spc-hpc-client.py --upload_files scripts/scp --script_file_name SPENSER_HPC_setup.sh --submodules submodules --lads E06000001 E06000002 E06000003 E06000004```

The batch functionality is implemented at the LAD level and follows the next steps.   
* Creates a new batch "Job" with a name composed of the `JOB_ID` variable from the `config.py` file as the name
 and the current time.
* Create a number Tasks for the Job, submitting one task per LAD.
* Upload the upload the files in the `--upload_files` path  (for the SCP pipeline all necesary files are in `scripts/scp` of this repo) to a time-stamped blob storage container.
* Define the `SPENSER_HPC_setup.sh` as the file to run in each parallel task (LAD). 

For each Task, the process is then:
* Submit the batch Task, which will run `SPENSER_HPC_setup.sh` on the batch node for a given
LAD.
* Once the Task is finished, upload the output data created (in the household_micro-synth and micromanipulation directories) to the blob storage.


### What does `SPENSER_HPC_setup.sh` do ?

The execution of a single Task on a batch node (which is an Ubuntu-X.X VM) is governed by this shell script `SPENSER_HPC_setup.sh`. Which has the input arguments:

1. The LAD to be simulated.

For this pipeline the command ran on a given task is the following:

``/bin/bash SPENSER_HPC_setup.sh E06000001``

The basic flow of the SPENSER_HPC_setup.sh script is:
* Install some packages, including miniconda, and create and activate a Python 3.9 conda environment.
* Clone the following repos: `UKCensusAPI`, `ukpopulation`, `humanleague`, `household_microsynth` and `microsimulation`.
* Install each of these packages from source as recommended in their repo.
* Move the following uploaded config files to the `microsimulation/config` directory.
  * A configuration file for the household_microsimulation step. (`ssm_current.json`)
  * A configuration file for the microsimulation time dependent step. (`ssm_h_current.json`)
  * Several configuration file for the microsimulation assigment step (`ass_current_year.json` for years 2012, 2020, 2022, 2032, 2039).
* Run the command following commands:
  * ```scripts/run_microsynth.py E06000001 OA11``` from the household_microsynth directory.
  * ```scripts/run_ssm.py.py -c config/ssm_current.json E06000001``` from the microsimulation directory.
  * ```scripts/run_ssm_h.py.py -c config/ssm_h_current.json E06000001``` from the microsimulation directory.
  * ```scripts/run_assignment.py -c config/ass_current_2012.json E06000001``` from the microsimulation directory.
  * ```scripts/run_assignment.py -c config/ass_current_2020.json E06000001``` from the microsimulation directory.
  * ```scripts/run_assignment.py -c config/ass_current_2022.json E06000001``` from the microsimulation directory.
  * ```scripts/run_assignment.py -c config/ass_current_2032.json E06000001``` from the microsimulation directory.
  * ```scripts/run_assignment.py -c config/ass_current_2039.json E06000001``` from the microsimulation directory.

### What happens when all tasks are submitted?

The script  `spc-hpc-client.py` will submit all the tasks and will wait for the tasks to finish. Once the tasks
have finished if requests the user in the command line if they want to delete the Pool and Jobs. 


## Running locally 
Due to the target system for this pipeline being Ubuntu 20.02 running on anything else may require tweaking and modifications. 
A Docker file is given to enable running of the pipeline locally with a consistent setup to azure.

To build the image run
```bash
docker build -t "dyme-spc:Dockerfile" .
```

then begin running 

```bash
docker run --name dyme -d -t "dyme-spc:Dockerfile"
```

with it running you can then start a bash terminal into the container
```
docker exec -it dyme bash 
```

Here, run the spenser script with a LADs list or single LAD. In the example below the full set of Wales will be run
```
./SPENSER_HPC_setup.sh `awk -F "\"*,\"*" '{print substr($1,2)}' new_lad_list_Wales.csv | awk 'NR!=1 {print}'`
```
