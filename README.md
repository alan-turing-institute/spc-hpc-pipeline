# Running `SPC pipeline` on the cloud - Microsoft Azure

It is possible to make use of Azure cloud infrastructure to run the SPC pipeline in two ways:
* Using Azure blob storage to store the microsimulation outputs.
* Using Azure batch to parallelize the running of microsimulation at the LAD level.  This can vastly speed up the running time of your job.

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
from your prefered environment in the top level directory of this repo.


-------------------------------------

## Running the SPC pipeline on batch


### SPC pipeline output

Note that using Azure storage as detailed above is a prerequisite for using Azure batch.


### Checking the status of your job on Azure batch


### Downloading data from Azure storage when it is ready


## What is going on "under the hood" when running on Azure batch?


### What does `SPENSER_HPC_setup.sh` do ?

### What happens when all tasks are submitted?
