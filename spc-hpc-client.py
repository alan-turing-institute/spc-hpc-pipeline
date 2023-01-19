# This script is based on the python quickstart client Code Sample with Copyright (c) Microsoft Corporation
#
"""
Create a pool of nodes to run the SCP pipeline and store results on azure blob
storage.


Update the Batch and Storage account credential strings in config.py with values
unique to your accounts. These are used when constructing connection strings
for the Batch and Storage client objects.

"""

import datetime
import os
import argparse
from os.path import basename

import config
import pandas as pd

import connection as conn
import helpers
import shutil

# ORDER IS KEY HERE! 
REQUIRED_FILES = ["script", "ssm_current", "ssm_h_current", "NOMIS_API_KEY"]
DELETE_CONTAINER = False
DELETE_JOB = False
DELETE_POOL = False
TIMEOUT = 60*24


def get_and_handle_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--upload_files", help="Path to files to be uploaded to batch container and used to run the script.", required=True)
    parser.add_argument("--submodules", help="Path where submodules are stored which are used by scripts", required=True)
    parser.add_argument("--script_file_name", help="Name of bash script to be ran on jobs, should exist in the path provided by '--upload_files' ", required=True)
    parser.add_argument("--lads", dest='alist', help="LADs codes to be ran in parallel, one code per task. Examples: --lads E06000001 E06000002 E06000003 E06000004 ",
                        type=str, nargs='*')
    parser.add_argument("--lads_file", help="Path to CSV file containing the LAD codes to be used, under a column names \"LAD20CD\"")

    args = parser.parse_args()

    if not args.alist and not args.lads_file:
        raise RuntimeError('Error: Need to provide either a LAD file or a list of LAD names')

    return args

def get_lads_list(args):
    if args.alist:
        lads_list = args.alist
    elif args.lads_file:
        try:
            lads_list = pd.read_csv(args.lads_file)['LAD20CD'].values
        except KeyError:
            raise RuntimeError('Data csv file must have a column named LAD20CD')
    return lads_list



def get_upload_fp(args):
    filepaths_to_upload = {f: '' for f in REQUIRED_FILES}
    filepaths_to_upload[REQUIRED_FILES[0]] = f"{args.upload_files}/{args.script_file_name}"
    for root, dirs, files in os.walk(args.upload_files):
        for filename in files:
            if '.sh' in filename:
                continue
            filepath = os.path.join(root, filename)
            filepaths_to_upload[basename(filepath).split('.')[0]] = filepath

    subname = "submodules"
    shutil.make_archive(subname, "zip", args.submodules)
    filepaths_to_upload[subname] = subname+".zip"
    if not any([ f=='' for f in filepaths_to_upload.values()]):
        return filepaths_to_upload
    else:
        raise FileNotFoundError(f"Not all required files have been found in: {filepaths_to_upload}")
if __name__ == '__main__':

    ### Handle setup!
    args = get_and_handle_args()
    lads_list = get_lads_list(args)
    start_time = datetime.datetime.now().replace(microsecond=0)
    print(f'Sample start: {start_time}\n')

    # Create the blob client, for use in obtaining references to
    # blob storage containers and uploading files to containers.
    blob_service_client = conn.getBlobServiceClient()
    container_name = conn.create_container(blob_service_client)

    # Organise files to be uploaded and perform upload
    filepaths_to_upload = get_upload_fp(args)
    input_files = {file_name:
        conn.upload_file_to_container(blob_service_client, container_name, file_path)
        for file_name, file_path in filepaths_to_upload.items()}

    # Create a Batch service client. We'll now be interacting with the Batch
    # service in addition to Storage
    batch_client = conn.getBatchServiceClient()

    try:
        # Create the pool that will contain the compute nodes that will execute the
        # tasks.
        conn.create_pool(batch_client, config.POOL_ID)

        # Create the job that will run the tasks.
        conn.create_job(batch_client, config.JOB_ID, config.POOL_ID)

        # Add the tasks to the job.
        conn.add_tasks(batch_client, config.JOB_ID,
                    input_files[REQUIRED_FILES[0]],
                    list(input_files.values()),
                    container_name, lads_list)

        # Pause execution until tasks reach Completed state.
        conn.wait_for_tasks_to_complete(batch_client,
                                    config.JOB_ID,
                                    datetime.timedelta(minutes=TIMEOUT))

        print("Success! All tasks reached the 'Completed' state within the "
                "specified timeout period.")

        # Print the stdout.txt and stderr.txt files for each task to the console
        conn.print_task_output(batch_client, config.JOB_ID)
        # Print out some timing info
        end_time = datetime.datetime.now().replace(microsecond=0)
        print()
        print(f'Sample end: {end_time}')
        elapsed_time = end_time - start_time
        print(f'Elapsed time: {elapsed_time}')
    except Exception as e:
        # Catching all exceptions as anything will require intervention!
        print(e)
    finally:
        conn.handle_post_run_cleanup(DELETE_CONTAINER, DELETE_JOB,
                                    DELETE_POOL, blob_service_client,
                                    batch_client, container_name)

