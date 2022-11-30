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
import sys
import time
import argparse

import config
import pandas as pd

import connection as conn


if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument("--upload_files", help="Path to files to be uploaded to batch container and used to run the script.", required=True)
    parser.add_argument("--script_file_name", help="Name of bash script to be ran on jobs, should exist in the path provided by '--upload_files' ", required=True)
    parser.add_argument("--lads", dest='alist', help="LADs codes to be ran in parallel, one code per task. Examples: --lads E06000001 E06000002 E06000003 E06000004 ",
                        type=str, nargs='*')
    parser.add_argument("--lads_file", help="Path to CSV file containing the LAD codes to be used, under a column names \"LAD20CD\"")

    args = parser.parse_args()

    if not args.alist and not args.lads_file:
        raise RuntimeError('Error: Need to provide either a LAD file or a list of LAD names')

    if args.alist:
        lads_list = args.alist
    elif args.lads_file:
        try:
            lads_list = pd.read_csv(args.lads_file)['LAD20CD'].values
        except KeyError:
            raise RuntimeError('Data csv file must have a column named LAD20CD')

    start_time = datetime.datetime.now().replace(microsecond=0)
    print(f'Sample start: {start_time}\n')

    # Create the blob client, for use in obtaining references to
    # blob storage containers and uploading files to containers.
    blob_service_client = conn.getBlobServiceClient()

    # Use the blob client to create the containers in Azure Storage if they
    # don't yet exist.
    container_name = conn.create_container(blob_service_client)

    # The collection of data files that are needed to run the tasks.
    filepaths_to_upload = []
    for root, dirs, files in os.walk(args.upload_files):
        for filename in files:
            filepath = os.path.join(root, filename)
            filepaths_to_upload.append(filepath)

    # Upload the data files.
    input_files = [
        conn.upload_file_to_container(blob_service_client, container_name, file_path)
        for file_path in filepaths_to_upload]

    # very hacky, need to change it to a better way...
    index_script = -1
    index_ssm_h = -1
    index_ssm = -1
    index_ass = -1
    index_nomis = -1

    for idx, files in enumerate(filepaths_to_upload):
        file_name = os.path.basename(files)
        if file_name == args.script_file_name:
            index_script = idx
        elif 'ssm_h_current' in file_name:
            index_ssm_h = idx
        elif 'ssm_current' in file_name:
            index_ssm = idx
        elif 'ass_current' in file_name:
            index_ass = idx
        elif 'ass_current' in file_name:
            index_ass = idx
        elif 'NOMIS_API_KEY' in file_name:
            index_nomis = idx

    if index_script == -1:
        raise RuntimeError('Error: Script to be run is not found in the input path: ' + args.upload_files)
    if index_ssm_h == -1:
        raise RuntimeError('Error: ssm_h_current.json file not file in the input path: ' + args.upload_files)
    if index_ssm == -1:
        raise RuntimeError('Error: ssm_current.json file not found in the input path: ' + args.upload_files)
    if index_ass == -1:
        raise RuntimeError('Error: ass_current.json file not found in the input path: ' + args.upload_files)

    # Create a Batch service client. We'll now be interacting with the Batch
    # service in addition to Storage

    batch_client = conn.getBatchServiceClient()

    try:
        # Create the pool that will contain the compute nodes that will execute the
        # tasks.
        create_pool(batch_client, config.POOL_ID)

        # Create the job that will run the tasks.
        create_job(batch_client, config.JOB_ID, config.POOL_ID)

        # Add the tasks to the job.
        add_tasks(batch_client, config.JOB_ID, input_files[index_script], input_files[index_ssm],
                  input_files[index_ssm_h],
                  input_files[index_ass], input_files[index_nomis], container_name, lads_list)

        # Pause execution until tasks reach Completed state.
        wait_for_tasks_to_complete(batch_client,
                                   config.JOB_ID,
                                   datetime.timedelta(minutes=600))

        print("Success! All tasks reached the 'Completed' state within the "
              "specified timeout period.")

        # Print the stdout.txt and stderr.txt files for each task to the console
        print_task_output(batch_client, config.JOB_ID)

        # Print out some timing info
        end_time = datetime.datetime.now().replace(microsecond=0)
        print()
        print(f'Sample end: {end_time}')
        elapsed_time = end_time - start_time
        print(f'Elapsed time: {elapsed_time}')
        print()
        input('Press ENTER to exit...')

    except batchmodels.BatchErrorException as err:
        print_batch_exception(err)
        raise

    finally:
      # Clean up storage resources
        print(f'Deleting container [{container_name}]...')
        # Clean up Batch resources (if the user so chooses).
        if query_yes_no('Delete job?') == 'yes':
            batch_client.job.delete(config.JOB_ID)

        if query_yes_no('Delete pool?') == 'yes':
            batch_client.pool.delete(config.POOL_ID)

