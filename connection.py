import datetime
import time
import os
import sys
import io

import config
import helpers
from azure.batch import BatchServiceClient
from azure.batch.batch_auth import SharedKeyCredentials
from azure.storage.blob import (
    BlobServiceClient,
    BlobSasPermissions,
    generate_blob_sas,
    generate_container_sas
)

from azure.batch import BatchServiceClient
import azure.batch.models as batchmodels
from azure.core.exceptions import ResourceExistsError



def getBatchServiceClient():
    credentials = SharedKeyCredentials(config.BATCH_ACCOUNT_NAME,
                                       config.BATCH_ACCOUNT_KEY)
    return BatchServiceClient(credentials,
        batch_url=config.BATCH_ACCOUNT_URL)


def getBlobServiceClient():
    return BlobServiceClient(
        account_url=f"https://{config.STORAGE_ACCOUNT_NAME}.{config.STORAGE_ACCOUNT_DOMAIN}/",
        credential=config.STORAGE_ACCOUNT_KEY
    )

DEFAULT_ENCODING = "utf-8"


def create_container(blob_service_client):
    """
    Creates a container in the given blob 

    Returns: string name of container
    """
    container_name = 'scp'  # pylint: disable=invalid-name
    current_time = time.strftime("%Y-%m-%d_%H-%M-%S")
    container_name += "__" + current_time
    container_name = helpers.sanitize_container_name(container_name)
    try:
        blob_service_client.create_container(container_name)
    except ResourceExistsError:
        print(f"Container {container_name} already exists!")   
        print("Using existing container!")
    return container_name


def print_batch_exception(batch_exception: batchmodels.BatchErrorException):
    """
    Prints the contents of the specified Batch exception.

    :param batch_exception:
    """
    print('-------------------------------------------')
    print('Exception encountered:')
    if batch_exception.error and \
            batch_exception.error.message and \
            batch_exception.error.message.value:
        print(batch_exception.error.message.value)
        if batch_exception.error.values:
            print()
            for mesg in batch_exception.error.values:
                print(f'{mesg.key}:\t{mesg.value}')
    print('-------------------------------------------')


def upload_file_to_container(blob_storage_service_client: BlobServiceClient,
                             container_name: str, file_path: str) -> batchmodels.ResourceFile:
    """
    Uploads a local file to an Azure Blob storage container.

    :param blob_storage_service_client: A blob service client.
    :param str container_name: The name of the Azure Blob storage container.
    :param str file_path: The local path to the file.
    :return: A ResourceFile initialized with a SAS URL appropriate for Batch
    tasks.
    """
    blob_name = os.path.basename(file_path)
    blob_client = blob_storage_service_client.get_blob_client(container_name, blob_name)

    print(f'Uploading file {file_path} to container [{container_name}]...')

    with open(file_path, "rb") as data:
        blob_client.upload_blob(data, overwrite=True)

    sas_token = generate_blob_sas(
        config.STORAGE_ACCOUNT_NAME,
        container_name,
        blob_name,
        account_key=config.STORAGE_ACCOUNT_KEY,
        permission=BlobSasPermissions(read=True, write=True),
        expiry=datetime.datetime.utcnow() + datetime.timedelta(hours=2)
    )


    sas_url = generate_sas_url(
        config.STORAGE_ACCOUNT_NAME,
        config.STORAGE_ACCOUNT_DOMAIN,
        container_name,
        blob_name,
        sas_token
    )

    return batchmodels.ResourceFile(
        http_url=sas_url,
        file_path=blob_name
    )


def generate_sas_url(
        account_name: str,
        account_domain: str,
        container_name: str,
        blob_name: str,
        sas_token: str
) -> str:
    """
    Generates and returns a sas url for accessing blob storage
    """
    return f"https://{account_name}.{account_domain}/{container_name}/{blob_name}?{sas_token}"


def create_pool(batch_service_client: BatchServiceClient, pool_id: str):
    """
    Creates a pool of compute nodes with the specified OS settings.

    :param batch_service_client: A Batch service client.
    :param str pool_id: An ID for the new pool.
    :param str publisher: Marketplace image publisher
    :param str offer: Marketplace image offer
    :param str sku: Marketplace image sku
    """
    print(f'Creating pool [{pool_id}]...')

    # Create a new pool of Linux compute nodes using an Azure Virtual Machines
    # Marketplace image. For more information about creating pools of Linux
    # nodes, see:
    # https://azure.microsoft.com/documentation/articles/batch-linux-nodes/
    new_pool = batchmodels.PoolAddParameter(
        id=pool_id,
        virtual_machine_configuration=batchmodels.VirtualMachineConfiguration(
            image_reference=batchmodels.ImageReference(
                publisher="canonical",
                offer="0001-com-ubuntu-server-focal",
                sku="20_04-lts",
                version="latest"
            ),
            node_agent_sku_id="batch.node.ubuntu 20.04"),
        vm_size=config.POOL_VM_SIZE,
        target_dedicated_nodes=config.POOL_NODE_COUNT
    )
    batch_service_client.pool.add(new_pool)


def create_job(batch_service_client: BatchServiceClient, job_id: str, pool_id: str):
    """
    Creates a job with the specified ID, associated with the specified pool.

    :param batch_service_client: A Batch service client.
    :param str job_id: The ID for the job.
    :param str pool_id: The ID for the pool.
    """
    print(f'Creating job [{job_id}]...')

    job = batchmodels.JobAddParameter(
        id=job_id,
        pool_info=batchmodels.PoolInformation(pool_id=pool_id))

    batch_service_client.job.add(job)


def add_tasks(batch_service_client: BatchServiceClient, job_id: str, input_script_file: str, config_ssm: str,
              config_ssm_h: str, config_ass: str, nomis_file: str,
              input_container_name: str, LAD_tasks):
    """
    Adds a task for each input file in the collection to the specified job.

    :param batch_service_client:  A Batch service client.
    :param job_id:  The ID of the job to which to add the tasks.
    :param input_script_file: Input script file to be ran on command.
    :param config_ssm: Script configuration file 1
    :param config_ssm_h: Script configuration file 2
    :param config_ass: Script configuration file 3
    :param nomis_file: Nomis API key
    :param input_container_name: The name of the Azure Blob storage container.
    :param LAD_tasks: A collection of inputs to be run as arguments to script. One task will be created for each argument file.

    """

    print(f'Adding {LAD_tasks} tasks to job [{job_id}]...')

    tasks = []

    for idx, lad in enumerate(LAD_tasks):
        command = "/bin/bash {} {} {} {} {}".format(
            input_script_file.file_path, lad, config_ssm.file_path, config_ssm_h.file_path, config_ass.file_path
        )

        output_file_path = f"*/data/*{lad}*.csv"

        sas_token = generate_container_sas(
            config.STORAGE_ACCOUNT_NAME,
            input_container_name,
            account_key=config.STORAGE_ACCOUNT_KEY,
            permission=BlobSasPermissions(read=True, write=True),
            expiry=datetime.datetime.utcnow() + datetime.timedelta(hours=10)
        )

        container_sas_url = "https://{}.blob.core.windows.net/{}?{}".format(
            config.STORAGE_ACCOUNT_NAME, input_container_name, sas_token)

        user = batchmodels.UserIdentity(
            auto_user=batchmodels.AutoUserSpecification(
                elevation_level=batchmodels.ElevationLevel.admin,
                scope=batchmodels.AutoUserScope.task,
            )
        )

        tasks.append(batchmodels.TaskAddParameter(
            id=f'Task{idx}_{lad}',
            command_line=command,
            resource_files=[input_script_file, config_ssm, config_ssm_h, config_ass, nomis_file],
            user_identity=user,
            output_files=[batchmodels.OutputFile(
                file_pattern=output_file_path,
                destination=batchmodels.OutputFileDestination(
                    container=batchmodels.OutputFileBlobContainerDestination(
                        container_url=container_sas_url, path=lad)),
                upload_options=batchmodels.OutputFileUploadOptions(
                    upload_condition=batchmodels.OutputFileUploadCondition.task_success))]
        )
        )

    batch_service_client.task.add_collection(job_id, tasks)


def wait_for_tasks_to_complete(batch_service_client: BatchServiceClient, job_id: str,
                               timeout: datetime.timedelta):
    """
    Returns when all tasks in the specified job reach the Completed state.

    :param batch_service_client: A Batch service client.
    :param job_id: The id of the job whose tasks should be to monitored.
    :param timeout: The duration to wait for task completion. If all
    tasks in the specified job do not reach Completed state within this time
    period, an exception will be raised.
    """
    timeout_expiration = datetime.datetime.now() + timeout

    print(f"Monitoring all tasks for 'Completed' state, timeout in {timeout}...", end='')

    while datetime.datetime.now() < timeout_expiration:
        print('.', end='')
        sys.stdout.flush()
        tasks = batch_service_client.task.list(job_id)

        incomplete_tasks = [task for task in tasks if
                            task.state != batchmodels.TaskState.completed]
        if not incomplete_tasks:
            print()
            return True

        time.sleep(1)

    print()
    raise RuntimeError("ERROR: Tasks did not reach 'Completed' state within "
                       "timeout period of " + str(timeout))


def print_task_output(batch_service_client: BatchServiceClient, job_id: str,
                      text_encoding: str = None):
    """
    Prints the stdout.txt file for each task in the job.

    :param batch_client: The batch client to use.
    :param str job_id: The id of the job with task output files to print.
    """

    print('Printing task output...')

    tasks = batch_service_client.task.list(job_id)

    for task in tasks:

        node_id = batch_service_client.task.get(
            job_id, task.id).node_info.node_id
        print(f"Task: {task.id}")
        print(f"Node: {node_id}")

        stream = batch_service_client.file.get_from_task(
            job_id, task.id, config.STANDARD_OUT_FILE_NAME)

        file_text = _read_stream_as_string(
            stream,
            text_encoding)

        if text_encoding is None:
            text_encoding = DEFAULT_ENCODING

        sys.stdout = io.TextIOWrapper(sys.stdout.detach(), encoding=text_encoding)
        sys.stderr = io.TextIOWrapper(sys.stderr.detach(), encoding=text_encoding)

        print("Standard output:")
        print(file_text)


def _read_stream_as_string(stream, encoding) -> str:
    """
    Read stream as string

    :param stream: input stream generator
    :param str encoding: The encoding of the file. The default is utf-8.
    :return: The file content.
    """
    output = io.BytesIO()
    try:
        for data in stream:
            output.write(data)
        if encoding is None:
            encoding = DEFAULT_ENCODING
        return output.getvalue().decode(encoding)
    finally:
        output.close()

def handle_post_run_cleanup(DELETE_CONTAINER,DELETE_JOB,DELETE_POOL,
                            blob_service_client, batch_client,
                            container_name ):
    # Clean up Batch resources (if the user so chooses)
    # This could be coded to be cleaner!
    if not DELETE_CONTAINER:
        if helpers.query_yes_no('Delete container?') == 'yes':
            blob_service_client.delete_container(container_name)
    else:
        blob_service_client.delete_container(container_name)

    if not DELETE_JOB:
        if helpers.query_yes_no('Delete job?') == 'yes':
            batch_client.job.delete(config.JOB_ID)
    else:
        batch_client.job.delete(config.JOB_ID)

    if not DELETE_POOL:
        if helpers.query_yes_no('Delete pool?') == 'yes':
            batch_client.pool.delete(config.POOL_ID)
    else:
        batch_client.pool.delete(config.POOL_ID)