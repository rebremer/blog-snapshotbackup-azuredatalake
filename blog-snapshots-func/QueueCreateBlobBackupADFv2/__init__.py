import logging
import os, json
import azure.functions as func

from azure.storage.blob import BlobServiceClient
from azure.identity import DefaultAzureCredential, ClientSecretCredential
import requests
import time

USING_BLOB_LEASE = True
MAX_LEASE_COPY_TIME_MINUTES = 10

def main(msg: func.QueueMessage) -> None:
    logging.info('Python queue trigger function processed a queue item: %s',
                 msg.get_body().decode('utf-8'))

    # get raw parameters
    raw = msg.get_body().decode('utf-8')
    logging.info(raw)
    msg_json =json.loads(raw)

    # parse parameters
    storage_account_source = os.environ["par_storage_account_name_source"]
    storage_account_source_url = "https://" + storage_account_source + ".blob.core.windows.net"
    storage_account_backup = os.environ["par_storage_account_name_backup"]
    storage_account_backup_url = "https://" + storage_account_backup + ".blob.core.windows.net"
    container_source = msg_json['container']
    container_backup = msg_json['container'] + "bak"
    blob_name = msg_json["blob_name"]
    blob_etag = msg_json["etag"]

    # create blob client for backup and source
    credential = DefaultAzureCredential()
    client_backup = BlobServiceClient(account_url=storage_account_backup_url, credential=credential)
    blob_backup = client_backup.get_blob_client(container=container_backup, blob=blob_name)
    client_source = BlobServiceClient(account_url=storage_account_source_url, credential=credential)
    blob_source = client_source.get_blob_client(container=container_source, blob=blob_name)
    create_container_backup_if_not_exists(client_backup, container_backup)

    # Check if etag is not changed in the meantime
    try:
        blob_source_properties = blob_source.get_blob_properties()
    except:
        logging.info("blob " + blob_name + " in container " +  container_source + " does not exist anymore")
        return

    int_blob_source_etag = int(blob_source_properties.etag.replace("\"",""),16)
    int_par_etag = int(blob_etag.replace("\"",""),16)
    if int_blob_source_etag != int_par_etag:
        logging.info("blob has already changed, old: " + str(blob_etag) + ", new: " + str(blob_source.get_blob_properties().etag))
        return

    # Start copying using ADFv2
    try:
        if not USING_BLOB_LEASE:
            copy_adf_blob_source_backup(blob_source, client_backup, blob_backup, blob_etag)
        else:
            # Copy with blob lease locks the file
            copy_with_lease(blob_source, client_backup, blob_backup, blob_etag)
    except:
        logging.info("copy failed")

def copy_with_lease(blob_source, client_backup, blob_backup, blob_etag):

    # Try to acquire lease on blob
    try:
        blob_lease_source = blob_source.acquire_lease(lease_duration=30) # seconds
    except:
        logging.info("lease failed")
        return

    # Start copy using ADFv2
    start_copy = copy_adf_blob_source_backup(blob_source, client_backup, blob_backup, blob_etag)
    if not start_copy:
        blob_lease_source.release()
        return
    # Wait until copy is finished
    retry = 0
    logging.info("0 seconds passed, blob {} being copied to {}".format(blob_backup.container_name, blob_backup.blob_name))
    while retry < MAX_LEASE_COPY_TIME_MINUTES * 4:
        time.sleep(15) # wait 15 seconds before next status update
        blob_copy_finished = check_blob_exists(client_backup, blob_backup.container_name, blob_backup.blob_name)
        if not blob_copy_finished:
            # File is not copied yet
            logging.info("{} seconds passed, blob {} being copied to {}".format(str(retry*15), blob_backup.container_name, blob_backup.blob_name))
            # extend lease
            blob_lease_source = blob_source.acquire_lease(lease_duration=30, lease_id=blob_lease_source.id)
        else:
            break
        retry += 1

    # Finally, release lease
    blob_lease_source.release()

def copy_adf_blob_source_backup(blob_source, client_backup, blob_backup, etag):

    source_modified = blob_source.get_blob_properties()['last_modified']
    blob_name_backup = append_timestamp_etag(blob_source.blob_name, source_modified, etag)
    # create bearer token to authenticate to adfv2
    msi_endpoint = os.environ["MSI_ENDPOINT"]
    msi_secret = os.environ["MSI_SECRET"]
    
    token_auth_uri = f"{msi_endpoint}?resource=https%3A%2F%2Fmanagement.azure.com%2F&api-version=2017-09-01"
    head_msi = {'Secret':msi_secret}
    resp = requests.get(token_auth_uri, headers=head_msi)
    access_token = resp.json()['access_token']

    url = "https://management.azure.com/subscriptions/{}/resourceGroups/{}/providers/Microsoft.DataFactory/factories/{}/pipelines/{}/createRun?api-version=2018-06-01".format(os.environ["par_subscription_id"], os.environ["par_resource_group_name"], os.environ["par_adfv2_name"], os.environ["par_adfv2_pipeline_name"])
    response = requests.post(url, 
        headers={'Authorization': "Bearer " + access_token},
        json={
            "container": "{}".format(blob_source.container_name),
            "container_backup": "{}".format(blob_backup.container_name),
            "blob_name": "{}".format(blob_source.blob_name),
            "blob_name_backup": "{}".format(blob_name_backup)
        }
    )

    # Check if copy is correctly started
    if response.status_code != 200:
        logging.info("Error: " + str(response.content))
        return True
    else:
        logging.info("blob {} being copied to {}".format(blob_backup.container_name, blob_backup.blob_name))
        return False

def check_blob_exists(client, container_name, blob_name):
    # Check if blob already exists
    # todo: see if this can be done without try except
    logging.info
    try:
        blob_source = client.get_blob_client(container=container_name, blob=blob_name)
        blob_source.get_blob_properties()
        return True
    except:
        return False

def append_timestamp_etag(filename, source_modified, etag):
    name, ext = os.path.splitext(filename)
    return "{name}_{modified}_{etag}{ext}".format(name=name, modified=source_modified, etag=etag, ext=ext)

def create_container_backup_if_not_exists(client, container_name):
    # blob does not exists, test if container exists and if not, create
    # todo: see if this can be done without try except
    container_client = client.get_container_client(container_name)
    try:
        container_client.get_container_properties()
    except:
        client.create_container(container_name)