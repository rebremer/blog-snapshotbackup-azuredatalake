import logging
import os

import azure.functions as func
from azure.identity import DefaultAzureCredential, ClientSecretCredential
from azure.storage.blob import BlobServiceClient
from azure.storage.queue import QueueService, QueueMessageFormat
import requests
import time

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    # DefaultAzureCredential supports managed identity or environment configuration (see docs)
    credential = DefaultAzureCredential()

    # parse parameters
    storage_account_source = os.environ["par_storage_account_name_source"]
    storage_account_source_url = "https://" + storage_account_source + ".blob.core.windows.net"
    storage_account_backup = os.environ["par_storage_account_name_backup"]
    storage_account_backup_url = "https://" + storage_account_backup + ".blob.core.windows.net"

    # create blob client for backup and source
    credential = DefaultAzureCredential()
    client_source = BlobServiceClient(account_url=storage_account_source_url, credential=credential)
    client_backup = BlobServiceClient(account_url=storage_account_backup_url, credential=credential)

    # Create queue clients
    queue_service = QueueService(account_name=os.environ['par_storage_account_name_queue'], account_key=os.environ['par_storage_account_key_queue'])
    queue_service.encode_function = QueueMessageFormat.text_base64encode

    # Get all blobs in sourcecontainer
    container_source_list = client_source.list_containers()
    for container in container_source_list:
        # Log container name
        logging.info(container.name)
        container_source = client_source.get_container_client(container.name)
        
        # Get all blobs in container
        prev_blob_name = ""
        prev_blob_etag = ""        
        blob_source_list = container_source.list_blobs(include=['snapshots'])
        for blob in blob_source_list:

            if blob.snapshot == None:
                # Blob that is not snapshot.
                # 1. Check if snapshot needs to be created
                if prev_blob_name != blob.name:
                    # New blob without snapshot, create snapshot/backup
                    logging.info("new blob" + blob.name + ", create snapshot/backup")
                    create_snapshot(client_source, queue_service, container.name, blob.name, blob.etag)
                elif prev_blob_etag != blob.etag:
                    # Existing blob that has changed, create snapshot/backup
                    logging.info(blob.name + "has changed, create snapshot/backup")
                    create_snapshot(client_source, queue_service, container.name, blob.name, blob.etag)
    
                # 2. Check if incremental backup needs to be created
                # get blob backup and source properties
                blob_source = client_source.get_blob_client(container=container.name, blob=blob.name)
                source_last_modified = blob_source.get_blob_properties()['last_modified']
                source_etag = str(blob_source.get_blob_properties()['etag']).replace("\"","")
                blob_name_backup = append_timestamp_etag(blob.name, source_last_modified, source_etag)
                blob_backup = client_backup.get_blob_client(container=container.name + "bak", blob=blob_name_backup)
                blob_exists = check_blob_exists(blob_backup)
                # Check if blob exists
                if blob_exists == False:
                    # Latest blob does not yet exist in backup, create message on queue to update
                    queue_json = "{" + "\"container\":\"{}\", \"blob_name\":\"{}\", \"etag\":\"{}\"".format(container.name, blob.name, source_etag) + "}"
                    logging.info("backup needed for: " + queue_json)
                    queue_service.put_message(os.environ['par_queue_name'], queue_json)                
                    #asyncio.run(copy_adf_blob_source_backup(blob_source, blob_backup))
            
            prev_blob_name = blob.name
            prev_blob_etag = blob.etag

    result = {"status": "ok"}
    return func.HttpResponse(str(result))

def check_blob_exists(bc_blob):
    # Check if blob already exists
    # todo: see if this can be done without try except
    try:
        bc_blob.get_blob_properties()
        return True
    except:
        return False

def create_snapshot(client_source, queue_service, container_name, blob_name, blob_etag):
    # create snapshot
    blob_client = client_source.get_blob_client(container=container_name, blob=blob_name)
    blob_client.create_snapshot()

def append_timestamp_etag(filename, source_modified, etag):
    name, ext = os.path.splitext(filename)
    return "{name}_{modified}_{etag}{ext}".format(name=name, modified=source_modified, etag=etag, ext=ext)