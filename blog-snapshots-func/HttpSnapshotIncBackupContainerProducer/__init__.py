import logging
import os

import azure.functions as func
from azure.identity import DefaultAzureCredential, ClientSecretCredential
from azure.storage.blob import BlobServiceClient
from azure.storage.queue import QueueService, QueueMessageFormat
from datetime import datetime

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')
    container_name = req.params.get('container')

    # create blob service client and container client
    credential = DefaultAzureCredential()
    storage_account_source_url = "https://" + os.environ["par_storage_account_name_source"] + ".blob.core.windows.net"
    client_source = BlobServiceClient(account_url=storage_account_source_url, credential=credential)
    container_source = client_source.get_container_client(container_name)

    # Create queue client
    queue_service = QueueService(account_name=os.environ['par_storage_account_name_queue'], account_key=os.environ['par_storage_account_key_queue'])
    queue_service.encode_function = QueueMessageFormat.text_base64encode

    # Get all blobs in container
    prev_blob_name = ""
    prev_blob_etag = ""
    blob_list = container_source.list_blobs(include=['snapshots'])
    for blob in blob_list:

        if blob.snapshot == None:
            # Blob that is not snapshot
            if prev_blob_name != blob.name:
                # New blob without snapshot, create snapshot/backup
                logging.info("new blob" + blob.name + ", create snapshot/backup")
                create_snapshot_backup(client_source, queue_service, container_name, blob.name, blob.etag)

            elif prev_blob_etag != blob.etag:
                # Existing blob that has changed, create snapshot/backup
                logging.info(blob.name + "has changed, create snapshot/backup")
                create_snapshot_backup(client_source, queue_service, container_name, blob.name, blob.etag)
    
        prev_blob_name = blob.name
        prev_blob_etag = blob.etag

    result = {"status": "ok"}
    return func.HttpResponse(str(result))

def create_snapshot_backup(client_source, queue_service, container_name, blob_name, blob_etag):

    # create snapshot
    blob_client = client_source.get_blob_client(container=container_name, blob=blob_name)
    blob_client.create_snapshot()

    # add message to queue to create backup
    queue_json = "{" + "\"container\":\"{}\", \"blob_name\":\"{}\", \"etag\":\"{}\"".format(container_name, blob_name, blob_etag) + "}"
    queue_service.put_message(os.environ['par_queue_name'], queue_json)