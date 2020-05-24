# See also https://github.com/Azure/azure-sdk-for-python/blob/master/sdk/storage/azure-storage-blob/samples/blob_samples_common.py
#          https://github.com/Azure/azure-storage-python/blob/master/tests/blob/test_common_blob.py
# Import libraries
from azure.storage.filedatalake import DataLakeServiceClient
from azure.core.exceptions import ResourceExistsError
from azure.storage.blob import (
    BlobServiceClient,
    ContainerClient
)
from azure.identity import ClientSecretCredential
from datetime import datetime, timezone

# Needs to be created on beforehand
STORAGE_ACCOUNT_NAME = "<<your ADLSgen2 name with HNS enabled>>"
CLIENT_ID = "<<SPN with blog storage data contributor role on ADLSgen2 account>>"
CLIENT_SECRET = "<<key of SPN>>"
TENANT_ID = "<<tenant id>>"

# Create token to authenticate to storage account
token_credential = ClientSecretCredential(
    TENANT_ID,
    CLIENT_ID,
    CLIENT_SECRET
)

# Create global handlers and variables
adls_service_client = DataLakeServiceClient(account_url="{}://{}.dfs.core.windows.net".format("https", STORAGE_ACCOUNT_NAME), credential=token_credential)
blob_service_client = BlobServiceClient(account_url="{}://{}.blob.core.windows.net".format("https", STORAGE_ACCOUNT_NAME), credential=token_credential)
currentTime = datetime.now() 
file_system_name = "test-file-system" + str(currentTime.strftime('%H%M%S'))
container_client = blob_service_client.get_container_client(file_system_name)

def init_file_system():

    try:
        file_system_client = adls_service_client.create_file_system(file_system=file_system_name)
    except ResourceExistsError:
        file_system_client = adls_service_client.get_file_system_client(file_system=file_system_name)
        print (file_system_name + " already exists")
    file_system_client.create_directory("dir1")
    file_system_client.create_directory("dir1/subdir1")
    file_system_client.create_directory("dir1/subdir2")
    
    directory_client = file_system_client.get_directory_client("dir1/subdir2")
    file_client = directory_client.create_file("test-snapshot.txt")
    file_contents = "testsnapshot"
    file_client.append_data(data=file_contents, offset=0, length=len(file_contents))
    file_client.flush_data(len(file_contents))

def delete_file_system():
    try:
        adls_service_client.delete_file_system(file_system=file_system_name)
        print (file_system_name + " deleted")        
    except ResourceExistsError:
        print (file_system_name + " does not exist, nothing to delete")
    
def corrupt_file():

    file_system_client = adls_service_client.get_file_system_client(file_system=file_system_name)
    directory_client = file_system_client.get_directory_client("dir1/subdir2")
    file_client = directory_client.create_file("test-snapshot.txt")
    file_contents = "corrupt data"
    file_client.append_data(data=file_contents, offset=0, length=len(file_contents))
    file_client.flush_data(len(file_contents))

def print_content_file():

    file_system_client = adls_service_client.get_file_system_client(file_system=file_system_name)
    directory_client = file_system_client.get_directory_client("dir1/subdir2")
    file_client = directory_client.get_file_client("test-snapshot.txt")
    download = file_client.download_file()
    downloaded_bytes = download.readall()
    print(str(downloaded_bytes))

def list_snapshots_blob(name=""):

    blob_list = container_client.list_blobs(name_starts_with=name, include=['snapshots', 'metadata'])
    for snapshot in blob_list:
        print(str(snapshot.name + ', ' + str(snapshot.snapshot) + ', ' + str(snapshot.metadata)))

def delete_snapshots_blob(name=""):

    blob_list = container_client.list_blobs(name_starts_with=name)
    for blob in blob_list:
        blob_client = blob_service_client.get_blob_client(container=file_system_name, blob=blob.name)
        print ("delete snapshots " + blob.name)
        blob_client.delete_blob(delete_snapshots="only")

def create_snapshot_container(name=""):
    
    # Find blob names and create snapshots
    snapshot_id = None
    blob_list = container_client.list_blobs(name_starts_with=name)
    for blob in blob_list:
        blob_client = blob_service_client.get_blob_client(container=file_system_name, blob=blob.name)
        if 'hdi_isfolder' not in blob_client.get_blob_properties()['metadata']:
            print (blob.name + ' is a file, create snapshot')
            metadata={'time':'{}'.format(currentTime)}
            snapshot_blob = blob_client.create_snapshot(metadata=metadata)
            snapshot_id = snapshot_blob.get('snapshot')
        else:
            print (blob.name + ' is a directory')

    return snapshot_id

def get_snapshot_blob(blob_name, snapshot_blob_id):

    blob_client_snapshot = blob_service_client.get_blob_client(container=file_system_name, blob=blob_name, snapshot=snapshot_blob_id)
    print(str(blob_client_snapshot.get_blob_properties().name + ', ' + str(blob_client_snapshot.get_blob_properties().snapshot) + ', ' + str(blob_client_snapshot.get_blob_properties().metadata)))

def restore_snapshot(blob_name, snapshot_blob_id):

    blob_client_snapshot = blob_service_client.get_blob_client(container=file_system_name, blob=blob_name)
    #print("{}://{}.blob.core.windows.net/{}/{}?{}".format("https", STORAGE_ACCOUNT_NAME, file_system_name, blob_name, snapshot_blob_id))
    
    blob_client_snapshot.start_copy_from_url("{}://{}.blob.core.windows.net/{}/{}?snapshot={}".format("https", STORAGE_ACCOUNT_NAME, file_system_name, blob_name, snapshot_blob_id))

def lease_blob(blob_name):
    
    blob_client = blob_service_client.get_blob_client(container=file_system_name, blob=blob_name)
    blob_lease_source = blob_client.acquire_lease()
    print("ID of leased blob"+ str(blob_lease_source.id))
    return blob_lease_source

def release_blob(blob_lease_source):
    
    blob_lease_source.release()
    print("blob released")

if __name__ == "__main__":

    print ("0. Create directories, files.")
    init_file_system()
    list_snapshots_blob()

    print("1. Create snapshots, check that no exceptions occur with directories/subdirectories")
    create_snapshot_container('dir1')
    create_snapshot_container('dir1/subdir2')
    create_snapshot_container('dir1/subdir2/test-snapshot.txt')
    list_snapshots_blob()
    
    print("2. Delete snapshots of file")
    delete_snapshots_blob('dir1/subdir2/test-snapshot.txt')
    list_snapshots_blob()

    print("3. Create snapshot of particular file and retrieve details")
    snapshot_blob_id = create_snapshot_container('dir1/subdir2/test-snapshot.txt')
    get_snapshot_blob('dir1/subdir2/test-snapshot.txt', snapshot_blob_id)

    print("4. Restore snapshot on blob without lease")
    snapshot_blob_id = create_snapshot_container('dir1/subdir2/test-snapshot.txt')
    print_content_file()
    corrupt_file()
    print_content_file()
    restore_snapshot('dir1/subdir2/test-snapshot.txt', snapshot_blob_id)
    print_content_file()  

    print("5. Restore snapshot on blob that has lease")
    snapshot_blob_id = create_snapshot_container('dir1/subdir2/test-snapshot.txt')
    print_content_file()
    corrupt_file()
    print_content_file()
    blob_lease_source = lease_blob('dir1/subdir2/test-snapshot.txt')
    try:
        restore_snapshot('dir1/subdir2/test-snapshot.txt', snapshot_blob_id)
    except:
        print("there is a lease on the file, unlease file first")
    print_content_file()
    release_blob(blob_lease_source)
    restore_snapshot('dir1/subdir2/test-snapshot.txt', snapshot_blob_id)
    print_content_file()

    print("6. delete file system again")
    delete_file_system()
