import logging
import os
import traceback

from azure.storage.blob import  BlobClient,BlobType,BlobServiceClient
from azure.core.exceptions import (ResourceNotFoundError,)

from . import settings
from . import exceptions

from .resource import Storage
from .utils import file_size

logger = logging.getLogger(__name__)

class AzureBlobStorage(Storage):
    def __init__(self,connection_string,container_name):
        self._connection_string = connection_string
        self._container_name = container_name
        self._service_client = BlobServiceClient.from_connection_string(self._connection_string,**settings.AZURE_BLOG_CLIENT_KWARGS)
        self._container_client = self._service_client.get_container_client(self._container_name)
        self._path = None
        self._client = None

    def __str__(self):
        return self._container_name

    def get_blob_client(self,path):
        if not self._client or self._path != path:
            self._client = self._container_client.get_blob_client(path)
            self._path = path
        return self._client

    def get_content(self,path):
        """
        read the content of the resource from storage
        """
        try:
            return self.get_blob_client(path).download_blob().readall()
        except ResourceNotFoundError as ex:
            raise exceptions.ResourceNotFound("Resource({}) Not Found".format(path))
            

    def delete(self,path):
        """
        Delete the resource from storage
        """
        try:
            self.get_blob_client(path).delete_blob(delete_snapshots="include")
        except:
            logger.error("Failed to delete the resource from blob storage.{}".format(path,traceback.format_exc()))

    def download(self,path,filename):
        """
        Download the blob resource to a file
        """
        with open(filename,'wb') as f:
            self.get_blob_client(path).download_blob().readinto(f)

    def update(self,path,byte_list):
        """
        Update the resource's data in bytes.
        byte_list must be not empty
        """
        self.get_blob_client(path).upload_blob(byte_list,blob_type=BlobType.BlockBlob,overwrite=True,timeout=3600,max_concurrency=5)

    def upload(self,path,data_stream,length=None):
        """
        Update the resource's data in bytes.
        data_stream must be not empty
        """
        self.get_blob_client(path).upload_blob(data_stream,blob_type=BlobType.BlockBlob,overwrite=True,timeout=3600,max_concurrency=5,length=length)

    def upload_file(self,path,sourcepath):
        """
        Update the resource's data in bytes.
        data_stream must be not empty
        """
        file_length = file_size(sourcepath)
        with open(sourcepath,'rb') as f:
            return self.upload(path,f,length=file_length)

    def list_resources(self,path=None):
        """
        List files in the path
        """
        if not path:
            path = None
        else:
            if path[-1] != "/":
                path = "{}/".format(path)
            if path[0] == "/":
                path = path[1:]

        return [m for m in self._container_client.list_blobs(name_starts_with=path)]
            


