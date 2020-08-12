import logging
import os
import traceback
import socket
import datetime
import json

from azure.storage.blob import  BlobClient,BlobType,BlobServiceClient
from azure.core.exceptions import (ResourceNotFoundError,ResourceExistsError)

from . import settings
from . import exceptions

from .resource import Storage
from .utils import file_size,JSONEncoder,JSONDecoder,timezone

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
        except ResourceNotFoundError as ex:
            pass
        except:
            logger.error("Failed to delete the resource from blob storage.{}".format(path,traceback.format_exc()))

    def download(self,path,filename):
        """
        Download the blob resource to a file
        """
        with open(filename,'wb') as f:
            self.get_blob_client(path).download_blob().readinto(f)

    def update(self,path,byte_list,overwrite=True):
        """
        Update the resource's data in bytes.
        byte_list must be not empty
        """
        self.get_blob_client(path).upload_blob(byte_list,blob_type=BlobType.BlockBlob,overwrite=overwrite,timeout=3600,max_concurrency=5)

    def upload(self,path,data_stream,length=None,overwrite=True):
        """
        Update the resource's data in bytes.
        data_stream must be not empty
        """
        self.get_blob_client(path).upload_blob(data_stream,blob_type=BlobType.BlockBlob,overwrite=overwrite,timeout=3600,max_concurrency=5,length=length)

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
            
    def acquire_lock(self,path,expired=None):
        """
        expired: lock expire time in seconds
        Acquire the exclusive lock, and return the time of the lock
        Throw AlreadyLocked exception if can't obtain the lock
        """
        if expired is not None and expired <= 0:
            expired = None
    
        try:
            lock = {
                "host": socket.getfqdn(),
                "pid":os.getpid(),
                "lock_time":timezone.now()
            }
            self.update(path,json.dumps(lock,cls=JSONEncoder).encode(),overwrite=False)
            #lock is acquired
            return lock["lock_time"]
        except ResourceExistsError as e:
            lock = json.loads(self.get_content(path).decode(),cls=JSONDecoder) or {}
            renew_time = lock.get("renew_time") or lock.get("lock_time")
            if not renew_time:
                raise exceptions.InvalidLockStatus("Can't find lock's renew_time or lock_time.{})".format(lock))

            #lock is exist, check whether it is expired or not.
            if expired and timezone.now() > renew_time + datetime.timedelta(seconds=expired):
                #lockfile is expired,remove the lock file
                self.release_lock(path)
                return self.acquire_lock(path,expired=expired)
            if "renew_time" in lock:
                raise exceptions.AlreadyLocked("Already Locked at {2} and renewed at {3} by process({1}) running in host({0})".format(lock.get("host"),lock.get("pid"),lock.get("lock_time"),lock["renew_time"]))
            else:
                raise exceptions.AlreadyLocked("Already Locked at {2} by process({1}) running in host({0})".format(lock.get("host"),lock.get("pid"),lock.get("lock_time")))

    def renew_lock(self,path,previous_renew_time):
        """
        Acquire the exclusive lock, and return the renew time
        Throw InvalidLockStatus exception if the previous_renew_time is not matched.
        """
        try:
            lock = json.loads(self.get_content(path).decode(),cls=JSONDecoder)
            renew_time = lock.get("renew_time") or lock.get("lock_time")
            if renew_time != previous_renew_time:
                raise exceptions.InvalidLockStatus("The lock's last renew time({}) is not equal with the provided last renew time({})".format(renew_time,previous_renew_time))
            lock["renew_time"] = timezone.now()
            self.update(path,json.dumps(lock,cls=JSONEncoder).encode())

            return lock["renew_time"]
        except exceptions.ResourceNotFound as ex:
            raise exceptions.InvalidLockStatus("The lock({}) Not Found".format(path))
            
    def release_lock(self,path):
        """
        relase the lock
        """
        try:
            self.delete(path)
        except Exception as ex:
            try:
                self.get_content(path)
                raise ex
            except exceptions.ResourceNotFound as ex1:
                pass


