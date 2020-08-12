import logging
import os
import stat
import shutil
import traceback
import json
import errno
import socket
import datetime

from . import settings
from . import exceptions
from .utils import remove_file,timezone,file_mtime,set_file_mtime,JSONEncoder,JSONDecoder

from .resource import Storage

logger = logging.getLogger(__name__)

class LocalStorage(Storage):
    def __init__(self,root_path):
        if not os.path.exists(root_path):
            raise Exception("Path({}) Not Exist".format(root_path))

        self._root_path = root_path

    def __str__(self):
        return "LocalStorage({})".format(self._root_path)

    def get_abspath(self,path):
        res_path = os.path.join(self._root_path,path)
        res_dir = os.path.dirname(res_path)
        if os.path.exists(res_dir):
            if not os.path.isdir(res_dir):
                raise Exception("The path({}) is not a folder".format(res_dir))
        else:
            os.makedirs(res_dir)
        return res_path

    def get_content(self,path):
        """
        read the content of the resource from storage
        """
        res_path = os.path.join(self._root_path,path)
        if os.path.exists(res_path):
            with open(os.path.join(self._root_path,path),'rb') as f:
                return f.read()
        else:
            raise exceptions.ResourceNotFound("Resource({}) Not Found".format(path))

    def create_dir(self,path,mode=stat.S_IROTH|stat.S_IXOTH|stat.S_IRGRP|stat.S_IXGRP|stat.S_IRWXU):
        """
        Create path with access mode if it doesn't exist
        """
        abs_path = os.path.join(self._root_path,path)
        if os.path.exists(abs_path):
            if not os.path.isdir(abs_path):
                raise Exception("The path({}) is not a folder".format(abs_path))
        else:
            os.makedirs(abs_path)
            self.chmod(abs_path,mode=mode)

    def chmod(self,path,mode=stat.S_IROTH|stat.S_IXOTH|stat.S_IRGRP|stat.S_IXGRP|stat.S_IRWXU):
        """
        Change the path's access mode
        """
        abs_path = os.path.join(self._root_path,path)
        os.chmod(abs_path,mode)


    def delete(self,path):
        """
        Delete the resource from storage
        """
        res_path = os.path.join(self._root_path,path)
        if not os.path.exists(res_path):
            return
        os.remove(res_path)
        #continue to remove empty path until the root_path
        res_dir = os.path.dirname(res_path)
        while res_dir != self._root_path and len(os.listdir(res_dir)) == 0:
            os.rmdir(res_dir)
            res_dir = os.path.dirname(res_dir)

    def download(self,path,filename):
        """
        Download the blob resource to a file
        """
        shutil.copyfile(os.path.join(self._root_path,path),filename)

    def update(self,path,byte_list):
        """
        Update the resource's data in bytes.
        byte_list must be not empty
        """
        res_path = self.get_abspath(path)
        with open(res_path,'wb') as f:
            f.write(byte_list)


    def upload(self,path,data_stream,length=None):
        """
        Update the resource's data in bytes.
        data_stream must be not empty
        """
        res_path = self.get_abspath(path)
        with open(res_path,'wb') as f:
            if length:
                f.write(f.read(length))
            else:
                f.write(f.read())

    def upload_file(self,path,sourcepath,length=None):
        """
        Update the resource's data in bytes.
        data_stream must be not empty
        """
        res_path = self.get_abspath(path)

        shutil.copyfile(sourcepath,res_path)

    def list_resources(self,path=None):
        """
        List files in the path
        """
        if not path:
            path = None
        else:
            if path[0] == "/":
                path = path[1:]
            resources = []
            abs_path = os.path.join(self._root_path,path)
            if not os.path.exists(abs_path):
                return []

            search_dirs = [abs_path]
            while search_dirs:
                search_dir = search_dirs[0]
                del search_dirs[0]
                for f in os.listdir(search_dir):
                    f_path = os.path.join(search_dir,f)
                    if os.path.isfile(f_path):
                        resources.append(os.path.relpath(f_path,self._root_path))
                    else:
                        search_dirs.append(f_path)

            return resources

        return [m for m in self._container_client.list_blobs(name_starts_with=path)]
            
    def acquire_lock(self,path,expired=None):
        """
        expired: lock expire time in seconds
        Acquire the exclusive lock, and return the time of the lock
        Throw AlreadyLocked exception if can't obtain the lock
        """
        if expired is not None and expired <= 0:
            expired = None
    
        fd = None
        lockfile = os.path.join(self._root_path,path)
        try:
            fd = os.open(lockfile, os.O_CREAT|os.O_EXCL|os.O_RDWR)
            os.write(fd,json.dumps({
                "host": socket.getfqdn(),
                "pid":os.getpid(),
                "lock_time":timezone.now()
            },cls=JSONEncoder).encode())
            #lock is acquired
            return file_mtime(lockfile)
        except OSError as e:
            if e.errno == errno.EEXIST:
                #lock is exist, check whether it is expired or not.
                if expired and timezone.now() > file_mtime(lockfile) + datetime.timedelta(seconds=expired):
                    #lockfile is expired,remove the lock file
                    remove_file(lockfile)
                    return self.acquire_lock(path,expired=expired)
    
                metadata = None
                with open(lockfile,"r") as f:
                    metadata = f.read()
                if metadata:
                    try:
                        metadata = json.loads(metadata,cls=JSONDecoder)
                    except:
                        metadata = {}
                else:
                    metadata = {}
                raise exceptions.AlreadyLocked("Already Locked at {2} and renewed at {3} by process({1}) running in host({0})".format(metadata["host"],metadata["pid"],metadata["lock_time"],file_mtime(lockfile)))
            else:
                raise
        finally:
            if fd:
                try:
                    os.close(fd)
                except:
                    pass

    def renew_lock(self,path,previous_renew_time):
        """
        Acquire the exclusive lock, and return the renew time
        Throw InvalidLockStatus exception if the previous_renew_time is not matched.
        """
        lockfile = os.path.join(self._root_path,path)
        if file_mtime(lockfile) != previous_renew_time:
            raise exceptions.InvalidLockStatus("The lock's last renew time({}) is not equal with the provided last renew time({})".format(file_mtime(lockfile),previous_renew_time))

        return set_file_mtime(lockfile)

    def release_lock(self,path):
        """
        relase the lock
        """
        lockfile = os.path.join(self._root_path,path)
        remove_file(lockfile)

