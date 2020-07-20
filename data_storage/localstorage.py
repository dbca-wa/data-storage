import logging
import os
import shutil
import traceback

from . import settings
from . import exceptions
from .utils import remove_file

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
            


