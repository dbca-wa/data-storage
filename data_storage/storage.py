import json
import tempfile
import os

from .utils import JSONEncoder,JSONDecoder,file_size

from .exceptions import *

class ResourceStorage(object):
    """
    A interface to list/upload/get a resource 
    """

    @property
    def resourcename(self):
        """
        the resource name.
        """
        raise NotImplementedError("Property 'resourcename' is not implemented.")

    def resource_metadatas(self,throw_exception=True,**kwargs):
        """
        kwargs should be 'resource_file' or the keys in resource_keys
        if kwargs['resource_file'] is None, navigate the resource's metadata
        if kwargs['resource_file'] is not None, navigate the pushed resource's metadata
        throw_exception: if True, throw exception if resource not found; otherwise return empty generator
        Return a generator to navigate the filtered meta datas
        """
        raise NotImplementedError("Property 'resourcemetadata' is not implemented.")

    def get_resource_metadata(self,*args,resource_file="current"):
        """
        if resource_file is 'current', it means the latest archive of the specific resource, only used for archive resource
        Return the resource's metadata
        Throw ResourceNotFoundError if not found
        """
        raise NotImplementedError("Method 'resource_exist' is not implemented.")

    def is_exist(self,*args):
        """
        Check whether resource exists or not
        """
        try:
            return True if self.get_resource_metadata(*args,resource_file=None) else False
        except ResourceNotFound as ex:
            return False
        

    def delete_resource(self,**kwargs):
        """
        delete the resource_group or specified resource 
        return the list of the metadata of deleted resources
        """
        raise NotImplementedError("Method 'delete_resource' is not implemented.")

    def download_resources(self,folder=None,overwrite=False,**kwargs):
        """
        Download the filtered resources, and return (resource metadata,local folder)
        overwrite: remove the existing file or folder if overwrite is True
        raise exception if failed or can't find the resource
        """
        raise NotImplementedError("Method 'get_resource' is not implemented.")

    def download_resource(self,*args,filename=None,overwrite=False):
        """
        Download the resource which is identified by *args, and return (resource metadata,local resource's filename)
        overwrite: remove the existing file or folder if overwrite is True
        raise exception if failed or can't find the resource
        """
        raise NotImplementedError("Method 'get_resource' is not implemented.")

    def get_json(self,*args):
        """
        Return (resource_metadata,resource as dict object)
        raise exception if failed or can't find the resource
        """
        metadata,filename = self.download_resource(*args)
        try:
            with open(filename,'r') as f:
                return (metadata,json.loads(f.read(),cls=JSONDecoder))
        finally:
            os.remove(filename)

    def push_resource(self,data,metadata=None,f_post_push=None,length=None):
        """
        Push the resource to the storage
        f_post_push: a function to call after pushing resource to blob container but before pushing the metadata, has one parameter "metadata"
        Return the new resourcemetadata.
        """
        raise NotImplementedError("Method 'push_resource' is not implemented.")

        
    def push_json(self,obj,metadata=None,f_post_push=None):
        """
        Push the resource to the storage
        f_post_push: a function to call after pushing resource to blob container but before pushing the metadata, has one parameter "metadata"
        Return the new resourcemetadata.
        """
        return self.push_resource(json.dumps(obj,cls=JSONEncoder).encode(),metadata=metadata,f_post_push=f_post_push)

    def push_file(self,filename,metadata=None,f_post_push=None):
        """
        Push the resource from file to the storage
        f_post_push: a function to call after pushing resource to blob container but before pushing the metadata, has one parameter "metadata"
        Return the new resourcemetadata.
        """
        file_length = file_size(filename)
        with open(filename,'rb') as f:
            return self.push_resource(f,metadata=metadata,f_post_push=f_post_push,length=file_length)
