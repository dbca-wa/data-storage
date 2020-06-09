import json
import inspect
import tempfile
import logging
import os
import shutil
import socket
import traceback

from azure.storage.blob import  BlobServiceClient,BlobClient,BlobType
from azure.core.exceptions import (ResourceNotFoundError,)

from .storage import ResourceStorage
from . import settings
from . import exceptions

from .utils import JSONEncoder,JSONDecoder,timezone,remove_file

logger = logging.getLogger(__name__)

class AzureBlob(object):
    """
    A blob client to get/update a blob resource
    """
    def __init__(self,blob_path,connection_string,container_name):
        self._blob_path = blob_path
        self._connection_string = connection_string
        self._container_name = container_name
        self._blob_client = BlobClient.from_connection_string(connection_string,container_name,blob_path,**settings.AZURE_BLOG_CLIENT_KWARGS)

    def delete(self):
        try:
            self._blob_client.delete_blob(delete_snapshots="include")
        except:
            logger.error("Failed to delete the resource from blob storage.{}".format(self._blob_path,traceback.format_exc()))


    def download(self,filename=None,overwrite=False):
        """
        Download the blob resource to a local file
        overwrite: throw exception if the local file already exist and overwrite is True; 
        Return the downloaded local resource file
        """
        if filename:
            if os.path.exists(filename):
                if not os.path.isfile(filename):
                    #is a folder
                    raise Exception("The path({}) is not a file.".format(filename))
                elif not overwrite:
                    #already exist and can't overwrite
                    raise Exception("The path({}) already exists".format(filename))

            with open(filename,'wb') as f:
                blob_data = self.get_blob_client(metadata["resource_path"]).download_blob().readinto(f)
        else:
            with tempfile.NamedTemporaryFile(prefix=self.resourcename,delete=False) as f:
                blob_data = self.get_blob_client(metadata["resource_path"]).download_blob().readinto(f)
                filename = f.name

        return filename
        

    def update(self,blob_data):
        """
        Update the blob data
        if blob_data is None, delete the blob resource
        """
        if blob_data is None:
            #delete the blob resource
            self.delete()
        else:
            if not isinstance(blob_data,bytes):
                #blob_data is not byte array, convert it to json string
                raise Exception("Updated data must be bytes type.")
            #self._blob_client.stage_block("main",blob_data)
            #self._blob_client.commit_block_list(["main"])
            self._blob_client.upload_blob(blob_data,overwrite=True,timeout=3600)

class AzureJsonBlob(AzureBlob):
    """
    A blob client to get/update a json blob resource
    """
    @property
    def json(self):
        """
        Return resource data as dict object.
        Return None if resource is not found
        """
        try:
            data = self._blob_client.download_blob().readall()
            return json.loads(data.decode(),cls=JSONDecoder)
        except ResourceNotFoundError as e:
            #blob not found
            return None

    def update(self,blob_data):
        """
        Update the blob data
        """
        blob_data = {} if blob_data is None else blob_data
        if not isinstance(blob_data,bytes):
            #blob_data is not byte array, convert it to json string and encode it to byte array
            blob_data = json.dumps(blob_data,cls=JSONEncoder).encode()
        super().update(blob_data)

class MetaMetadataMixin(object):
    def __init__(self,*args,**kwargs):
        super().__init__(*args,**kwargs)
        if self._resource_base_path:
            meta_metadata_filepath = "{}/meta_metadata.json".format(self._resource_base_path)
        else:
            meta_metadata_filepath = "meta_metadata.json"

        self._meta_metadata_client = AzureJsonBlob(meta_metadata_filepath,self._connection_string,self._container_name)

        self._update_meta_metadata()
    
    def _update_meta_metadata(self):
        meta_metadata_json = self._meta_metadata_client.json
        current_meta_metadata_json = {
            "class":self.__class__.__name__,
            "kwargs":{}
        }
        for (k,p) in self.meta_metadata_kwargs:
            current_meta_metadata_json["kwargs"][k] = getattr(self,p)

        if meta_metadata_json and meta_metadata_json == current_meta_metadata_json:
            #meta meta data is not changed
            logger.debug("Meta metadata({}) is up to date".format(self._meta_metadata_client._blob_path))
            return

        self._meta_metadata_client.update(current_meta_metadata_json)
        logger.debug("Update the meta metadata({}) successfully".format(self._meta_metadata_client._blob_path))

class AzureBlobMetadataBase(AzureJsonBlob):
    """
    A client to get/create/update a blob resource's metadata
    metadata is a json object.
    """
    meta_metadata_kwargs = [("metaname","_metaname"),("resource_base_path","_resource_base_path")]
    def __init__(self,connection_string,container_name,resource_base_path=None,cache=False,metaname="metadata"):
        self._metaname = metaname or "metadata"
        metadata_file = "{}.json".format(self._metaname) 
        self._resource_base_path = resource_base_path
        if resource_base_path:
            metadata_filepath = "{}/{}".format(resource_base_path,metadata_file)
        else:
            metadata_filepath = metadata_file

        super().__init__(metadata_filepath,connection_string,container_name)
        self._cache = cache

    @property
    def metaname(self):
        return self._metaname

    @property
    def json(self):
        """
        Return the resource's meta data as dict object.
        Return None if resource's metadata is not found
        """
        if self._cache and hasattr(self,"_json"):
            #json data is already cached
            return self._json

        json_data = super().json

        if self._cache and json_data is not None:
            #cache the json data
            self._json = json_data

        return json_data

    def update(self,metadata):
        if not metadata:
            #metadata is empty, delete the metedata
            self.delete()
            return

        logger.debug("Update the meta file '{}'".format(self._blob_path))
        super().update(metadata)
        if self._cache:
            #cache the result
            self._json = metadata

    def delete(self):
        logger.debug("Delete the meta file '{}'".format(self._blob_path))
        super().delete()
        if self._cache:
            self._json = None

class AzureBlobMetadataIndex(AzureBlobMetadataBase):
    """
    The class for index meta file
    """
    def __init__(self,connection_string,container_name,resource_base_path=None,cache=False,index_metaname="_metadata_index"):
        super().__init__(connection_string,container_name,resource_base_path=resource_base_path,cache=cache,metaname=index_metaname)

    def add_metafile(self,metaname,metadata_filepath):
        """
        Add a individual meta file to the index meta file
        """
        index_json = self.json

        if index_json is None:
            index_json = [[metaname,metadata_filepath]]
        else:
            data = next((m for m in index_json if m[0] == metaname),None)
            if data:
                #already exist
                return
            else:
                #doesn't exist
                index_json.append([metaname,metadata_filepath])

        self.update(index_json)

    def remove_metafile(self,metaname):
        """
        remove this metadata file from index metadata file
        """
        index_json = self.json
        if not index_json :
            return
        else:
            #find the index of the metaname;
            index = len(index_json) - 1
            while index >= 0:
                if index_json[index][0] == metaname:
                    break
                else:
                    index -= 1

            if index >= 0:
                #found,remove it
                del index_json[index]
                #update it
                if index_json:
                    #still have some other individual meta files
                    self.update(index_json)
                else:
                    #no more individual metadata files, remove the index metadata file
                    self.delete()
            else:
                #not found
                return

class AzureBlobIndexedMetadata(AzureBlobMetadataIndex):
    """
    A class to implement indexed meta file which include index meta file and indiviudal meta files
    """
    meta_metadata_kwargs = [("resource_base_path","_resource_base_path"),("index_metaname","_metaname"),("archive","_archive"),('f_metaname','_f_metaname')]
    metaclient_class = None
    def __init__(self,connection_string,container_name,f_metaname,resource_base_path=None,cache=False,archive=False,index_metaname="_metadata_index"):
        super().__init__(connection_string,container_name,resource_base_path=resource_base_path,cache=cache,index_metaname=index_metaname)
        self._cache = cache
        self._archive = archive
        self._f_metaname = f_metaname
        self._metadata_client = None
        self._metaname = None

    @property
    def metadata_client(self):
        """
        Return the individual meta file against the current metaname
        Return None if current metaname is None
        """
        if self._metaname:
            if not self._metadata_client or self._metadata_client._metaname != self._metaname:
                self._meta_client = self.metaclient_class(self._connection_string,self._container_name,resource_base_path=self._resource_base_path,cache=self._cache,metaname=self._metaname,archive=self._archive)
            return self._meta_client
        else:
            return None


    def resource_metadatas(self,throw_exception=True,**kwargs):
        """
        kwargs should be 'resource_file' or the keys in resource_keys
        if kwargs['resource_file'] is None, navigate the resource's metadata
        if kwargs['resource_file'] is not None, navigate the pushed resource's metadata
        throw_exception: if True, throw exception if resource not found; otherwise return empty generator
        Return a generator to navigate the filtered meta datas
        
        """
        unknown_args = [a for a in kwargs.keys() if a not in self.resource_keys and a not in ("resource_file",)]
        if unknown_args:
            raise Exception("Unsupported keywords arguments({})".format(unknown_args))

        if self.resource_keys[0] not in kwargs:
            #return all resource metadata
            index_json = self.json
            if index_json:
                for metaname,metapath in index_json:
                    meta_client = self.metaclient_class(self._connection_string,self._container_name,resource_base_path=self._resource_base_path,cache=False,metaname=metaname,archive=self._archive)
                    for metadata in meta_client.resource_metadatas(throw_exception=throw_exception):
                        yield metadata

        else:
            self._metaname = self._f_metaname(kwargs[self.resource_keys[0]])
            for metadata in self.metadata_client.resource_metadatas(throw_exception=throw_exception,**kwargs):
                yield metadata

    def get_resource_metadata(self,*args,resource_file="current"):
        """
        Return resource's metadata or pushed resource's metadata if resource_file is not None; if not exist, throw exception
        """
        self._metaname = self._f_metaname(args[0])

        return self.metadata_client.get_resource_metadata(*args,resource_file=resource_file)

    def remove_resource(self,*args):
        """
        Remove the resource's metadata. 
        Return the metadata of the remove resource; if not found, return None
        """
        self._metaname = self._f_metaname(args[0])
        metadata = self.metadata_client.remove_resource(*args)
        if metadata:
            #resource is deleted, delete the metadata file from indexed metadata if the metadata file is deleted
            if self.metadata_client.json is None:
                #metadata file was deleted,remove it from indexed file
                self.remove_metafile(self._metaname)
        return metadata

    def update_resource(self,resource_metadata):
        """
        Add or update the resource's metadata
        Return a tuple(the whole  metadata,created?)
        """
        self._metaname = self._f_metaname(resource_metadata[self.resource_keys[0]])
        result = self.metadata_client.update_resource(resource_metadata)
        if result[1]:
            #new created, add the metafile to indexed file if not exist before
            self.add_metafile(self._metaname,self.metadata_client._blob_path)
        
        return result


class AzureBlobResourceMetadataBase(AzureBlobMetadataBase):
    """
    A client to get/create/update a blob resource's metadata
    metadata is a json object.
    """
    #The resource keys in metadata used to identify a resource
    resource_keys =  []

    meta_metadata_kwargs = [("metaname","_metaname"),("resource_base_path","_resource_base_path"),("archive","_archive")]
    def __init__(self,connection_string,container_name,resource_base_path=None,cache=False,metaname="metadata",archive=False):
        super().__init__(connection_string,container_name,resource_base_path=resource_base_path,cache=cache,metaname=metaname)
        self._archive = True if archive else False

    def _get_pushed_resource_metadata(self,metadata,resource_file="current"):
        """
        get metadata from resource's metadata against resource_file
        resource_file:  
        """
        if self._archive and resource_file:
            if metadata.get("current",{}).get("resource_file") and (metadata.get("current",{}).get("resource_file") == resource_file or resource_file == "current"):
                return metadata["current"]

            if metadata.get("histories") and resource_file != "current":
                try:
                    return next(m for m in metadata.get("histories",[]) if m["resource_file"] == resource_file)
                except StopIteration as ex:
                    raise exceptions.ResourceNotFound("Resource({},resource_file={}) Not Found".format(".".join(metadata[k] for k in self.resource_keys),resource_file))
            else:
                raise exceptions.ResourceNotFound("Resource({},resource_file={}) Not Found".format(".".join(metadata[k] for k in self.resource_keys),resource_file))
        else:
            return metadata

    def resource_metadatas(self,throw_exception=True,**kwargs):
        """
        Return a generator to navigate the metadata of all pushed individual resources or specified resource; if not exist, return a empty generator
        if kwargs['resource_file'] is None, navigate the resource's metadata
        if kwargs['resource_file'] is not None, navigate the pushed resource's metadata
        throw_exception: if True, throw exception if resource not found; otherwise return empty generator
        """
        unknown_args = [a for a in kwargs.keys() if a not in self.resource_keys and a not in ("resource_file",)]
        if unknown_args:
            raise Exception("Unsupported keywords arguments({})".format(unknown_args))

        metadata = self.json or {}
        index = 0
        while index < len(self.resource_keys):
            key = self.resource_keys[index]
            if kwargs.get(key):
                index += 1
                if metadata.get(kwargs[key]):
                    metadata = metadata[kwargs[key]]
                elif throw_exception:
                    raise exceptions.ResourceNotFound("Resource({}) Not Found".format(".".join(kwargs[k] for k in self.resource_keys[0:index])))
                else:
                    return
            else:
                break

        resource_file = kwargs.get("resource_file")
        if index == len(self.resource_keys):
            yield self._get_pushed_resource_metadata(metadata,resource_file)
        else:
            for m1 in metadata.values():
                if (index + 1) == len(self.resource_keys):
                    yield self._get_pushed_resource_metadata(m1,resource_file)
                else:
                    for m2 in m1.values():
                        if (index + 2) == len(self.resource_keys):
                            yield self._get_pushed_resource_metadata(m2,resource_file)
                        else:
                            for m3 in m2.values():
                                if (index + 3) == len(self.resource_keys):
                                    yield self._get_pushed_resource_metadata(m3,resource_file)
                                else:
                                    raise Exception("Not implemented")


    def get_resource_metadata(self,*args,resource_file="current"):
        """
        Return resource's metadata or pushed resource's metadata if resource_file is not None; if not exist, throw exception
        """
        return next(self.resource_metadatas(resource_file=resource_file,**dict(zip(self.resource_keys,args))))

    def remove_resource(self,*args):
        """
        Remove the resource's metadata. 
        Return the metadata of the removed resource; if not found, return None
        """
        metadata = self.json or {}
        p_metadata = metadata
        if len(self.resource_keys) != len(args):
            raise Exception("Invalid args({})".format(args))

        for key in args[:-1]:
            p_metadata = p_metadata.get(key)
            if not p_metadata:
                #not exist
                return None

        if args[-1] not in p_metadata:
            #not exist
            return None
        else:
            resource_metadata = p_metadata[args[-1]]
            del p_metadata[args[-1]]
         
            #delete the meta file if meta file is empty
            last_index = len(args) - 2
            while last_index >= 0:
                p_metadata = metadata
                if last_index > 0:
                    for key in args[0:last_index]:
                        p_metadata = p_metadata[key]
                if args[last_index] in p_metadata and not p_metadata[args[last_index]]:
                    del p_metadata[args[last_index]]
                last_index -= 1

            if metadata:
                self.update(metadata)
            else:
                self.delete()
        return resource_metadata

    def update_resource(self,resource_metadata):
        """
        Add or update the resource's metadata
        Return a tuple(the whole  metadata,created?)
        """
        metadata = self.json or {}
        exist_metadata = metadata
        existed = True
        for k in self.resource_keys:
            val = resource_metadata.get(k)
            if not val:
                raise Exception("Missing key({}) in resource metadata".format(k))
            if val not in exist_metadata:
                existed = False
                exist_metadata[val] = {}
            exist_metadata = exist_metadata[val]


        if self._archive:
            if existed:
                if exist_metadata.get("histories"):
                    exist_metadata["histories"].insert(0,exist_metadata["current"])
                else:
                    exist_metadata["histories"] = [exist_metadata["current"]]
            exist_metadata["current"] = resource_metadata
        else:
            exist_metadata.update(resource_metadata)
        self.update(metadata)
        return (metadata,not existed)

class BasicAzureBlobResourceMetadata(AzureBlobResourceMetadataBase):
    #The resource keys in metadata used to identify a resource
    resource_keys =  ["resource_id"]

class BasicAzureBlobGroupResourceMetadata(AzureBlobResourceMetadataBase):
    #The resource keys in metadata used to identify a resource
    resource_keys =  ["resource_group","resource_id"]

class AzureBlobResourceMetadata(MetaMetadataMixin,BasicAzureBlobResourceMetadata):
    pass

class AzureBlobGroupResourceMetadata(MetaMetadataMixin,BasicAzureBlobGroupResourceMetadata):
    pass

class AzureBlobIndexedResourceMetadata(MetaMetadataMixin,AzureBlobIndexedMetadata):
    metaclient_class = BasicAzureBlobResourceMetadata
    resource_keys = metaclient_class.resource_keys

class AzureBlobIndexedGroupResourceMetadata(MetaMetadataMixin,AzureBlobIndexedMetadata):
    metaclient_class = BasicAzureBlobGroupResourceMetadata
    resource_keys = metaclient_class.resource_keys


class AzureBlobResourceBase(object):
    """
    A base client to manage a Azure Resourcet
    """
    data_path = "data"
    def __init__(self,resource_name,connection_string,container_name,resource_base_path=None,archive=True):
        self._resource_name = resource_name
        self._resource_base_path = resource_name if resource_base_path is None else resource_base_path
        if self._resource_base_path:
            self._resource_data_path = "{}/{}".format(self._resource_base_path,self.data_path)
        else:
            self._resource_data_path = self.data_path
        self._connection_string = connection_string
        self._container_name = container_name
        self._archive = archive

    @property
    def resourcename(self):
        return self._resource_name

    @property
    def metadata_client(self):
        return self._metadata_client

    def _get_resource_file(self,resourceid):
        """
        Get a default resource file from resourceid
        for archived resource, each push will create another blob resource named by resource_file
        for non-archived resource,each push will create a new blob resource or update the exist resource, so resourceid is the same as resource_file
        """
        if self._archive:
            file_name,file_ext = os.path.splitext(resourceid)
            return "{0}_{1}{2}".format(file_name,timezone.now().strftime("%Y-%m-%d-%H-%M-%S"),file_ext)
        else:
            return resourceid

    def _get_resource_path(self,metadata):
        """
        Get the resoure path for resource_file
        resource path is the path in blob storage
        """
        if len(self._metadata_client.resource_keys) > 1:
            return "{0}/{1}/{2}".format(self._resource_data_path,"/".join(metadata[k] for k in self._metadata_client.resource_keys[:-1]),metadata["resource_file"])
        else:
            return "{0}/{1}".format(self._resource_data_path,metadata["resource_file"])

    def resource_metadatas(self,throw_exception=True,**kwargs):
        for m in self._metadata_client.resource_metadatas(throw_exception=throw_exception,**kwargs):
            yield m

    def get_resource_metadata(self,*args,resource_file="current"):
        """
        if resurce_file is "current", it means the latest archive of the specific resource; otherwise, it should be a resource's resource file ; only meaningful for archived resource
        throw exception if not found
        Return the resource's metadata
        """
        return self._metadata_client.get_resource_metadata(*args,resource_file=resource_file)


    def delete_resource(self,**kwargs):
        """
        delete the resource_group or specified resource 
        return the list of the metadata of deleted resources
        """
        unknown_args = [a for a in kwargs.keys() if a not in self._metadata_client.resource_keys and a not in ("throw_exception",)]
        if unknown_args:
            raise Exception("Unsupported keywords arguments({})".format(unknown_args))

        metadatas = [ m for m in self._metadata_client.resource_metadatas(resource_file=None,**kwargs)]
        for m in metadatas:
            self._delete_resource(m)

        return metadatas

    def _delete_resource(self,metadata):
        """
        The metadata of the specific resource you want to delete
        Delete the current archive and all histories archives for archive resource. 
        """
        logger.debug("Delete the resource({}.{})".format(self.resourcename,".".join(metadata[k] for k in self._metadata_client.resource_keys)))
        #delete the resource file from storage
        if self._archive:
            #archive resource
            #delete the current archive
            blob_client = self.get_blob_client(metadata["current"]["resource_path"])
            try:
                blob_client.delete_blob()
            except:
                logger.error("Failed to delete the current resource({}) from blob storage.{}".format(metadata["current"]["resource_path"],traceback.format_exc()))
            #delete all history arvhives
            for m in metadata.get("histroies") or []:
                blob_client = self.get_blob_client(m["resource_path"])
                try:
                    blob_client.delete_blob()
                except:
                    logger.error("Failed to delete the history resource({}) from blob storage.{}".format(m["resource_path"],traceback.format_exc()))

            
        else:
            blob_client = self.get_blob_client(metadata["resource_path"])
            try:
                blob_client.delete_blob()
            except:
                logger.error("Failed to delete the resource({}) from blob storage.{}".format(metadata["resource_path"],traceback.format_exc()))
            
        #remove the resource from metadata
        self._metadata_client.remove_resource(*[metadata[k] for k in self._metadata_client.resource_keys])
        

    def download_resources(self,folder=None,overwrite=False,**kwargs):
        """
        Only available for group resource
        """
        unknown_args = [a for a in kwargs.keys() if a not in self._metadata_client.resource_keys]
        if unknown_args:
            raise Exception("Unsupported keywords arguments({})".format(unknown_args))

        folder_exist = False
        if folder:
            if os.path.exists(folder):
                if not os.path.isdir(folder):
                    #is not a folder
                    raise Exception("The path({}) is not a folder.".format(folder))
                else:
                    folder_exist = True
            else:
                #create the folder
                os.makedirs(folder)
        else:
            folder = tempfile.mkdtemp(prefix=resource_group)

        if self._archive:
            kwargs["resource_file"] = "current"

        metadatas = [m for m in self._metadata_client.resource_metadatas(throw_exception=True,**kwargs)]
        for metadata in metadatas:
            if metadata.get("resource_file") and metadata.get("resource_path"):
                logger.debug("Download resource {}".format(metadata["resource_path"]))
                filename = os.path.join(folder,metadata["resource_file"])
                if folder_exist and os.path.exists(filename):
                    if not os.path.isfile(filename):
                        #is not a file
                        raise Exception("The path({}) is not a file.".format(filename))
                    elif not overwrite:
                        raise Exception("The file({}) already exists".format(filename))

                with open(filename,'wb') as f:
                    self.get_blob_client(metadata["resource_path"]).download_blob().readinto(f)

        return (metadatas,folder)

    def download_resource(self,*args,filename=None,overwrite=False):
        """
        Download the resource with resourceid, and return the filename 
        remove the existing file or folder if overwrite is True
        """
        if filename:
            if os.path.exists(filename):
                if not os.path.isfile(filename):
                    #is a folder
                    raise Exception("The path({}) is not a file.".format(filename))
                elif not overwrite:
                    #already exist and can't overwrite
                    raise Exception("The file({}) already exists".format(filename))
        
        metadata = self.get_resource_metadata(*args,resource_file="current")
    
        logger.debug("Download resource {}".format(metadata["resource_path"]))
        if not filename:
            with tempfile.NamedTemporaryFile(suffix="_{}".format(os.path.split(metadata["resource_file"])[1]),delete=False) as f:
                self.get_blob_client(metadata["resource_path"]).download_blob().readinto(f)
                filename = f.name
        else:
            with open(filename,'wb') as f:
                self.get_blob_client(metadata["resource_path"]).download_blob().readinto(f)

        return (metadata,filename)

    def get_blob_client(self,resource_path):
        return BlobClient.from_connection_string(self._connection_string,self._container_name,resource_path,**settings.AZURE_BLOG_CLIENT_KWARGS)


    def push_resource(self,data,metadata,f_post_push=None,length=None):
        """
        Push the resource to the storage
        f_post_push: a function to call after pushing resource to blob container but before pushing the metadata, has one parameter "metadata"
        Return the new resourcemetadata.
        """
        #populute the latest resource metadata
        for key in self._metadata_client.resource_keys:
            if key not in metadata:
                raise Exception("Missing resource key({}) in metadata".format(key))

        if "resource_file" not in metadata:
            metadata["resource_file"] = self._get_resource_file(metadata["resource_id"])
        metadata["resource_path"] = self._get_resource_path(metadata)     
        metadata["publish_date"] = timezone.now()

        #push the resource to azure storage
        blob_client = self.get_blob_client(metadata["resource_path"])
        logger.debug("Push the resource({}.{}) to blob storage.".format(metadata["resource_id"],metadata["resource_path"]))
        blob_client.upload_blob(data,blob_type=BlobType.BlockBlob,overwrite=True,timeout=3600,max_concurrency=5,length=length)
        #update the resource metadata
        if f_post_push:
            f_post_push(metadata)

        self._metadata_client.update_resource(metadata)

        return self._metadata_client.json
        
class AzureBlobResource(AzureBlobResourceBase,ResourceStorage):
    def __init__(self,resource_name,connection_string,container_name,resource_base_path=None,archive=True,metaname="metadata",cache=True):
        super().__init__(resource_name,connection_string,container_name,resource_base_path=resource_base_path,archive=archive)
        self._metadata_client = AzureBlobResourceMetadata(connection_string,container_name,resource_base_path=self._resource_base_path,cache=cache,metaname=metaname,archive=archive)

class AzureBlobGroupResource(AzureBlobResourceBase,ResourceStorage):
    def __init__(self,resource_name,connection_string,container_name,resource_base_path=None,archive=True,metaname="metadata",cache=True):
        super().__init__(resource_name,connection_string,container_name,resource_base_path=resource_base_path,archive=archive)
        self._metadata_client = AzureBlobGroupResourceMetadata(connection_string,container_name,resource_base_path=self._resource_base_path,cache=cache,metaname=metaname,archive=archive)



class AzureBlobIndexedResource(AzureBlobResourceBase,ResourceStorage):
    def __init__(self,resource_name,connection_string,container_name,f_metaname,resource_base_path=None,archive=True,index_metaname="_metadata_index",cache=True):
        super().__init__(resource_name,connection_string,container_name,resource_base_path=resource_base_path,archive=archive)
        self._metadata_client = AzureBlobIndexedResourceMetadata(connection_string,container_name,f_metaname,resource_base_path=self._resource_base_path,cache=cache,archive=archive,index_metaname=index_metaname)

    def push_resource(self,data,metadata,f_post_push=None,length=None):
        super().push_resource(data,metadata,f_post_push=f_post_push,length=length)
        return self._metadata_client.metadata_client.json

class AzureBlobIndexedGroupResource(AzureBlobResourceBase,ResourceStorage):
    def __init__(self,resource_name,connection_string,container_name,f_metaname=None,resource_base_path=None,archive=True,index_metaname="_metadata_index",cache=True):
        super().__init__(resource_name,connection_string,container_name,resource_base_path=resource_base_path,archive=archive)
        self._metadata_client = AzureBlobIndexedGroupResourceMetadata(connection_string,container_name,f_metaname,resource_base_path=self._resource_base_path,cache=cache,archive=archive,index_metaname=index_metaname)


    def push_resource(self,data,metadata,f_post_push=None,length=None):
        super().push_resource(data,metadata,f_post_push=f_post_push,length=length)
        return self._metadata_client.metadata_client.json



class AzureBlobResourceClients(AzureBlobResourceBase):
    """
    A client to track the non group resource consuming status of a client
    Incompleted and not tested
    """

    data_path = "clients"
    def __init__(self,resource_name,connection_string,container_name,resource_base_path=None):
        super().__init__(resource_name,connection_string,container_name,resource_base_path=resource_base_path,archive=False)
        self._metadata_client = BasicAzureBlobResourceMetadata(connection_string,container_name,resource_base_path=self._resource_base_path,cache=False,metaname="clients_metadata",archive=False)

        if self._resource_base_path:
            meta_metadata_filepath = "{}/meta_metadata.json".format(self._resource_base_path)
        else:
            meta_metadata_filepath = "meta_metadata.json"

        self._meta_metadata_client = AzureJsonBlob(meta_metadata_filepath,self._connection_string,self._container_name)
        self._blob_resource_client = self._get_blob_resource_client()


    def _get_blob_resource_client(self):
        meta_metadata_json = self._meta_metadata_client.json
        if not meta_metadata_json:
            raise exceptions.MetaMetadataMissing(slef._resource_name,self._meta_metadata_client._blob_path)

        blob_resource_class = next(cls for cls,meta_cls in [
            (AzureBlobResource,AzureBlobResourceMetadata),
            (AzureBlobGroupResource,AzureBlobGroupResourceMetadata),
            (AzureBlobIndexedResource,AzureBlobIndexedResourceMetadata),
            (AzureBlobIndexedGroupResource,AzureBlobIndexedGroupResourceMetadata)] if meta_cls.__name__== meta_metadata_json["class"])
        return blob_resource_class(self._resource_name,self._connection_string,self._container_name,cache=False,**meta_metadata_json["kwargs"])


    def get_client_metadatas(self,throw_exception=True,**kwargs):
        for m in self._metadata_client.resource_metadatas(throw_exception=throw_exception,**kwargs):
            yield m

    def get_client_metadata(self,clientid,resource_file="current"):
        return self.get_resource_metadata(clientid)

    def delete_clients(self,clientid=None):
        return self.delete_resource(resource_id = clientid)

    def is_client_exist(self,clientid):
        """
        Check whether client exists or not
        """
        try:
            return True if self.get_resource_metadata(clientid,resource_file=None) else False
        except exceptions.ResourceNotFound as ex:
            return False
        
    def get_client_consume_status(self,clientid):
        if self.is_client_exist(clientid):
            metadata,filename = self.download_resource(clientid)
            try:
                with open(filename,'r') as f:
                    return json.loads(f.read(),cls=JSONDecoder)
            finally:
                os.remove(filename)
        else:
            return None


class AzureBlobResourceClient(AzureBlobResourceClients):
    NOT_CHANGED = 0
    NEW = 1
    UPDATED = 2
    DELETED = 3
  

    def __init__(self,resource_name,connection_string,container_name,clientid,resource_base_path=None):
        super().__init__(resource_name,connection_string,container_name,resource_base_path=resource_base_path)
        self._clientid = clientid

    @property
    def consume_status(self):
        return self.get_client_consume_status(self._clientid)


    def get_resource_consume_status(self,*args,consume_status=None):
        """
        Get the resource consume status; return None if not consumed before.
        """
        consume_status = self.consume_status if consume_status is None else consume_status
        if not consume_status:
            return None
    
        parent_status = consume_status
        for arg in args[:-1]:
            parent_status = parent_status.get(arg,None)
            if not parent_status:
                return None

        return parent_status.get(args[-1],None)

    def set_resource_consume_status(self,*args,res_consume_status,consume_status=None):
        """
        Set the resource consume status; 
        Return the updated consume status
        """
        consume_status = self.consume_status if consume_status is None else consume_status
        if consume_status is None:
            consume_status = {}
    
        parent_status = consume_status
        for arg in args[:-1]:
            if arg not in parent_status:
                parent_status[arg] = {}
            parent_status = parent_status[arg]
                
        parent_status[args[-1]] = res_consume_status
        return consume_status


    def remove_resource_consume_status(self,*args,consume_status=None):
        """
        Remove the resource consume status; 
        return the updated consume status
        """
        consume_status = self.consume_status if consume_status is None else consume_status

        if not consume_status:
            return
    
        parent_status = consume_status
        for arg in args[:-1]:
            if arg not in parent_status:
                #not exist
                return
            parent_status = parent_status[arg]
       
        if args[-1] in parent_status:
            del parent_status[args[-1]]

        return consume_status

    def is_behind(self,resource_ids_list=None):
        """
        resource_ids_list: the list of resource id for consuming.
        Return True if some resource is changed after last consuming;otherwise return False
        """
        client_consume_status = self.consume_status
        if client_consume_status is None:
            client_consume_status = {}

        resource_keys = self._blob_resource_client._metadata_client.resource_keys
        if resource_ids_list:
            #Consume specified resources in order
            for resource_ids in resource_ids_list:
                try:
                    if not isinstance(resource_ids,(list,tuple)):
                        resource_ids = [resource_ids]
                    res_consume_status = self.get_resource_consume_status(*resource_ids,consume_status=client_consume_status)
                    res_meta = self._blob_resource_client.get_resource_metadata(*resource_ids)
                except exceptions.ResourceNotFound as ex:
                    if res_consume_status:
                        #this resource was consuemd before and now it was deleted
                        logger.debug("Found a deleted resource({},{})".format(resource_ids,res_consume_status["resource_metadata"]["resource_path"]))
                        return True
                    else:
                        #this resource was not conusmed and also it doesn't exist
                        logger.warning("The resource({}) doesn't exist".format(resource_ids))
                    continue

                if not res_consume_status:
                    #new resource
                    logger.debug("Found a new resource({},{})".format(resource_ids,res_meta["resource_path"]))
                    return True
                elif res_meta != res_consume_status["resource_metadata"]:
                    #resource was changed
                    logger.debug("Found a updated resource({},{})".format(resource_ids,res_meta["resource_path"]))
                    return True
                else:
                    #reosurce was consumed before
                    logger.debug("The resource({},{}) is not changed after last consuming".format(resource_ids,res_meta["resource_path"]))
                    continue
        else:
            #find new and updated resources
            checked_resources = set()
            for res_meta in self._blob_resource_client.resource_metadatas(throw_exception=False):
                resource_ids = tuple(res_meta[key] for key in resource_keys)
                checked_resources.add(resource_ids)
                res_consume_status = self.get_resource_consume_status(*resource_ids,consume_status=client_consume_status)
                if not res_consume_status:
                    logger.debug("Found a new resource({},{})".format(resource_ids,res_meta["resource_path"]))
                    return True
                elif res_meta != res_consume_status["resource_metadata"]:
                    #resource was changed
                    logger.debug("Found a updated resource({},{})".format(resource_ids,res_meta["resource_path"]))
                    return True
                else:
                    #reosurce was consumed before
                    logger.debug("The resource({},{}) is not changed after last consuming".format(resource_ids,res_meta["resource_path"]))
                    continue
    
            #find deleted resources
            level = 1
            for val in client_consume_status.values():
                if level == len(resource_keys):
                    resource_ids = tuple(val["resource_metadata"][key] for key in resource_keys)
                    if resource_ids in checked_resources:
                        continue
                    else:
                        logger.debug("Found a deleted resource({})".format(resource_ids))
                        return True
                else:
                    level += 1
                    for val2 in val.values():
                        if level == len(resource_keys):
                            resource_ids = tuple(val2["resource_metadata"][key] for key in resource_keys)
                            if resource_ids in checked_resources:
                                continue
                            else:
                                logger.debug("Found a deleted resource({})".format(resource_ids))
                                return True
                        else:
                            level += 1
                            for val3 in val2.values():
                                if level == len(resource_keys):
                                    resource_ids = tuple(val3["resource_metadata"][key] for key in resource_keys)
                                    if resource_ids in checked_resources:
                                        continue
                                    else:
                                        logger.debug("Found a deleted resource({})".format(resource_ids))
                                        return True
                                else:
                                    raise Exception("Not implemented")


    def consume(self,callback,resource_ids_list=None,reconsume=False):
        """
        resource_ids_list: the list of resource id for consuming.
        callback: two mode
            callback per resource,callback's parameters is : resource_status,res_meta,res_file
            callback for all resource, callback's parameter is list of [resource_status,res_meta,res_file]
        Return the number of some resources which have been consumed; otherwise return 0
        """
        f_args = inspect.getfullargspec(callback)
        if len(f_args.args) == 1:
            callback_per_resource = False
        elif len(f_args.args) == 3:
            callback_per_resource = True
        else:
            raise Exception("Callback should have one parameter(list of tuple(resource_status,resource_metadata,file_name) to run in batch mode ,or have three parameters (resource_status,resource_metadata,file_name) to run in callback per resource mode")

        client_consume_status = self.consume_status
        if client_consume_status is None:
            client_consume_status = {}

        resource_status = self.NOT_CHANGED
        metadata = {
            "resource_id":self._clientid,
            "last_consume_host":socket.getfqdn(),
            "last_consume_pid":os.getpid()
        }
        resource_keys = self._blob_resource_client._metadata_client.resource_keys
        consumed = 0
        updated_resources = []
        if resource_ids_list:
            #Consume specified resources in order
            for resource_ids in resource_ids_list:
                try:
                    if not isinstance(resource_ids,(list,tuple)):
                        resource_ids = [resource_ids]
                    res_consume_status = self.get_resource_consume_status(*resource_ids,consume_status=client_consume_status)
                    res_meta = self._blob_resource_client.get_resource_metadata(*resource_ids)
                except exceptions.ResourceNotFound as ex:
                    if res_consume_status:
                        #this resource was consuemd before and now it was deleted
                        logger.debug("Consume the deleted resource({},{})".format(resource_ids,res_consume_status["resource_metadata"]["resource_path"]))
                        self.remove_resource_consume_status(*resource_ids,consume_status=client_consume_status)
                        consumed += 1
                        metadata["last_consumed_resource"] = resource_ids
                        metadata["last_consumed_resource_status"] = "Deleted"
                        metadata["last_consume_date"] = timezone.now()
                        if callback_per_resource:
                            callback(self.DELETED,res_consume_status["resource_metadata"],None)
                            self.push_resource(json.dumps(client_consume_status,cls=JSONEncoder).encode(),metadata=metadata)
                        else:
                            updated_resources.append((self.DELETED,res_consume_status["resource_metadata"],None))
                    else:
                        #this resource was not conusmed and also it doesn't exist
                        logger.warning("The resource({}) doesn't exist".format(resource_ids))
                    continue

                if not res_consume_status:
                    #new resource
                    resource_status = self.NEW
                    res_consume_status = {
                        "resource_metadata":res_meta,
                        "resource_status":"New",
                        "consume_date":timezone.now()
                    }
                    self.set_resource_consume_status(*resource_ids,res_consume_status = res_consume_status,consume_status=client_consume_status)
                    logger.debug("Consume the new resource({},{})".format(resource_ids,res_meta["resource_path"]))
                elif res_meta != res_consume_status["resource_metadata"]:
                    #resource was changed
                    resource_status = self.UPDATED
                    res_consume_status["resource_metadata"] = res_meta
                    res_consume_status["resource_status"] = "Update"
                    res_consume_status["consume_date"] = timezone.now()
                    logger.debug("Consume the updated resource({},{})".format(resource_ids,res_meta["resource_path"]))
                elif reconsume:
                    #resource was not changed
                    resource_status = self.NOT_CHANGED
                    res_consume_status["resource_metadata"] = res_meta
                    res_consume_status["resource_status"] = "Reconsume"
                    res_consume_status["consume_date"] = timezone.now()
                    logger.debug("Reconsume the resource({},{})".format(resource_ids,res_meta["resource_path"]))
                else:
                    #reosurce was consumed before
                    logger.debug("The resource({},{}) is not changed after last consuming".format(resource_ids,res_meta["resource_path"]))
                    continue
    
                consumed += 1
                res_file = self._blob_resource_client.download_resource(*resource_ids)[1]
                metadata["last_consumed_resource"] = resource_ids
                metadata["last_consumed_resource_status"] = "New" if resource_status == self.NEW else "Updated"
                metadata["last_consume_date"] = timezone.now()
                if callback_per_resource:
                    try:
                        callback(resource_status,res_meta,res_file)
                        self.push_resource(json.dumps(client_consume_status,cls=JSONEncoder).encode(),metadata=metadata)
                    finally:
                        remove_file(res_file)
                else:
                    updated_resources.append((resource_status,res_meta,res_file))

        else:
            #find new and updated resources
            checked_resources = set()
            for res_meta in self._blob_resource_client.resource_metadatas(throw_exception=False):
                resource_ids = tuple(res_meta[key] for key in resource_keys)
                checked_resources.add(resource_ids)
                res_consume_status = self.get_resource_consume_status(*resource_ids,consume_status=client_consume_status)
                if not res_consume_status:
                    #new resource
                    resource_status = self.NEW
                    res_consume_status = {
                        "resource_metadata":res_meta,
                        "resource_status":"New",
                        "consume_date":timezone.now()
                    }
                    self.set_resource_consume_status(*resource_ids,res_consume_status = res_consume_status,consume_status=client_consume_status)
                    logger.debug("Consume the new resource({},{})".format(resource_ids,res_meta["resource_path"]))
                elif res_meta != res_consume_status["resource_metadata"]:
                    #resource was changed
                    resource_status = self.UPDATED
                    res_consume_status["resource_metadata"] = res_meta
                    res_consume_status["resource_status"] = "Update"
                    res_consume_status["consume_date"] = timezone.now()
                    logger.debug("Consume the updated resource({},{})".format(resource_ids,res_meta["resource_path"]))
                elif reconsume:
                    #resource was not changed
                    resource_status = self.NOT_CHANGED
                    res_consume_status["resource_metadata"] = res_meta
                    res_consume_status["resource_status"] = "Reconsume"
                    res_consume_status["consume_date"] = timezone.now()
                    logger.debug("Reconsume the resource({},{})".format(resource_ids,res_meta["resource_path"]))
                else:
                    #reosurce was consumed before
                    logger.debug("The resource({},{}) is not changed after last consuming".format(resource_ids,res_meta["resource_path"]))
                    continue
                
                consumed += 1
                res_file = self._blob_resource_client.download_resource(*resource_ids)[1]
                metadata["last_consumed_resource"] = resource_ids
                metadata["last_consumed_resource_status"] = "New" if resource_status == self.NEW else "Updated"
                metadata["last_consume_date"] = timezone.now()
                if callback_per_resource:
                    try:
                        callback(resource_status,res_meta,res_file)
                        self.push_resource(json.dumps(client_consume_status,cls=JSONEncoder).encode(),metadata=metadata)
                    finally:
                        remove_file(res_file)
                else:
                    updated_resources.append((resource_status,res_meta,res_file))
    
    
            #find deleted resources
            level = 1
            deleted_resources = []
            for val in client_consume_status.values():
                level = 1
                if level == len(resource_keys):
                    resource_ids = tuple(val["resource_metadata"][key] for key in resource_keys)
                    if resource_ids in checked_resources:
                        continue
                    else:
                        deleted_resources.append(resource_ids)
                else:
                    level += 1
                    for val2 in val.values():
                        if level == len(resource_keys):
                            resource_ids = tuple(val2["resource_metadata"][key] for key in resource_keys)
                            if resource_ids in checked_resources:
                                continue
                            else:
                                deleted_resources.append(resource_ids)
                        else:
                            level += 1
                            for val3 in val2.values():
                                if level == len(resource_keys):
                                    resource_ids = tuple(val3["resource_metadata"][key] for key in resource_keys)
                                    if resource_ids in checked_resources:
                                        continue
                                    else:
                                        deleted_resources.append(resource_ids)
                                else:
                                    raise Exception("Not implemented")
            if deleted_resources:
                consumed += 1
                for resource_ids in deleted_resources:
                    res_consume_status = self.get_resource_consume_status(*resource_ids,consume_status=client_consume_status)
                    logger.debug("Consume the deleted resource({},{})".format(resource_ids,res_consume_status["resource_metadata"]["resource_path"]))
                    self.remove_resource_consume_status(*resource_ids,consume_status=client_consume_status)
                    metadata["last_consumed_resource"] = resource_ids
                    metadata["last_consumed_resource_status"] = "Deleted"
                    metadata["last_consume_date"] = timezone.now()
                    if callback_per_resource:
                        callback(self.DELETED,res_consume_status["resource_metadata"],None)
                        self.push_resource(json.dumps(client_consume_status,cls=JSONEncoder).encode(),metadata=metadata)
                    else:
                        updated_resources.append((self.DELETED,res_consume_status["resource_metadata"],None))

        if not callback_per_resource and updated_resources:
            try:
                callback(updated_resources)
                self.push_resource(json.dumps(client_consume_status,cls=JSONEncoder).encode(),metadata=metadata)
            finally:
                for res_status,res_meta,res_file in updated_resources:
                    remove_file(res_file)
                
        return consumed


