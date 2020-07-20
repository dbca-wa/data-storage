import json
import inspect
import tempfile
import logging
import os
import socket
import traceback

from . import settings
from . import exceptions

from .utils import JSONEncoder,JSONDecoder,timezone,remove_file,file_size

logger = logging.getLogger(__name__)

class ResourceConstant(object):
    NORMAL_RESOURCE = 1
    DELETED_RESOURCE = 2
    ALL_RESOURCE = NORMAL_RESOURCE | DELETED_RESOURCE

    DELETED_KEY = "deleted"

class Storage(object):
    """
    A resource storage
    provide read/delete/download/update/copy a resource
    """
    #indicate whether copying a file is supported or not.
    def get_content(self,path):
        """
        read the content of the resource from storage
        """
        raise NotImplementedError("Method 'read' is not implemented.")

    def get_text(self,path):
        """
        read the content of the resource from storage
        """
        return self.get_content(path).decode()

    def delete(self,path):
        """
        Delete the resource from storage
        """
        raise NotImplementedError("Method 'delete' is not implemented.")

    def download(self,path,filename):
        """
        Download the blob resource to a file
        """
        raise NotImplementedError("Method 'download' is not implemented.")

    def update(self,path,byte_list):
        """
        Update the resource's data in bytes.
        byte_list must be not empty
        """
        raise NotImplementedError("Method 'update' is not implemented.")

    def upload(self,path,data_stream,length=None):
        """
        Update the resource's data in bytes.
        data_stream must be not empty
        """
        raise NotImplementedError("Method 'update' is not implemented.")

    def upload_file(self,path,sourcepath):
        """
        upload a file to path
        """
        raise NotImplementedError("Method 'upload' is not implemented.")

    def list_resources(self,folder=None):
        """
        List files in the folder
        """
        return [m for m in self._container_client.list_container(name_starts_with=folder)]
            

class Resource(object):
    """
    manage a resource in storage.
    """
    def __init__(self,storage,resource_path):
        self._storage = storage
        self._resource_path = resource_path

    def download(self,filename=None,overwrite=False):
        """
        Download the resource to a local file
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
            
        else:
            with tempfile.NamedTemporaryFile(prefix="resource_repository",delete=False) as f:
                filename = f.name

        self._storage.download(self._resource_path,filename)

        return filename


    def update(self,byte_list):
        """
        Update the blob data of the resource
        if byte_list is None, delete the resource from storage
        """
        if byte_list is None:
            #delete the blob resource
            self._storage.delete(self._resource_path)
        else:
            if not isinstance(byte_list,bytes):
                #byte_list is not byte array, convert it to json string
                raise Exception("Updated data must be bytes type.")
            self._storage.update(self._resource_path,byte_list)

    def upload(self,filename):
        if not os.path.exists(filename):
            raise Exception("File({}) Not Found".format(filename))

        self._storage.upload_file(self._resource_path,filename)
            

    def get_content(self):
        """
        Read the resource content
        """
        return self._storage.get_content(self._resource_path)

    def get_text(self):
        """
        Read the resource content
        """
        return self._storage.get_text(self._resource_path)

    def delete(self):
        """
        Delete the resource
        """
        self._storage.delete(self._resource_path)



class JsonResource(Resource):
    """
    manage a json resource in storage.
    """
    @property
    def json(self):
        """
        Return resource data as dict object.
        Return None if resource is not found
        """
        try:
            return json.loads(self.get_text(),cls=JSONDecoder)
        except exceptions.ResourceNotFound as e:
            #blob not found
            return None

    def update(self,byte_list):
        """
        Update the resource data
        """
        byte_list = {} if byte_list is None else byte_list
        if not isinstance(byte_list,bytes):
            #byte_list is not byte array, convert it to json string and encode it to byte array
            byte_list = json.dumps(byte_list,cls=JSONEncoder,sort_keys=True,indent=4).encode()
        super().update(byte_list)

class ResourceRepositoryMetaMetadataMixin(object):
    """
    manage the meta metadata file
    """
    def __init__(self,*args,**kwargs):
        super().__init__(*args,**kwargs)
        if self._resource_base_path:
            meta_metadata_filepath = "{}/meta_metadata.json".format(self._resource_base_path)
        else:
            meta_metadata_filepath = "meta_metadata.json"

        self._meta_metadata_client = JsonResource(self._storage,meta_metadata_filepath)

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
            logger.debug("Meta metadata({}) is up to date".format(self._meta_metadata_client._resource_path))
            return

        self._meta_metadata_client.update(current_meta_metadata_json)
        logger.debug("Update the meta metadata({}) successfully".format(self._meta_metadata_client._resource_path))

class MetadataBase(JsonResource):
    """
    manage resource repository's  meta data
    metadata is a json object.
    """
    meta_metadata_kwargs = [("metaname","_metaname"),("resource_base_path","_resource_base_path"),("logical_delete","_logical_delete")]
    def __init__(self,storage,resource_base_path=None,cache=False,metaname="metadata",logical_delete=False):
        self._metaname = metaname or "metadata"
        metadata_file = "{}.json".format(self._metaname) 
        self._resource_base_path = resource_base_path
        if self._resource_base_path:
            if self._resource_base_path[0] == "/":
                self._resource_base_path = self._resource_base_path[1:]
            metadata_filepath = "{}/{}".format(self._resource_base_path,metadata_file)
        else:
            metadata_filepath = metadata_file

        super().__init__(storage,metadata_filepath)
        self._cache = cache
        self._logical_delete = logical_delete

    @property
    def metaname(self):
        return self._metaname

    @property
    def json(self):
        """
        Return the resource repository's meta data as dict object.
        Return None if resource repository's metadata is not found
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
        """
        Update the metadata
        if metadata is empty, delete the metadata file
        """
        if not metadata:
            #metadata is empty, delete the metedata
            self.delete()
            return

        logger.debug("Update the meta file '{}'".format(self._resource_path))
        super().update(metadata)
        if self._cache:
            #cache the result
            self._json = metadata

    def delete(self):
        """
        Delete the metaata file
        """
        logger.debug("Delete the meta file '{}'".format(self._resource_path))
        super().delete()
        if self._cache:
            self._json = None

class MetadataIndex(MetadataBase):
    """
    manage the metadata index file
    """
    def __init__(self,storage,resource_base_path=None,cache=False,index_metaname="_metadata_index",logical_delete=False):
        super().__init__(storage,resource_base_path=resource_base_path,cache=cache,metaname=index_metaname,logical_delete=logical_delete)

    def add_metafile(self,metaname,metadata_filepath):
        """
        Add a individual meta file to the metadata index file
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
        remove a metadata file from the metadata index file
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

class IndexedResourceRepositoryMetadataMixin(MetadataIndex):
    """
    A mixin class to manage indexed resource repository meta file 
    """
    meta_metadata_kwargs = [("resource_base_path","_resource_base_path"),("index_metaname","_metaname"),("archive","_archive"),('f_metaname','_f_metaname'),("logical_delete","_logical_delete")]
    metaclient_class = None
    def __init__(self,storage,f_metaname,resource_base_path=None,cache=False,archive=False,index_metaname="_metadata_index",logical_delete=False):
        super().__init__(storage,resource_base_path=resource_base_path,cache=cache,index_metaname=index_metaname,logical_delete=logical_delete)
        self._cache = cache
        self._archive = archive
        self._f_metaname = f_metaname
        self._metadata_client = None
        self._current_metaname = None

    @property
    def metadata_client(self):
        """
        Return the individual resource repository meta file against the current metaname
        Return None if current metaname is None
        """
        if self._current_metaname:
            if not self._metadata_client or self._metadata_client._metaname != self._current_metaname:
                self._metadata_client = self.metaclient_class(self._storage,resource_base_path=self._resource_base_path,cache=self._cache,metaname=self._current_metaname,archive=self._archive,logical_delete=self._logical_delete)
            return self._metadata_client
        else:
            return None


    def resource_metadatas(self,throw_exception=True,resource_status=ResourceConstant.NORMAL_RESOURCE,resource_file="current",**kwargs):
        """
        kwargs should be the keys in resource_keys
        resource_status: can be ResourceConstant.NORMAL_RESOURCE or ResourceConstant.DELETED_RESOURCE or BOTH
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
            metadata_index_json = self.json
            if metadata_index_json:
                for metaname,metapath in metadata_index_json:
                    meta_client = self.metaclient_class(self._storage,resource_base_path=self._resource_base_path,cache=False,metaname=metaname,archive=self._archive)
                    for metadata in meta_client.resource_metadatas(throw_exception=throw_exception,resource_status=resource_status,resource_file=resource_file):
                        yield metadata

        else:
            self._current_metaname = self._f_metaname(kwargs[self.resource_keys[0]])
            for metadata in self.metadata_client.resource_metadatas(throw_exception=throw_exception,resource_status=resource_status,resource_file=resource_file,**kwargs):
                yield metadata

    def get_resource_metadata(self,*args,resource_file="current",resource_status=ResourceConstant.NORMAL_RESOURCE):
        """
        resource_status: can be ResourceConstant.NORMAL_RESOURCE or ResourceConstant.DELETED_RESOURCE or BOTH
        Return resource's metadata or pushed resource's metadata if resource_file is not None; if not exist, throw exception
        """
        self._current_metaname = self._f_metaname(args[0])

        return self.metadata_client.get_resource_metadata(*args,resource_file=resource_file,resource_status=resource_status)

    def remove_resource(self,*args,permanent_delete=False):
        """
        Remove the resource's metadata. 
        permanent_delete : only useful if logical_delete is True; delete the resource permanently if permanent_delete is True
        Return the metadata of the remove resource if delete(logical or permanently) a resource or permanently delete a logical deleted resource  
            return None if not found or logical delete a already logical deleted resource
        """
        self._current_metaname = self._f_metaname(args[0])
        metadata = self.metadata_client.remove_resource(*args,permanent_delete=permanent_delete)
        if metadata and (not self._logical_delete or permanent_delete):
            #resource is deleted, delete the metadata file from indexed metadata if the metadata file is deleted
            if self.metadata_client.json is None:
                #metadata file was deleted,remove it from indexed file
                self.remove_metafile(self._current_metaname)
        return metadata

    def update_resource(self,resource_metadata):
        """
        Add or update the resource's metadata
        Return a tuple(the whole  metadata,created?)
        """
        self._current_metaname = self._f_metaname(resource_metadata[self.resource_keys[0]])
        result = self.metadata_client.update_resource(resource_metadata)
        if result[1]:
            #new created, add the metafile to indexed file if not exist before
            self.add_metafile(self._current_metaname,self.metadata_client._resource_path)
        
        return result


class ResourceRepositoryMetadataBase(MetadataBase):
    """
    manage resource repository's metadata 
    metadata is a json object.
    """
    #The resource keys in metadata used to identify a resource
    resource_keys =  []

    meta_metadata_kwargs = [("metaname","_metaname"),("resource_base_path","_resource_base_path"),("archive","_archive"),("logical_delete","_logical_delete")]
    def __init__(self,storage,resource_base_path=None,cache=False,metaname="metadata",archive=False,logical_delete=False):
        super().__init__(storage,resource_base_path=resource_base_path,cache=cache,metaname=metaname,logical_delete=logical_delete)
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

    def resource_metadatas(self,throw_exception=True,resource_status=ResourceConstant.NORMAL_RESOURCE,resource_file="current",**kwargs):
        """
        Return a generator to navigate the metadata of the filtered resources in resource repository; if not exist, return a empty generator
        resource_status: can be ResourceConstant.NORMAL_RESOURCE or ResourceConstant.DELETED_RESOURCE or BOTH
       
        resource_file :is None, navigate the resource's metadata; is not None, navigate the individual pushed resource's metadata
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

        if index == len(self.resource_keys):
            if resource_status == ResourceConstant.ALL_RESOURCE:
                yield self._get_pushed_resource_metadata(metadata,resource_file)
            elif resource_status == ResourceConstant.NORMAL_RESOURCE and not metadata.get(ResourceConstant.DELETED_KEY,False):
                yield self._get_pushed_resource_metadata(metadata,resource_file)
            elif resource_status == ResourceConstant.DELETED_RESOURCE and metadata.get(ResourceConstant.DELETED_KEY,False):
                yield self._get_pushed_resource_metadata(metadata,resource_file)
            else:
                raise exceptions.ResourceNotFound("Resource({}) Not Found".format(".".join(kwargs[k] for k in self.resource_keys[0:index])))
        else:
            for m1 in metadata.values():
                if (index + 1) == len(self.resource_keys):
                    if resource_status == ResourceConstant.ALL_RESOURCE:
                        yield self._get_pushed_resource_metadata(m1,resource_file)
                    elif resource_status == ResourceConstant.NORMAL_RESOURCE and not m1.get(ResourceConstant.DELETED_KEY,False):
                        yield self._get_pushed_resource_metadata(m1,resource_file)
                    elif resource_status == ResourceConstant.DELETED_RESOURCE and m1.get(ResourceConstant.DELETED_KEY,False):
                        yield self._get_pushed_resource_metadata(m1,resource_file)
                else:
                    for m2 in m1.values():
                        if (index + 2) == len(self.resource_keys):
                            if resource_status == ResourceConstant.ALL_RESOURCE:
                                yield self._get_pushed_resource_metadata(m2,resource_file)
                            elif resource_status == ResourceConstant.NORMAL_RESOURCE and not m2.get(ResourceConstant.DELETED_KEY,False):
                                yield self._get_pushed_resource_metadata(m2,resource_file)
                            elif resource_status == ResourceConstant.DELETED_RESOURCE and m2.get(ResourceConstant.DELETED_KEY,False):
                                yield self._get_pushed_resource_metadata(m2,resource_file)
                        else:
                            for m3 in m2.values():
                                if (index + 3) == len(self.resource_keys):
                                    if resource_status == ResourceConstant.ALL_RESOURCE:
                                        yield self._get_pushed_resource_metadata(m3,resource_file)
                                    elif resource_status == ResourceConstant.NORMAL_RESOURCE and not m3.get(ResourceConstant.DELETED_KEY,False):
                                        yield self._get_pushed_resource_metadata(m3,resource_file)
                                    elif resource_status == ResourceConstant.DELETED_RESOURCE and m3.get(ResourceConstant.DELETED_KEY,False):
                                        yield self._get_pushed_resource_metadata(m3,resource_file)
                                else:
                                    raise Exception("Not implemented")


    def get_resource_metadata(self,*args,resource_file="current",resource_status=ResourceConstant.NORMAL_RESOURCE):
        """
        resource_status: can be ResourceConstant.NORMAL_RESOURCE or ResourceConstant.DELETED_RESOURCE or BOTH
        Return resource's metadata or pushed resource's metadata if resource_file is not None; if not exist, throw exception
        """
        if len(self.resource_keys) != len(args):
            raise Exception("Invalid args({})".format(args))
        return next(self.resource_metadatas(resource_file=resource_file,resource_status=resource_status,**dict(zip(self.resource_keys,args))))

    def remove_resource(self,*args,permanent_delete=False):
        """
        Remove the resource's metadata. 
        Return the metadata of the remove resource if delete(logical or permanently) a resource or permanently delete a logical deleted resource  
            return None if not found or logical delete a already logical deleted resource
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
            if self._logical_delete:
                #logical delete is enabled
                if resource_metadata.get(ResourceConstant.DELETED_KEY,False):
                    #already logically deleted before
                    if permanent_delete:
                        #try to permanently delete this resource
                        del p_metadata[args[-1]]
                    else:
                        #try to logically delete this resource, but it is already logically deleted
                        return None
                else:
                    #not deleted before
                    if permanent_delete:
                        #try to permanently delete this resource
                        del p_metadata[args[-1]]
                    else:
                        #try to logically delete this resource
                        resource_metadata[ResourceConstant.DELETED_KEY] = True
            else:
                #logical delete is disabled, delete this resource permanently.
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
        Add or update a individual resource's metadata
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

        if ResourceConstant.DELETED_KEY in exist_metadata:
            #logically deleted before, restore it.
            del exist_metadata[ResourceConstant.DELETED_KEY]

        self.update(metadata)
        return (metadata,not existed)

class BasicResourceRepositoryMetadata(ResourceRepositoryMetadataBase):
    #The resource keys in metadata used to identify a resource
    resource_keys =  ["resource_id"]

class BasicGroupResourceRepositoryMetadata(ResourceRepositoryMetadataBase):
    #The resource keys in metadata used to identify a resource
    resource_keys =  ["resource_group","resource_id"]

class ResourceRepositoryMetadata(ResourceRepositoryMetaMetadataMixin,BasicResourceRepositoryMetadata):
    pass

class GroupResourceRepositoryMetadata(ResourceRepositoryMetaMetadataMixin,BasicGroupResourceRepositoryMetadata):
    pass

class IndexedResourceRepositoryMetadata(ResourceRepositoryMetaMetadataMixin,IndexedResourceRepositoryMetadataMixin):
    metaclient_class = BasicResourceRepositoryMetadata
    resource_keys = metaclient_class.resource_keys

class IndexedGroupResourceRepositoryMetadata(ResourceRepositoryMetaMetadataMixin,IndexedResourceRepositoryMetadataMixin):
    metaclient_class = BasicGroupResourceRepositoryMetadata
    resource_keys = metaclient_class.resource_keys

class ResourceRepositoryBase(object):
    """
    A base client to manage a Azure Resourcet
    """
    data_path = "data"
    def __init__(self,storage,resource_name,resource_base_path=None):
        self._resource_name = resource_name
        self._resource_base_path = resource_name if resource_base_path is None else resource_base_path
        if self._resource_base_path:
            if self._resource_base_path[0] == "/":
                self._resource_base_path = self._resource_base_path[1:]
            self._resource_data_path = "{}/{}".format(self._resource_base_path,self.data_path)
        else:
            self._resource_data_path = self.data_path
        self._storage = storage

    @property
    def resource_keys(self):
        return self._metadata_client.resource_keys

    @property
    def resourcename(self):
        return self._resource_name

    @property
    def metadata_client(self):
        return self._metadata_client

    @property
    def logical_delete(self):
        return self._metadata_client._logical_delete

    @property
    def archive(self):
        return self._metadata_client._archive

    @property
    def cache(self):
        return self._metadata_client._cache

    def _get_resource_file(self,resourceid):
        """
        Get a default resource file from resourceid
        for archived resource, each push will create another blob resource named by resource_file
        for non-archived resource,each push will create a new blob resource or update the exist resource, so resourceid is the same as resource_file
        """
        if self.archive:
            file_name,file_ext = os.path.splitext(resourceid)
            return "{0}_{1}{2}".format(file_name,timezone.now().strftime("%Y-%m-%d-%H-%M-%S"),file_ext)
        else:
            return resourceid

    def _get_resource_path(self,metadata):
        """
        Get the resoure path for resource_file
        resource path is the path in blob storage
        """
        if metadata["resource_file"][0] == "/":
            if len(self._metadata_client.resource_keys) > 1:
                return "{0}/{1}{2}".format(self._resource_data_path,"/".join(metadata[k] for k in self._metadata_client.resource_keys[:-1]),metadata["resource_file"])
            else:
                return "{0}{1}".format(self._resource_data_path,metadata["resource_file"])
        else:
            if len(self._metadata_client.resource_keys) > 1:
                return "{0}/{1}/{2}".format(self._resource_data_path,"/".join(metadata[k] for k in self._metadata_client.resource_keys[:-1]),metadata["resource_file"])
            else:
                return "{0}/{1}".format(self._resource_data_path,metadata["resource_file"])

    def get_download_path(self,metadata,folder):
        path = os.path.join(folder,os.path.relpath(metadata["resource_path"],self._resource_data_path))
        if not os.path.exists(os.path.dirname(path)):
            os.makedirs(os.path.dirname(path))
        return path


    def resource_metadatas(self,throw_exception=True,resource_status=ResourceConstant.NORMAL_RESOURCE,current_resource=True,**kwargs):
        for m in self._metadata_client.resource_metadatas(throw_exception=throw_exception,resource_status=resource_status,resource_file="current" if current_resource else None,**kwargs):
            yield m

    def get_resource_metadata(self,*args,resource_file="current",resource_status=ResourceConstant.NORMAL_RESOURCE):
        """
        Get the metadata of the resource
        resource_file: only meaningful for archived resource:
           current: return the meta data of the latest archive of the resource
           archive_path: return the meta data of the archive with the same archive path
           None: return the meta data of the resource including current and histories archive
        throw exception if not found
        Return the resource's metadata
        """
        return self._metadata_client.get_resource_metadata(*args,resource_file=resource_file,resource_status=resource_status)


    def delete_resource(self,*args,permanent_delete=False):
        """
        delete the resource; for archived resource, both current resource and all histories resources will be deleted.
        args: the resource keys
        permanent_delete : only useful if logical_delete is True; delete the resource permanently if permanent_delete is True
        return the metadata of the deleted resource; return None if not found
        """
        try:
            metadata = self.get_resource_metadata(*args,resource_file=None,resource_status=ResourceConstant.ALL_RESOURCE if permanent_delete else ResourceConstant.NORMAL_RESOURCE)
            metadata = self._delete_resource(metadata,permanent_delete=permanent_delete)
            return metadata
        except exceptions.ResourceNotFound as ex:
            return None

    def delete_resources(self,permanent_delete=False,**kwargs):
        """
        delete the resources filtered by resource keys;
        for archived resource, both current resource and all histories resources will be deleted.
        permanent_delete : only useful if logical_delete is True; delete the resource permanently if permanent_delete is True
        return the list of the metadata of deleted resources
        """
        unknown_args = [a for a in kwargs.keys() if a not in self._metadata_client.resource_keys and a not in ("throw_exception",)]
        if unknown_args:
            raise Exception("Unsupported keywords arguments({})".format(unknown_args))

        metadatas = [ m for m in self._metadata_client.resource_metadatas(resource_file=None,resource_status=ResourceConstant.ALL_RESOURCE if permanent_delete else ResourceConstant.NORMAL_RESOURCE,**kwargs)]
        index = 0
        while index < len(metadatas):
            metadatas[index] = self._delete_resource(metadatas[index],permanent_delete=permanent_delete)
            index += 1

        return metadatas

    def _delete_resource(self,metadata,permanent_delete=False):
        """
        The metadata of the specific resource you want to delete
        Delete the current archive and all histories archives for archive resource. 
        """
        if self.archive:
            resource_ids = [metadata["current"][k] for k in self._metadata_client.resource_keys]
        else:
            resource_ids = [metadata[k] for k in self._metadata_client.resource_keys]

        if self.logical_delete and not permanent_delete:
            logger.debug("Logically delete the resource({}.{})".format(self.resourcename,".".join(resource_ids)))
        else:
            logger.debug("Permanently delete the resource({}.{})".format(self.resourcename,".".join(resource_ids)))

            #delete the resource file from storage
            if self.archive:
                #archive resource
                #delete the current archive
                resource = self.get_resource(metadata["current"]["resource_path"])
                try:
                    resource.delete()
                except:
                    logger.error("Failed to delete the current resource({}) from blob storage.{}".format(metadata["current"]["resource_path"],traceback.format_exc()))
                #delete all history arvhives
                for m in metadata.get("histories") or []:
                    resource = self.get_resource(m["resource_path"])
                    try:
                        resource.delete()
                    except:
                        logger.error("Failed to delete the history resource({}) from blob storage.{}".format(m["resource_path"],traceback.format_exc()))
    
                
            else:
                resource = self.get_resource(metadata["resource_path"])
                try:
                    resource.delete()
                except:
                    logger.error("Failed to delete the resource({}) from blob storage.{}".format(metadata["resource_path"],traceback.format_exc()))
                
        #remove the resource from metadata
        return self._metadata_client.remove_resource(*resource_ids,permanent_delete=permanent_delete)
        
    def purge(self):
        """
        Delete all logically deleted resource from blob storage
        only useful for resource which support logical delete; 
        do nothing for resource without logical delete support
        """
        if not self.logical_delete:
            return

        metadatas = [ m for m in self._metadata_client.resource_metadatas(resource_file=None,resource_status=ResourceConstant.DELETED_RESOURCE)]
        for m in metadatas:
            self._delete_resource(m,permanent_delete=True)

        return metadatas

    def download_resources(self,folder=None,overwrite=False,resource_status=ResourceConstant.NORMAL_RESOURCE,**kwargs):
        """
        download multiple resources filtered by resource keys.
        for archived resource, only download the latest archive.
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
            folder = tempfile.mkdtemp(prefix=self._resource_name)

        metadatas = [m for m in self._metadata_client.resource_metadatas(throw_exception=True,resource_status=resource_status,resource_file="current",**kwargs)]
        for metadata in metadatas:
            if metadata.get("resource_file") and metadata.get("resource_path"):
                logger.debug("Download resource {}".format(metadata["resource_path"]))
                filename = self.get_download_path(metadata,folder)
                self.get_resource(metadata["resource_path"]).download(filename=filename,overwrite=overwrite)

        return (metadatas,folder)

    def download_resource(self,*args,filename=None,overwrite=False,resource_status=ResourceConstant.NORMAL_RESOURCE,resource_file="current"):
        """
        Download the resource with resourceid, and return the filename 
        remove the existing file or folder if overwrite is True
        resource_file: only meaningful for archived resource:
           current: download the latest archive of the resource
           archive_path: download the archive with the same archive path
           None: download the latest archive of the resource
        """
        resource_file = resource_file or "current"
        metadata = self.get_resource_metadata(*args,resource_file=resource_file,resource_status=resource_status)
        logger.debug("Download resource {}".format(metadata["resource_path"]))
        filename = self.get_resource(metadata["resource_path"]).download(filename=filename,overwrite=overwrite)
        return (metadata,filename)

    def get_resource(self,resource_path):
        return Resource(self._storage,resource_path)


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
        resource = self.get_resource(metadata["resource_path"])
        logger.debug("Push the resource({}.{}) to blob storage.".format(metadata["resource_id"],metadata["resource_path"]))
        resource.update(data)
        #update the resource metadata
        if f_post_push:
            f_post_push(metadata)

        repo_metadata,created = self._metadata_client.update_resource(metadata)

        return repo_metadata
        
    def is_exist(self,*args,resource_status=ResourceConstant.NORMAL_RESOURCE,resource_file="current"):
        """
        Check whether resource exists or not
        """
        try:
            return True if self.get_resource_metadata(*args,resource_file=resource_file,resource_status=resource_status) else False
        except exceptions.ResourceNotFound as ex:
            return False
        
    def get_json(self,*args,resource_status=ResourceConstant.NORMAL_RESOURCE,resource_file="current"):
        """
        for archived resource, return the latest archive
        Return (resource_metadata,resource as dict object)
        raise exception if failed or can't find the resource
        """
        metadata,text_content = self.get_text(*args,resource_status=resource_status,resource_file=resource_file)
        return (metadata,json.loads(text_content,cls=JSONDecoder))

    def get_text(self,*args,resource_status=ResourceConstant.NORMAL_RESOURCE,resource_file="current"):
        """
        for archived resource, return the latest archive
        Return (resource_metadata,resource as string)
        raise exception if failed or can't find the resource
        """
        metadata = self.get_resource_metadata(*args,resource_file=resource_file,resource_status=resource_status)
        resource = self.get_resource(metadata["resource_path"])
        return (metadata,resource.get_text())

    def get_content(self,*args,resource_status=ResourceConstant.NORMAL_RESOURCE,resource_file="current"):
        """
        for archived resource, return the latest archive
        Return (resource_metadata,resource as string)
        raise exception if failed or can't find the resource
        """
        metadata = self.get_resource_metadata(*args,resource_file=resource_file,resource_status=resource_status)
        resource = self.get_resource(metadata["resource_path"])
        return (metadata,resource.get_content())

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
        #populute the latest resource metadata
        for key in self._metadata_client.resource_keys:
            if key not in metadata:
                raise Exception("Missing resource key({}) in metadata".format(key))

        if "resource_file" not in metadata:
            metadata["resource_file"] = self._get_resource_file(metadata["resource_id"])
        metadata["resource_path"] = self._get_resource_path(metadata)     
        metadata["publish_date"] = timezone.now()

        #push the resource to azure storage
        resource = self.get_resource(metadata["resource_path"])
        resource.upload(filename)
        logger.debug("Push file  to the storage({}.{}).".format(filename,metadata["resource_id"],metadata["resource_path"]))
        #update the resource metadata
        if f_post_push:
            f_post_push(metadata)

        repo_metadata,created = self._metadata_client.update_resource(metadata)

        return repo_metadata

class ResourceRepository(ResourceRepositoryBase):
    def __init__(self,storage,resource_name,resource_base_path=None,archive=False,metaname="metadata",cache=True,logical_delete=False):
        super().__init__(storage,resource_name,resource_base_path=resource_base_path)
        self._metadata_client = ResourceRepositoryMetadata(storage,resource_base_path=self._resource_base_path,cache=cache,metaname=metaname,archive=archive,logical_delete=logical_delete)

class GroupResourceRepository(ResourceRepositoryBase):
    def __init__(self,storage,resource_name,resource_base_path=None,archive=False,metaname="metadata",cache=True,logical_delete=False):
        super().__init__(storage,resource_name,resource_base_path=resource_base_path)
        self._metadata_client = GroupResourceRepositoryMetadata(storage,resource_base_path=self._resource_base_path,cache=cache,metaname=metaname,archive=archive,logical_delete=logical_delete)



class IndexedResourceRepository(ResourceRepositoryBase):
    def __init__(self,storage,resource_name,f_metaname,resource_base_path=None,archive=False,index_metaname="_metadata_index",cache=True,logical_delete=False):
        super().__init__(storage,resource_name,resource_base_path=resource_base_path)
        self._metadata_client = IndexedResourceRepositoryMetadata(storage,f_metaname,resource_base_path=self._resource_base_path,cache=cache,archive=archive,index_metaname=index_metaname,logical_delete=logical_delete)

class IndexedGroupResourceRepository(ResourceRepositoryBase):
    def __init__(self,storage,resource_name,f_metaname,resource_base_path=None,archive=False,index_metaname="_metadata_index",cache=True,logical_delete=False):
        super().__init__(storage,resource_name,resource_base_path=resource_base_path)
        self._metadata_client = IndexedGroupResourceRepositoryMetadata(storage,f_metaname,resource_base_path=self._resource_base_path,cache=cache,archive=archive,index_metaname=index_metaname,logical_delete=logical_delete)


def get_resource_repository(storage,resource_name,resource_base_path=None,cache=True):
    resource_base_path = resource_name if resource_base_path is None else resource_base_path
    if resource_base_path:
        meta_metadata_filepath = "{}/meta_metadata.json".format(resource_base_path)
    else:
        meta_metadata_filepath = "meta_metadata.json"
    meta_metadata_client = JsonResource(storage,meta_metadata_filepath)
    meta_metadata_json = meta_metadata_client.json
    if not meta_metadata_json:
        raise exceptions.MetaMetadataMissing(resource_name,meta_metadata_filepath)

    resource_class = next(cls for cls,meta_cls in [
        (ResourceRepository,ResourceRepositoryMetadata),
        (GroupResourceRepository,GroupResourceRepositoryMetadata),
        (IndexedResourceRepository,IndexedResourceRepositoryMetadata),
        (IndexedGroupResourceRepository,IndexedGroupResourceRepositoryMetadata)] if meta_cls.__name__== meta_metadata_json["class"])
    return resource_class(storage,resource_name,cache=cache,**meta_metadata_json["kwargs"])


class ResourceConsumeClients(ResourceRepositoryBase):
    """
    A client to track the non group resource consuming status of a client
    Incompleted and not tested
    """

    data_path = "clients"
    def __init__(self,storage,resource_name,resource_base_path=None):
        super().__init__(storage,resource_name,resource_base_path=resource_base_path)
        self._metadata_client = BasicResourceRepositoryMetadata(storage,resource_base_path=self._resource_base_path,cache=False,metaname="clients_metadata",archive=False)

        self._resource_repository = get_resource_repository(storage,resource_name,resource_base_path=resource_base_path,cache=False)

    def get_client_metadatas(self,throw_exception=True,**kwargs):
        for m in self._metadata_client.resource_metadatas(throw_exception=throw_exception,**kwargs):
            yield m

    def get_client_metadata(self,clientid,resource_file="current"):
        return self.get_resource_metadata(clientid)

    def delete_clients(self,clientid=None):
        return self.delete_resources(resource_id = clientid)

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
            metadata,json_obj = self.get_json(clientid)
            return json_obj
        else:
            return None


class ResourceConsumeClient(ResourceConsumeClients):
    NOT_CHANGED = 0
    NEW = 1
    UPDATED = 2
    PHYSICALLY_DELETED = -1
    LOGICALLY_DELETED = -2
  

    def __init__(self,storage,resource_name,clientid,resource_base_path=None):
        super().__init__(storage,resource_name,resource_base_path=resource_base_path)
        self._clientid = clientid

    @property
    def consume_status(self):
        return self.get_client_consume_status(self._clientid)

    @property
    def clientid(self):
        return self._clientid

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

    def is_behind(self,resources=None):
        """
        resources: the list of resource id, or a filter which take the arugments (resource ids) for consuming.
        Return True if some resource is changed after last consuming;otherwise return False
        """
        client_consume_status = self.consume_status
        if client_consume_status is None:
            client_consume_status = {}

        resource_keys = self._resource_repository._metadata_client.resource_keys
        if resources and isinstance(resources,(tuple,list)):
            #Consume specified resources in order
            for resource_ids in resources:
                try:
                    if not isinstance(resource_ids,(list,tuple)):
                        resource_ids = [resource_ids]
                    res_consume_status = self.get_resource_consume_status(*resource_ids,consume_status=client_consume_status)
                    res_meta = self._resource_repository.get_resource_metadata(*resource_ids,resource_status=ResourceConstant.ALL_RESOURCE,resource_file=None)
                    logically_deleted = res_meta.get(ResourceConstant.DELETED_KEY,False) if self._resource_repository.logical_delete else False
                    if self._resource_repository.archive:
                        res_meta = res_meta["current"]
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
                    if not logically_deleted:
                        logger.debug("Found a new resource({},{})".format(resource_ids,res_meta["resource_path"]))
                        return True
                elif logically_deleted:
                    logger.debug("Found a deleted resource({},{})".format(resource_ids,res_meta["resource_path"]))
                    return True
                elif res_consume_status.get("consume_failed_msg"):
                    if res_consume_status["resource_status"] == "New":
                        logger.debug("Found a new resource({},{}) which is consumed failed last time".format(resource_ids,res_meta["resource_path"]))
                        return True
                    elif res_meta != res_consume_status["resource_metadata"]:
                        logger.debug("Found a recently changed resource({},{}) which is consumed failed last time".format(resource_ids,res_meta["resource_path"]))
                        return True
                    elif res_consume_status["resouce_status"] == "Update":
                        logger.debug("Found a updated resource({},{}) which is consumed failed last time".format(resource_ids,res_meta["resource_path"]))
                        return True
                    else:
                        logger.debug("Found a  resource({},{}) which is reconsumed failed last time".format(resource_ids,res_meta["resource_path"]))
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
            for res_meta in self._resource_repository.resource_metadatas(throw_exception=False,resource_status=ResourceConstant.ALL_RESOURCE,current_resource=False):
                logically_deleted = res_meta.get(ResourceConstant.DELETED_KEY,False) if self._resource_repository.logical_delete else False
                if self._resource_repository.archive:
                    res_meta = res_meta["current"]

                resource_ids = tuple(res_meta[key] for key in resource_keys)
                if resources and not resources(*resource_ids):
                    continue
                checked_resources.add(resource_ids)
                res_consume_status = self.get_resource_consume_status(*resource_ids,consume_status=client_consume_status)
                if not res_consume_status:
                    if not logically_deleted:
                        logger.debug("Found a new resource({},{})".format(resource_ids,res_meta["resource_path"]))
                        return True
                elif logically_deleted:
                    logger.debug("Found a deleted resource({},{})".format(resource_ids,res_meta["resource_path"]))
                    return True
                elif res_consume_status.get("consume_failed_msg"):
                    if res_consume_status["resource_status"] == "New":
                        logger.debug("Found a new resource({},{}) which is consumed failed last time".format(resource_ids,res_meta["resource_path"]))
                        return True
                    elif res_meta != res_consume_status["resource_metadata"]:
                        logger.debug("Found a recently changed resource({},{}) which is consumed failed last time".format(resource_ids,res_meta["resource_path"]))
                        return True
                    elif res_consume_status["resource_status"] == "Update":
                        logger.debug("Found a updated resource({},{}) which is consumed failed last time".format(resource_ids,res_meta["resource_path"]))
                        return True
                    else:
                        logger.debug("Found a  resource({},{}) which is reconsumed failed last time".format(resource_ids,res_meta["resource_path"]))
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
                level = 1
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
        return False


    def get_consume_status_name(self,resource_status):
        if resource_status == self.PHYSICALLY_DELETED:
            return "physically deleted"
        elif resource_status == self.LOGICALLY_DELETED:
            return "logically deleted"
        elif resource_status == self.NEW:
            return "new"
        elif resource_status == self.UPDATED:
            return "updated"
        elif resource_status == self.NOT_CHANGED:
            return "non-changed"
        else:
            raise Exception("Unknown resource status({})".format(resource_status))

    def consume(self,callback,resources=None,reconsume=False,sortkey_func=None,stop_if_failed=True):
        """
        resources: the list of resource id, or a filter which take the arugments (resource ids) for consuming.
        stop_if_failed: only useful for callback per resource
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
        resource_keys = self._resource_repository._metadata_client.resource_keys
        consume_result = ([],[])
        updated_resources = []

        def _update_client_consume_status(resource_status,resource_ids,res_consume_status,res_meta,commit=True,failed_msg=None):
            if resource_status in (self.PHYSICALLY_DELETED,self.LOGICALLY_DELETED):
                if failed_msg:
                    res_consume_status["resource_metadata"] = res_meta
                    res_consume_status["resource_status"] = "Physically Deleted" if resource_status == self.PHYSICALLY_DELETED else "Logically Deleted"
                    res_consume_status["consume_date"] = timezone.now()
                    res_consume_status["consume_failed_msg"] = failed_msg
                else:
                    self.remove_resource_consume_status(*resource_ids,consume_status=client_consume_status)
            elif resource_status == self.NEW:
                res_consume_status = {
                    "resource_metadata":res_meta,
                    "resource_status":"New",
                    "consume_date":timezone.now()
                }
                if failed_msg:
                    res_consume_status["consume_failed_msg"] = failed_msg
                self.set_resource_consume_status(*resource_ids,res_consume_status = res_consume_status,consume_status=client_consume_status)
            elif resource_status == self.UPDATED:
                res_consume_status["resource_metadata"] = res_meta
                res_consume_status["resource_status"] = "Update"
                res_consume_status["consume_date"] = timezone.now()
                if failed_msg:
                    res_consume_status["consume_failed_msg"] = failed_msg
                elif "consume_failed_msg" in res_consume_status:
                    del res_consume_status["consume_failed_msg"]
            elif resource_status == self.NOT_CHANGED:
                res_consume_status["resource_metadata"] = res_meta
                res_consume_status["resource_status"] = "Reconsume"
                res_consume_status["consume_date"] = timezone.now()
                if failed_msg:
                    res_consume_status["consume_failed_msg"] = failed_msg
                elif "consume_failed_msg" in res_consume_status:
                    del res_consume_status["consume_failed_msg"]

            metadata["last_consumed_resource"] = resource_ids
            metadata["last_consumed_resource_status"] = res_consume_status["resource_status"]
            metadata["last_consume_date"] = timezone.now()
            if failed_msg:
                metadata["last_consume_failed_msg"] = failed_msg
            elif "last_consume_failed_msg" in metadata:
                del metadata["last_consume_failed_msg"]
            if commit:
                self.push_resource(json.dumps(client_consume_status,cls=JSONEncoder,sort_keys=True,indent=4).encode(),metadata=metadata)

        def _consume_resource(resource_status,resource_ids,res_consume_status,res_meta):
            if resource_status == self.PHYSICALLY_DELETED:
                logger.info("Consume the physically deleted resource({},{})".format(resource_ids,(res_meta or res_consume_status["resource_metadata"])["resource_path"]))
            elif resource_status == self.LOGICALLY_DELETED:
                logger.info("Consume the logically deleted resource({},{})".format(resource_ids,(res_meta or res_consume_status["resource_metadata"])["resource_path"]))
            elif resource_status == self.NEW:
                logger.info("Consume the new resource({},{})".format(resource_ids,res_meta["resource_path"]))
            elif resource_status == self.UPDATED:
                logger.info("Consume the updated resource({},{})".format(resource_ids,res_meta["resource_path"]))
            elif resource_status == self.NOT_CHANGED:
                logger.info("Reconsume the resource({},{})".format(resource_ids,res_meta["resource_path"]))
            else:
                raise Exception("Unknown resource status({})".format(resource_status))

            resource_status_name = self.get_consume_status_name(resource_status)
            res_file = None
            try:
                if res_meta:
                    res_file = self._resource_repository.download_resource(*resource_ids,resource_status=ResourceConstant.ALL_RESOURCE)[1]
            
                callback(resource_status,res_meta or res_consume_status["resource_metadata"],res_file)
                _update_client_consume_status(resource_status,resource_ids,res_consume_status,res_meta)
                consume_result[0].append((resource_status,resource_status_name,resource_ids))
            except Exception as ex:
                _update_client_consume_status(resource_status,resource_ids,res_consume_status,res_meta,failed_msg=str(ex))
                if stop_if_failed:
                    raise
                msg = "Failed to consume the {} resource({}).{}".format(resource_status_name,resource_ids,traceback.format_exc())
                logger.error(msg)
                consume_result[1].append((resource_status,resource_status_name,resource_ids,str(ex)))
            finally:
                remove_file(res_file)


        if resources and not callable(resources):
            #Consume specified resources in order
            for resource_ids in resources:
                try:
                    if not isinstance(resource_ids,(list,tuple)):
                        resource_ids = [resource_ids]
                    res_consume_status = self.get_resource_consume_status(*resource_ids,consume_status=client_consume_status)
                    res_meta = self._resource_repository.get_resource_metadata(*resource_ids,resource_status=ResourceConstant.ALL_RESOURCE,resource_file=None)
                    logically_deleted = res_meta.get(ResourceConstant.DELETED_KEY,False) if self._resource_repository.logical_delete else False
                    if self._resource_repository.archive:
                        res_meta = res_meta["current"]
                except exceptions.ResourceNotFound as ex:
                    if res_consume_status:
                        #this resource was consuemd before and now it was deleted
                        if callback_per_resource and not sortkey_func:
                            _consume_resource(self.PHYSICALLY_DELETED,resource_ids,res_consume_status,None)
                        else:
                            updated_resources.append((self.PHYSICALLY_DELETED,resource_ids,res_consume_status,None))
                    else:
                        #this resource was not conusmed and also it doesn't exist
                        logger.warning("The resource({}) doesn't exist".format(resource_ids))
                    continue

                if not res_consume_status:
                    #new resource
                    if not logically_deleted:
                        resource_status = self.NEW
                    else:
                        continue
                elif logically_deleted:
                    resource_status = self.LOGICALLY_DELETED
                elif res_consume_status.get("consume_failed_msg"):
                    if res_consume_status["resource_status"] == "New":
                        resource_status = self.NEW
                    elif res_meta != res_consume_status["resource_metadata"]:
                        resource_status = self.UPDATED
                    elif res_consume_status["resource_status"] == "Update":
                        resource_status = self.UPDATED
                    else:
                        resource_status = self.NOT_CHANGED
                elif res_meta != res_consume_status["resource_metadata"]:
                    #resource was changed
                    resource_status = self.UPDATED
                elif reconsume:
                    #resource was not changed
                    resource_status = self.NOT_CHANGED
                else:
                    #reosurce was consumed before
                    logger.debug("The resource({},{}) is not changed after last consuming".format(resource_ids,res_meta["resource_path"]))
                    continue
    
                if callback_per_resource and not sortkey_func:
                    _consume_resource(resource_status,resource_ids,res_consume_status,res_meta)
                else:
                    updated_resources.append((resource_status,resource_ids,res_consume_status,res_meta))

        else:
            #find new and updated resources
            checked_resources = set()
            for res_meta in self._resource_repository.resource_metadatas(throw_exception=False,resource_status=ResourceConstant.ALL_RESOURCE,current_resource=False):
                logically_deleted = res_meta.get(ResourceConstant.DELETED_KEY,False) if self._resource_repository.logical_delete else False
                if self._resource_repository.archive:
                    res_meta = res_meta["current"]

                resource_ids = tuple(res_meta[key] for key in resource_keys)
                if resources and not resources(*resource_ids):
                    continue
                checked_resources.add(resource_ids)
                res_consume_status = self.get_resource_consume_status(*resource_ids,consume_status=client_consume_status)

                if not res_consume_status:
                    if not logically_deleted:
                        #new resource
                        resource_status = self.NEW
                    else:
                        continue
                elif logically_deleted:
                    resource_status = self.LOGICALLY_DELETED
                elif res_consume_status.get("consume_failed_msg"):
                    if res_consume_status["resource_status"] == "New":
                        resource_status = self.NEW
                    elif res_meta != res_consume_status["resource_metadata"]:
                        resource_status = self.UPDATED
                    elif res_consume_status["resource_status"] == "Update":
                        resource_status = self.UPDATED
                    else:
                        resource_status = self.NOT_CHANGED
                elif res_meta != res_consume_status["resource_metadata"]:
                    #resource was changed
                    resource_status = self.UPDATED
                elif reconsume:
                    #resource was not changed
                    resource_status = self.NOT_CHANGED
                else:
                    #reosurce was consumed before
                    logger.debug("The resource({},{}) is not changed after last consuming".format(resource_ids,res_meta["resource_path"]))
                    continue
                
                if callback_per_resource and not sortkey_func:
                    _consume_resource(resource_status,resource_ids,res_consume_status,res_meta)
                else:
                    updated_resources.append((resource_status,resource_ids,res_consume_status,res_meta))

            #find deleted resources
            level = 1
            deleted_resources = []
            for val in client_consume_status.values():
                level = 1
                if level == len(resource_keys):
                    resource_ids = tuple(val["resource_metadata"][key] for key in resource_keys)
                    if resources and not resources(*resource_ids):
                        continue
                    elif resource_ids in checked_resources:
                        continue
                    else:
                        deleted_resources.append(resource_ids)
                else:
                    level += 1
                    for val2 in val.values():
                        if level == len(resource_keys):
                            resource_ids = tuple(val2["resource_metadata"][key] for key in resource_keys)
                            if resources and not resources(*resource_ids):
                                continue
                            elif resource_ids in checked_resources:
                                continue
                            else:
                                deleted_resources.append(resource_ids)
                        else:
                            level += 1
                            for val3 in val2.values():
                                if level == len(resource_keys):
                                    resource_ids = tuple(val3["resource_metadata"][key] for key in resource_keys)
                                    if resources and not resources(*resource_ids):
                                        continue
                                    elif resource_ids in checked_resources:
                                        continue
                                    else:
                                        deleted_resources.append(resource_ids)
                                else:
                                    raise Exception("Not implemented")
            if deleted_resources:
                for resource_ids in deleted_resources:
                    res_consume_status = self.get_resource_consume_status(*resource_ids,consume_status=client_consume_status)

                    if callback_per_resource and not sortkey_func:
                        _consume_resource(self.PHYSICALLY_DELETED,resource_ids,res_consume_status,None)
                    else:
                        updated_resources.append((self.PHYSICALLY_DELETED,resource_ids,res_consume_status,None))

        if updated_resources:
            if sortkey_func:
                updated_resources.sort(key=sortkey_func)
            if callback_per_resource :
                for updated_resource in updated_resources:
                    _consume_resource(*updated_resource)
            else:
                callback_arguments = []
                try:
                    #download files and populate callback arugments
                    for updated_resource in updated_resources:
                        consume_result[0].append((updated_resource[0],self.get_consume_status_name(updated_resource[0]),updated_resource[1]))
                        if updated_resource[3]:
                            res_file = self._resource_repository.download_resource(*updated_resource[1],resource_status=ResourceConstant.ALL_RESOURCE)[1]
                        else:
                            res_file = None
                        callback_arguments.append((updated_resource[0],updated_resource[3] or updated_resource[2]["resource_metadata"],res_file))

                    callback(callback_arguments)
                    #update client consume status
                    for updated_resource in updated_resources:
                        _update_client_consume_status(*updated_resource,commit=False)
                    #push client consume status to blob storage
                    self.push_resource(json.dumps(client_consume_status,cls=JSONEncoder,sort_keys=True,indent=4).encode(),metadata=metadata)
                finally:
                    #remote temporary files
                    for res_status,res_meta,res_file in callback_arguments:
                        remove_file(res_file)
                
        return consume_result


