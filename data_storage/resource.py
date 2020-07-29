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

def compare_resource_id(resource_id1,resource_id2):
    """
    Compare two resource id
    resource_id1, resource_id2 : list for multiple keys; string or list for single keys

    0: equal
    1: greater
    2: less
    """
    if resource_id1 is None:
        if resource_id2 is None:
            return 0
        else:
            return -1
    elif resource_id2 is None:
        return 1

    if isinstance(resource_id1,(list,tuple)):
        index = 0
        while index < len(resource_id1):
            if resource_id1[index] is None:
                if resource_id2[index] is None:
                    index += 1
                else:
                    return -1
            elif resource_id2[index] is None:
                return 1
            elif resource_id1[index] < resource_id2[index]:
                return -1
            elif resource_id1[index] == resource_id2[index]:
                index += 1
            else:
                return 1

        return 0
    else:
        if resource_id1 < resource_id2:
            return -1
        elif resource_id1 == resource_id2:
            return 0
        else:
            return 1
        return 0


EQUAL = 1
GREATER = 2
GREATER_AND_EQUAL = 3
LESS = 4
LESS_AND_EQUAL = 5

def find_resource_index(resource_datas,resource_id,policy=EQUAL):
    """
    policy
        EQUAL: find the index of the resource data whose resource_id is equal with resource_id; return -1 if not found
        GREATER_AND_EQUAL: find the index of the smallest resource data whose resource_id is greater than or equal with resource_id; return -1 if not found
        LESS_AND_EQUAL: find the index of the greatest resource data whose resource_id is less than or equal with resource_id; return -1 if not found
    """
    def _find_index(resource_datas,min_index,max_index,resource_id,policy):
        if min_index == max_index:
            result = compare_resource_id(resource_id,resource_datas[min_index][0])
            if result == 0:
                if policy in (EQUAL,GREATER_AND_EQUAL,LESS_AND_EQUAL):
                    return min_index
                elif policy == GREATER:
                    return min_index + 1
                else:
                    return min_index - 1
            elif result == 1:
                if policy == EQUAL:
                    return -1
                elif policy in (GREATER,GREATER_AND_EQUAL):
                    return min_index + 1
                else:
                    return min_index
            else:
                if policy == EQUAL:
                    return -1
                elif policy in (GREATER,GREATER_AND_EQUAL):
                    return min_index
                else:
                    return min_index - 1
        elif min_index + 1 == max_index:
            result = compare_resource_id(resource_id,resource_datas[min_index][0])
            if result == 0:
                if policy in (EQUAL,GREATER_AND_EQUAL,LESS_AND_EQUAL):
                    return min_index
                elif policy == GREATER:
                    return min_index + 1
                else:
                    return min_index - 1
            elif result == -1:
                if policy == EQUAL:
                    return -1
                elif policy in (GREATER,GREATER_AND_EQUAL):
                    return min_index
                else:
                    return min_index - 1
            else:
                result = compare_resource_id(resource_id,resource_datas[max_index][0])
                if result == 0:
                    if policy in (EQUAL,GREATER_AND_EQUAL,LESS_AND_EQUAL):
                        return max_index
                    elif policy == GREATER:
                        return max_index + 1
                    else:
                        return max_index - 1
                elif result == 1:
                    if policy == EQUAL:
                        return -1
                    elif policy in (GREATER,GREATER_AND_EQUAL):
                        return max_index + 1
                    else:
                        return max_index
                else:
                    if policy == EQUAL:
                        return -1
                    elif policy in (GREATER,GREATER_AND_EQUAL):
                        return max_index
                    else:
                        return max_index - 1
        else:
            mid_index = int((min_index+max_index) / 2)
            result = compare_resource_id(resource_id,resource_datas[mid_index][0])
            if result == 0:
                if policy in (EQUAL,GREATER_AND_EQUAL,LESS_AND_EQUAL):
                    return mid_index
                elif policy == GREATER:
                    return mid_index + 1
                else:
                    return mid_index - 1
            elif result == 1:
                return _find_index(resource_datas,mid_index + 1,max_index,resource_id,policy=policy)
            else:
                return _find_index(resource_datas,min_index,mid_index - 1,resource_id,policy=policy)


    min_index = 0
    max_index = len(resource_datas) - 1
    if max_index < 0:
        #empty resource datas
        return -1
    else:
        result = compare_resource_id(resource_id,resource_datas[min_index][0])
        if result == -1:
            #resource_id is less than the smallest data
            if policy in (GREATER,GREATER_AND_EQUAL):
                return min_index
            else:
                return -1
        elif result == 0:
            #resource_id is equal with the smallest data
            if policy in (EQUAL,GREATER_AND_EQUAL,LESS_AND_EQUAL):
                return min_index
            elif policy == GREATER:
                if max_index == min_index:
                    return -1
                else:
                    return min_index + 1
            else:
                return -1
        elif max_index == min_index:
            #resource_id is greater than the smallest data , and resource datas only has one data
            if policy in (EQUAL,GREATER_AND_EQUAL,GREATER):
                return -1
            else:
                return min_index
        else:
            #resource_id is greater than the smallest data , and resource datas only has more than one data
            result = compare_resource_id(resource_id,resource_datas[max_index][0])
            if result == 1:
                #resource_id is greater than the biggest data , and resource datas only has one data
                if policy in (EQUAL,GREATER_AND_EQUAL,GREATER):
                    return -1
                else:
                    return max_index
            elif result == 0:
                #resource_id is equal with the biggest data , and resource datas only has one data
                if policy in (EQUAL,GREATER_AND_EQUAL,LESS_AND_EQUAL):
                    return max_index
                elif policy == GREATER:
                    return -1
                else:
                    return max_index - 1
            elif max_index == min_index + 1:
                #resource_id is greater than the smallest data and less than the biggest data , and resource datas only has two datas
                if policy == EQUAL:
                    return -1
                elif policy in (GREATER,GREATER_AND_EQUAL):
                    return max_index
                else:
                    return max_index - 1
                return -1
            else:
                min_index = min_index + 1
                max_index = max_index - 1

    index = _find_index(resource_datas,min_index,max_index,resource_id,policy=policy)
    if index > max_index or index < min_index:
        return -1
    else:
        return index

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

    @property
    def json(self):
        obj = super().json
        return [] if obj is None else obj

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
    metaclient_class = None
    def __init__(self,storage,f_metaname,resource_base_path=None,cache=False,archive=False,index_metaname="_metadata_index",logical_delete=False):
        super().__init__(storage,resource_base_path=resource_base_path,cache=cache,index_metaname=index_metaname,logical_delete=logical_delete)
        self._cache = cache
        self._archive = archive
        self._f_metaname = f_metaname
        self._metadata_client = None
        self._current_metaname = None

    def create_metadata_client(self,metaname):
        """
        Create metadata client
        """
        return self.metaclient_class(self._storage,resource_base_path=self._resource_base_path,cache=self._cache,metaname=metaname,archive=self._archive,logical_delete=self._logical_delete)

    @property
    def metadata_client(self):
        """
        Return the individual resource repository meta file against the current metaname
        Return None if current metaname is None
        """
        if self._current_metaname:
            if not self._metadata_client or self._metadata_client._metaname != self._current_metaname:
                self._metadata_client = self.create_metadata_client(self._current_metaname)
            return self._metadata_client
        else:
            return None

    @property
    def current_metadata_index(self):
        """
        Return the index of the meatadata file in metadata index;return None if not found
        """
        indexed_meta = self.json
        index = 0
        while index < len(indexed_meta):
            if indexed_meta[index][0] == self._current_metaname:
                return index
            index += 1

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
                    meta_client = self.create_metadata_client(metaname)
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
            if not self.metadata_client.json:
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


class IndexedHistoryDataRepositoryMetadataMixin(IndexedResourceRepositoryMetadataMixin):

    def create_metadata_client(self,metaname):
        """
        Create metadata client
        """
        return self.metaclient_class(self._storage,resource_base_path=self._resource_base_path,cache=self._cache,metaname=metaname)

    @property
    def last_resource(self):
        """
        Return a tuple(last resource's id, last resource's metadata) ; return None if no last resource
        """
        indexed_meta = self.json
        index  = len(indexed_meta) - 1
        last_res = None
        while index >= 0:
            metaname,metapath = indexed_meta[index]
            if metaname == self._current_metaname:
                last_res = self.metadata_client.last_resource
            else:
                metadata_client = self.create_metadata_client(metaname)
                last_res = metadata_client.last_resource
            if last_res:
                return last_res
            else:
                index -= 1
            
        return None

    @property
    def last_resource_id(self):
        last_resource = self.last_resource
        return last_resource[0] if last_resource else None
    
    def find_resource_index(self,resource_id,policy=EQUAL):
        if len(self.resource_keys) == 1:
            self._current_metaname = self._f_metaname(resource_id)
        else:
            self._current_metaname = self._f_metaname(resource_id[0])

        index = self.metadata_client.find_resource_index(resource_id,policy=policy)
        if index == -1 and policy != EQUAL:
            #can't find a resource which is greater than or equal with the resource_id. 
            #if current metadata file is not the last current metadata file, set the next metadata file as the current metadata file, and return 0
            indexed_meta = self.json
            current_index = self.current_metadata_index
            if policy in (GREATER_AND_EQUAL,GREATER):
                if current_index < len(indexed_meta) - 1:
                    self._current_metaname = indexed_meta[current_index + 1][0]
                    index = 0
            elif current_index > 0:
                self._current_metaname = indexed_meta[current_index - 1][0]
                index = len(self.metadata_client.json) - 1


        return index


    def resources_in_range(self,min_resource_id,max_resource_id,min_resource_included=True,max_resource_included=False):
        """
        Return a generator to navigate the (resource_id,metadata) of the resource from min_resource_id to max_resource_id
        """
        indexed_meta = self.json 
        if min_resource_id:
            if len(self.resource_keys) == 1:
                min_metaname = self._f_metaname(min_resource_id)
            else:
                min_metaname = self._f_metaname(min_resource_id[0])
        else:
            min_metaname = None

        if max_resource_id:
            if len(self.resource_keys) == 1:
                max_metaname = self._f_metaname(max_resource_id)
            else:
                max_metaname = self._f_metaname(max_resource_id[0])
        else:
            max_metaname = None

        for metaname,metapath in indexed_meta:
            if min_metaname is None:
                min_id = None
            elif metaname == min_metaname:
                min_id = min_resource_id
            elif metaname < min_metaname:
                continue
            else:
                min_id = None
            
            if max_metaname is None:
                max_id = None
            elif metaname == max_metaname:
                max_id = max_resource_id
            elif metaname < max_metaname:
                max_id = None
            else:
                break

            self._current_metaname = metaname

            for resource_id,metadata in self.metadata_client.resources_in_range(min_id,max_id,min_resource_included=min_resource_included,max_resource_included=max_resource_included):
                yield (resource_id,metadata)

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

    @property
    def json(self):
        obj = super().json
        return {} if obj is None else obj

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
        kwargs: can be a resource id by providing the value for each resource key or act as a filter.
       
        resource_file :is None, navigate the resource's metadata; is not None, navigate the individual pushed resource's metadata
        throw_exception: if True, throw exception if resource filter or resource id not found; otherwise return empty generator
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
            elif throw_exception:
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

class HistoryDataRepositoryMetadataBase(MetadataBase):
    """
    manage history data repository's metadata 
    metadata is a json object.
    """
    #The resource keys in metadata used to identify a resource
    resource_keys =  []

    meta_metadata_kwargs = [("metaname","_metaname"),("resource_base_path","_resource_base_path")]
    def __init__(self,storage,resource_base_path=None,cache=False,metaname="metadata"):
        super().__init__(storage,resource_base_path=resource_base_path,cache=cache,metaname=metaname,logical_delete=False)

    @property
    def json(self):
        obj = super().json
        return [] if obj is None else obj

    @property
    def last_resource(self):
        """
        Return a tuple(last resource's id, last resource's metadata) ; return None if no last resource
        """
        metadata = self.json
        if metadata:
            return metadata[-1]
        else:
            return None

    @property
    def last_resource_id(self):
        last_resource = self.last_resource
        return last_resource[0] if last_resource else None
        

    def find_resource_index(self,resource_id,policy=EQUAL):
        return find_resource_index(self.json,resource_id,policy=policy)

    def resource_metadatas(self,throw_exception=True,resource_status=None,resource_file=None,**kwargs):
        """
        throw_exception: useless
        resource_status:useless
        resource_file:useless
        Return a generator to navigate the metadata of the filtered resources in resource repository; if not exist, return a empty generator
        kwargs: can be a resource id by providing the value for each resource key or act as a filter.
       
        """
        unknown_args = [a for a in kwargs.keys() if a not in self.resource_keys]
        if unknown_args:
            raise Exception("Unsupported keywords arguments({})".format(unknown_args))

        metadata = self.json or []

        filter_id = []
        for key in self.resource_keys:
            if key in kwargs:
                filter_id.append(kwargs[key])
            else:
                break

        for resource_id,res_metadata in metadata:
            if filter_id:
                if len(self.resource_keys) == 1: 
                    if resource_id != filter_id[0]:
                        continue
                elif len(filter_id) == 1:
                    if resource_id[0] != filter_id[0]:
                        continue
                else:
                    matched = True
                    index = 0
                    while index < len(filter_id):
                        if resource_id[index] != filter_id[index]:
                            matched = False
                            break
                        index += 1
                    if not matched:
                        continue
            yield res_metadata

    def resources_in_range(self,min_resource_id,max_resource_id,min_resource_included=True,max_resource_included=False):
        """
        Return a generator to navigate the (resource_id,metadata) of the resource from min_resource_id to max_resource_id
        """
        metadata = self.json
        if min_resource_id:
            min_index = self.find_resource_index(min_resource_id,policy=GREATER_AND_EQUAL if min_resource_included else GREATER)
            if min_index == -1:
                return
            elif min_index == 0:
                min_index = None
        else:
            min_index = None

        if max_resource_id:
            max_index = self.find_resource_index(max_resource_id,policy=LESS_AND_EQUAL if max_resource_included else LESS)
            if max_index == -1:
                return
            elif max_index >= len(metadata) - 1:
                mex_index = None
            else:
                max_index += 1
        else:
            max_index = None

        for resource_id, res_metadata in metadata if (min_index is None and max_index is None) else metadata[min_index or 0:max_index or len(metadata)]:
            yield (resource_id,res_metadata)

    def get_resource_metadata(self,*args,resource_file="current",resource_status=ResourceConstant.NORMAL_RESOURCE):
        """
        resoure_file: useless
        resource_status:useless
        Return resource's metadata or pushed resource's metadata if resource_file is not None; if not exist, throw exception
        """
        if len(self.resource_keys) != len(args):
            raise Exception("Invalid args({})".format(args))

        metadata = self.json
        if len(self.resource_keys) == 1:
            index = self.find_resource_index(args[0])
        else:
            index = self.find_resource_index(args)

        if index == -1:
            raise exceptions.ResourceNotFound("Resource({}) Not Found".format(dict(zip(self.resource_keys,args))))
        else:
            return metadata[index][1]

    def remove_resource(self,*args,permanent_delete=False):
        """
        Remove the resource's metadata. 
        permanent_delete:useless
        Return the metadata of the remove resource if delete(logical or permanently) a resource or permanently delete a logical deleted resource  
            return None if not found or logical delete a already logical deleted resource
        """
        if len(self.resource_keys) != len(args):
            raise Exception("Invalid args({})".format(args))

        metadata = self.json 

        if len(self.resource_keys) == 1:
            index = self.find_resource_index(args[0])
        else:
            index = self.find_resource_index(args)

        if index == -1:
            return None
        else:
            resource_id,res_metadata = metadata[index]
            del metadata[index]
         
            #delete the meta file if meta file is empty
            if metadata:
                self.update(metadata)
            else:
                self.delete()

            return res_metadata

    def update_resource(self,resource_metadata):
        """
        Add or update a individual resource's metadata
        Return a tuple(the whole  metadata,created?)
        throw 
            ResourceAlreadyExist if resurce already exists
            InvalidResource if resource id is not greater than the last resource id
        """
        metadata = self.json 

        if len(self.resource_keys) == 1:
            resource_id = resource_metadata[self.resource_keys[0]]
        else:
            resource_id = [resource_metadata[key] for key in self.resource_keys]

        last_resource_id = self.last_resource_id
        result = compare_resource_id(resource_id,last_resource_id)
        if result == 0:
            raise exceptions.ResourceAlreadyExist("Can't update existing history data({})".format(resource_id))
        elif result == 1:
            metadata.append([resource_id,resource_metadata])
        else:
            index = self.find_resource_index(resource_id)
            if index == -1:
                raise exceptions.InvalidResource("The resource id({}) of the new history data must be greater than the resource id({}) of the last history data".format(resource_id,last_resource_id))
            else:
                raise exceptions.ResourceAlreadyExist("Can't update existing history data({})".format(resource_id))

        self.update(metadata)
        return (metadata,True)

class BasicResourceRepositoryMetadata(ResourceRepositoryMetadataBase):
    #The resource keys in metadata used to identify a resource
    resource_keys =  ["resource_id"]

class BasicGroupResourceRepositoryMetadata(ResourceRepositoryMetadataBase):
    #The resource keys in metadata used to identify a resource
    resource_keys =  ["resource_group","resource_id"]

class BasicHistoryDataRepositoryMetadata(HistoryDataRepositoryMetadataBase):
    #The resource keys in metadata used to identify a resource
    resource_keys =  ["resource_id"]

class BasicGroupHistoryDataRepositoryMetadata(HistoryDataRepositoryMetadataBase):
    #The resource keys in metadata used to identify a resource
    resource_keys =  ["resource_group","resource_id"]


class ResourceRepositoryMetadata(ResourceRepositoryMetaMetadataMixin,BasicResourceRepositoryMetadata):
    pass

class GroupResourceRepositoryMetadata(ResourceRepositoryMetaMetadataMixin,BasicGroupResourceRepositoryMetadata):
    pass

class HistoryDataRepositoryMetadata(ResourceRepositoryMetaMetadataMixin,BasicHistoryDataRepositoryMetadata):
    pass

class GroupHistoryDataRepositoryMetadata(ResourceRepositoryMetaMetadataMixin,BasicGroupHistoryDataRepositoryMetadata):
    pass

class IndexedResourceRepositoryMetadata(ResourceRepositoryMetaMetadataMixin,IndexedResourceRepositoryMetadataMixin):
    metaclient_class = BasicResourceRepositoryMetadata
    resource_keys = metaclient_class.resource_keys
    meta_metadata_kwargs = [("resource_base_path","_resource_base_path"),("index_metaname","_metaname"),("archive","_archive"),('f_metaname','_f_metaname'),("logical_delete","_logical_delete")]

class IndexedGroupResourceRepositoryMetadata(ResourceRepositoryMetaMetadataMixin,IndexedResourceRepositoryMetadataMixin):
    metaclient_class = BasicGroupResourceRepositoryMetadata
    resource_keys = metaclient_class.resource_keys
    meta_metadata_kwargs = [("resource_base_path","_resource_base_path"),("index_metaname","_metaname"),("archive","_archive"),('f_metaname','_f_metaname'),("logical_delete","_logical_delete")]

class IndexedHistoryDataRepositoryMetadata(ResourceRepositoryMetaMetadataMixin,IndexedHistoryDataRepositoryMetadataMixin):
    metaclient_class = BasicHistoryDataRepositoryMetadata
    resource_keys = metaclient_class.resource_keys
    meta_metadata_kwargs = [("resource_base_path","_resource_base_path"),("index_metaname","_metaname"),('f_metaname','_f_metaname')]

class IndexedGroupHistoryDataRepositoryMetadata(ResourceRepositoryMetaMetadataMixin,IndexedHistoryDataRepositoryMetadataMixin):
    metaclient_class = BasicGroupHistoryDataRepositoryMetadata
    resource_keys = metaclient_class.resource_keys
    meta_metadata_kwargs = [("resource_base_path","_resource_base_path"),("index_metaname","_metaname"),('f_metaname','_f_metaname')]

class ResourceRepositoryBase(object):
    """
    A base client to manage a resource repository
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

class HistoryDataRepositoryBase(ResourceRepositoryBase):
    """
    A base client to manage history data repository
    """

    @property
    def last_resource(self):
        return self._metadata_client.last_resource

    @property
    def last_resource_id(self):
        return self._metadata_client.last_resource_id

    @property
    def logical_delete(self):
        return False

    @property
    def archive(self):
        return False

    def push_resource(self,data,metadata,f_post_push=None,length=None):
        """
        Push the resource to the storage
        f_post_push: a function to call after pushing resource to blob container but before pushing the metadata, has one parameter "metadata"
        Return the new resourcemetadata.
        throw 
            ResourceAlreadyExist if resurce already exists
            InvalidResource if resource id is not greater than the last resource id
        """
        #check whether resource exists or not
        try:
            if len(self._metadata_client.resource_keys) == 1:
                resource_id = metadata[self._metadata_client.resource_keys[0]]
            else:
                resource_id = [metadata[k] for k in self._metadata_client.resource_keys]
        except KeyError as  ex:
            raise Exception("Missing resource key in metadata,{}".format(str(ex)))

        last_resource_id = self._metadata_client.last_resource_id
        result = compare_resource_id(resource_id,last_resource_id)
        if result == 0:
            raise exceptions.ResourceAlreadyExist("Can't update existing history data({})".format(resource_id))
        elif result == -1:
            index = self._metadata_client.find_resource_index(resource_id)
            if index == -1:
                raise exceptions.InvalidResource("The resource id({}) of the new history data must be greater than the last resource id({}) of the last history data".format(resource_id,last_resource_id))
            else:
                raise exceptions.ResourceAlreadyExist("Can't update existing history data({})".format(resource_id))

        return super().push_resource(data,metadata,f_post_push=f_post_push,length=length)

    def push_file(self,filename,metadata=None,f_post_push=None):
        """
        Push the resource from file to the storage
        f_post_push: a function to call after pushing resource to blob container but before pushing the metadata, has one parameter "metadata"
        Return the new resourcemetadata.
        throw 
            ResourceAlreadyExist if resurce already exists
            InvalidResource if resource id is not greater than the last resource id
        """
        #check whether resource exists or not
        try:
            if len(self._metadata_client.resource_keys) == 1:
                resource_id = metadata[self._metadata_client.resource_keys[0]]
            else:
                resource_id = [metadata[k] for k in self._metadata_client.resource_keys]
        except KeyError as  ex:
            raise Exception("Missing resource key in metadata,{}".format(str(ex)))

        last_resource_id = self._metadata_client.last_resource_id
        result = compare_resource_id(resource_id,last_resource_id)
        if result == 0:
            raise exceptions.ResourceAlreadyExist("Can't update existing history data({})".format(resource_id))
        elif result == -1:
            index = self._metadata_client.find_resource_index(resource_id)
            if index == -1:
                raise exceptions.InvalidResource("The resource id({}) of the new history data must be greater than the last resource id({}) of the last history data".format(resource_id,last_resource_id))
            else:
                raise exceptions.ResourceAlreadyExist("Can't update existing history data({})".format(resource_id))

        return super().push_file(filename,metadata=metadata,f_post_push=f_post_push)


class HistoryDataCleanMixin(object):
    def get_earliest_id(self):
        """
        earliest_id is 
            resource id for HistoryDataRepository
            (resource_group,resource_id) for GroupHistoryDataRepository
            meta_name for indexed history data repository
        """
        raise NotImplementedError("The method 'get_earliest_id' Not Implemented")

    def auto_clean(self):
        max_resource_id = self.get_earliest_id()
        if not max_resource_id:
            return
        for resource_id,res_meta in self._metadata_client.resources_in_range(None,max_resource_id,max_resource_included=False):
            if len(self.resource_keys) == 1:
                self.delete_resource(resource_id)
            else:
                self.delete_resource(*resource_id)

    def push_resource(self,data,metadata,f_post_push=None,length=None):
        result = super().push_resource(data,metadata,f_post_push=f_post_push,length=length)
        try:
            self.auto_clean()
        except Exception as ex:
            logger.error("Failed to clean the history data.{}".format(str(ex)))
        return result

    def push_file(self,filename,metadata=None,f_post_push=None):
        result = super().push_file(filename,metadata=metadata,f_post_push=f_post_push)
        try:
            self.auto_clean()
        except Exception as ex:
            logger.error("Failed to clean the history data.{}".format(str(ex)))
        return result

class IndexedHistoryDataCleanMixin(HistoryDataCleanMixin):
    def get_earliest_id(self):

        return self._f_earliest_metaname(self.last_resource_id) if self._f_earliest_metaname else None

    def auto_clean(self):
        max_metaname = self.get_earliest_id()
        if not max_metaname:
            return
        
        while True:
            indexed_meta = self._metadata_client.json 
            if not indexed_meta:
                #can't find any metafile
                break

            metaname = indexed_meta[0][0]
            if metaname >= max_metaname:
                #the first metafile is greater than or equal with max_metaname
                break
            #remove all resoures in the first meta file
            for resource_id,res_meta in self._metadata_client.create_metadata_client(metaname).json:
                if len(self.resource_keys) == 1:
                    self.delete_resource(resource_id)
                else:
                    self.delete_resource(*resource_id)

class ResourceRepository(ResourceRepositoryBase):
    def __init__(self,storage,resource_name,resource_base_path=None,archive=False,metaname="metadata",cache=True,logical_delete=False):
        super().__init__(storage,resource_name,resource_base_path=resource_base_path)
        self._metadata_client = ResourceRepositoryMetadata(storage,resource_base_path=self._resource_base_path,cache=cache,metaname=metaname,archive=archive,logical_delete=logical_delete)

class GroupResourceRepository(ResourceRepositoryBase):
    def __init__(self,storage,resource_name,resource_base_path=None,archive=False,metaname="metadata",cache=True,logical_delete=False):
        super().__init__(storage,resource_name,resource_base_path=resource_base_path)
        self._metadata_client = GroupResourceRepositoryMetadata(storage,resource_base_path=self._resource_base_path,cache=cache,metaname=metaname,archive=archive,logical_delete=logical_delete)


class HistoryDataRepository(HistoryDataCleanMixin,HistoryDataRepositoryBase):
    def __init__(self,storage,resource_name,resource_base_path=None,metaname="metadata",cache=True,f_earliest_resource_id = None):
        super().__init__(storage,resource_name,resource_base_path=resource_base_path)
        self._metadata_client = HistoryDataRepositoryMetadata(storage,resource_base_path=self._resource_base_path,cache=cache,metaname=metaname)
        self._f_earliest_resource_id = f_earliest_resource_id

    def get_earliest_id(self):
        return self._f_earliest_resource_id(self.last_resource_id) if self._f_earliest_resource_id else None

class GroupHistoryDataRepository(HistoryDataCleanMixin,HistoryDataRepositoryBase):
    def __init__(self,storage,resource_name,resource_base_path=None,metaname="metadata",cache=True,f_earliest_group=None):
        super().__init__(storage,resource_name,resource_base_path=resource_base_path)
        self._metadata_client = GroupHistoryDataRepositoryMetadata(storage,resource_base_path=self._resource_base_path,cache=cache,metaname=metaname)
        self._f_earliest_group = f_earliest_group

    def get_earliest_id(self):
        return (self._f_earliest_group(self.last_resource_id),None) if self._f_earliest_group else None

class IndexedResourceRepository(ResourceRepositoryBase):
    def __init__(self,storage,resource_name,f_metaname,resource_base_path=None,archive=False,index_metaname="_metadata_index",cache=True,logical_delete=False):
        super().__init__(storage,resource_name,resource_base_path=resource_base_path)
        self._metadata_client = IndexedResourceRepositoryMetadata(storage,f_metaname,resource_base_path=self._resource_base_path,cache=cache,archive=archive,index_metaname=index_metaname,logical_delete=logical_delete)

class IndexedGroupResourceRepository(ResourceRepositoryBase):
    def __init__(self,storage,resource_name,f_metaname,resource_base_path=None,archive=False,index_metaname="_metadata_index",cache=True,logical_delete=False):
        super().__init__(storage,resource_name,resource_base_path=resource_base_path)
        self._metadata_client = IndexedGroupResourceRepositoryMetadata(storage,f_metaname,resource_base_path=self._resource_base_path,cache=cache,archive=archive,index_metaname=index_metaname,logical_delete=logical_delete)

class IndexedHistoryDataRepository(IndexedHistoryDataCleanMixin,HistoryDataRepositoryBase):
    def __init__(self,storage,resource_name,f_metaname,resource_base_path=None,index_metaname="_metadata_index",cache=True,f_earliest_metaname=None):
        super().__init__(storage,resource_name,resource_base_path=resource_base_path)
        self._metadata_client = IndexedHistoryDataRepositoryMetadata(storage,f_metaname,resource_base_path=self._resource_base_path,cache=cache,index_metaname=index_metaname)
        self._f_earliest_metaname = f_earliest_metaname

class IndexedGroupHistoryDataRepository(IndexedHistoryDataCleanMixin,HistoryDataRepositoryBase):
    def __init__(self,storage,resource_name,f_metaname,resource_base_path=None,index_metaname="_metadata_index",cache=True,f_earliest_metaname=None):
        super().__init__(storage,resource_name,resource_base_path=resource_base_path)
        self._metadata_client = IndexedGroupHistoryDataRepositoryMetadata(storage,f_metaname,resource_base_path=self._resource_base_path,cache=cache,index_metaname=index_metaname)
        self._f_earliest_metaname = f_earliest_metaname


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
        (HistoryDataRepository,HistoryDataRepositoryMetadata),
        (GroupHistoryDataRepository,GroupHistoryDataRepositoryMetadata),
        (IndexedHistoryDataRepository,IndexedHistoryDataRepositoryMetadata),
        (IndexedGroupHistoryDataRepository,IndexedGroupHistoryDataRepositoryMetadata),
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


class BasicConsumeClient(ResourceConsumeClients):
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
        return self.get_client_consume_status(self._clientid) or {}

    @property
    def clientid(self):
        return self._clientid

    @property
    def resource_keys(self):
        return self._resource_repository.resource_keys

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

    def _populate_resource_consume_status(self,consume_status,resource_status,res_meta,failed_msg):
        if resource_status == self.PHYSICALLY_DELETED:
            consume_status["resource_status"] = "Physically Deleted"
        elif resource_status == self.LOGICALLY_DELETED:
            consume_status["resource_status"] = "Logically Deleted"
        elif resource_status == self.UPDATED:
            consume_status["resource_status"] = "Update"
        elif resource_status == self.NEW:
            consume_status["resource_status"] = "New"
        else:
            consume_status["resource_status"] = "Reconsume"


        consume_status["consume_date"] = timezone.now()
        if failed_msg:
            consume_status["consume_failed_msg"] = failed_msg
        elif "consume_failed_msg" in consume_status:
            del consume_status["consume_failed_msg"]

        return consume_status


    def _update_client_consume_status(self,client_metadata,client_consume_status,resource_status,resource_ids,res_consume_status,res_meta,commit=True,failed_msg=None):
        if resource_status in (self.PHYSICALLY_DELETED,self.LOGICALLY_DELETED):
            if failed_msg:
                res_consume_status = self._populate_resource_consume_status(res_consume_status,resource_status,res_meta,failed_msg)
            else:
                self.remove_resource_consume_status(*resource_ids,consume_status=client_consume_status)

        elif resource_status == self.NEW:
            res_consume_status = self._populate_resource_consume_status({},resource_status,res_meta,failed_msg)
            self.set_resource_consume_status(*resource_ids,res_consume_status = res_consume_status,consume_status=client_consume_status)
        else:
            res_consume_status = self._populate_resource_consume_status(res_consume_status,resource_status,res_meta,failed_msg)

        client_metadata["last_consumed_resource"] = resource_ids
        client_metadata["last_consumed_resource_status"] = res_consume_status["resource_status"]
        client_metadata["last_consume_date"] = timezone.now()
        if failed_msg:
            client_metadata["last_consume_failed_msg"] = failed_msg
        elif "last_consume_failed_msg" in client_metadata:
            del client_metadata["last_consume_failed_msg"]
        if commit:
            self.push_resource(json.dumps(client_consume_status,cls=JSONEncoder,sort_keys=True,indent=4).encode(),metadata=client_metadata)

    def _consume_resource(self,consume_metadata,client_consume_status,resource_status,resource_ids,res_consume_status,res_meta,callback):
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

        res_file = None
        try:
            if res_meta:
                res_file = self._resource_repository.download_resource(*resource_ids,resource_status=ResourceConstant.ALL_RESOURCE)[1]
        
            callback(resource_status,res_meta or res_consume_status["resource_metadata"],res_file)
            self._update_client_consume_status(consume_metadata,client_consume_status,resource_status,resource_ids,res_consume_status,res_meta)
        except Exception as ex:
            resource_status_name = self.get_consume_status_name(resource_status)
            self._update_client_consume_status(consume_metadata,client_consume_status,resource_status,resource_ids,res_consume_status,res_meta,failed_msg=str(ex))
            msg = "Failed to consume the {} resource({}).{}".format(resource_status_name,resource_ids,traceback.format_exc())
            logger.error(msg)
            raise exceptions.ResourceConsumeFailed(msg)
        finally:
            remove_file(res_file)

class ResourceConsumeClient(BasicConsumeClient):
    def _populate_resource_consume_status(self,consume_status,resource_status,res_meta,failed_msg):
        consume_status = super()._populate_resource_consume_status(consume_status,resource_status,res_meta,failed_msg)
        consume_status["resource_metadata"] = res_meta
        return consume_status

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
            return consume_status
    
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

    def consume(self,callback,resources=None,reconsume=False,sortkey_func=None,stop_if_failed=True):
        """
        resources: the list of resource id, or a filter which take the arugments (resource ids) for consuming.
        stop_if_failed: only useful for callback per resource
        callback: two mode
            callback per resource,callback's parameters is : resource_status,res_meta,res_file
            callback for all resource, callback's parameter is list of [resource_status,res_meta,res_file]
        Return a tuple([resource_status,resource_status_name,resource_ids],[resource_status,resource_status_name,resource_ids,str(ex)])
        """
        f_args = inspect.getfullargspec(callback)
        if len(f_args.args) == 1:
            callback_per_resource = False
        elif len(f_args.args) == 3:
            callback_per_resource = True
        else:
            raise Exception("Callback should have one parameter(list of tuple(resource_status,resource_metadata,file_name) to run in batch mode ,or have three parameters (resource_status,resource_metadata,file_name) to run in callback per resource mode")

        client_consume_status = self.consume_status

        resource_status = self.NOT_CHANGED
        metadata = {
            "resource_id":self._clientid,
            "last_consume_host":socket.getfqdn(),
            "last_consume_pid":os.getpid()
        }
        resource_keys = self._resource_repository._metadata_client.resource_keys
        consume_result = ([],[])
        updated_resources = []

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
                            resource_status = self.PHYSICALLY_DELETED
                            resource_status_name = self.get_consume_status_name(resource_status)
                            try:
                                self._consume_resource(metadata,client_consume_status,resource_status,resource_ids,res_consume_status,None,callback)
                                consume_result[0].append((resource_status,resource_status_name,resource_ids))
                            except exceptions.ResourceConsumeFailed as ex:
                                consume_result[1].append((resource_status,resource_status_name,resource_ids,str(ex)))
                                if stop_if_failed:
                                    return consume_result
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
                    resource_status_name = self.get_consume_status_name(resource_status)
                    try:
                        self._consume_resource(metadata,client_consume_status,resource_status,resource_ids,res_consume_status,res_meta,callback)
                        consume_result[0].append((resource_status,resource_status_name,resource_ids))
                    except exceptions.ResourceConsumeFailed as ex:
                        consume_result[1].append((resource_status,resource_status_name,resource_ids,str(ex)))
                        if stop_if_failed:
                            return consume_result
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
                    resource_status_name = self.get_consume_status_name(resource_status)
                    try:
                        self._consume_resource(metadata,client_consume_status,resource_status,resource_ids,res_consume_status,res_meta,callback)
                        consume_result[0].append((resource_status,resource_status_name,resource_ids))
                    except exceptions.ResourceConsumeFailed as ex:
                        consume_result[1].append((resource_status,resource_status_name,resource_ids,str(ex)))
                        if stop_if_failed:
                            return consume_result
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
                        resource_status = self.PHYSICALLY_DELETED
                        resource_status_name = self.get_consume_status_name(resource_status)
                        try:
                            self._consume_resource(metadata,client_consume_status,resource_status,resource_ids,res_consume_status,None,callback)
                            consume_result[0].append((resource_status,resource_status_name,resource_ids))
                        except exceptions.ResourceConsumeFailed as ex:
                            consume_result[1].append((resource_status,resource_status_name,resource_ids,str(ex)))
                            if stop_if_failed:
                                return consume_result
                    else:
                        updated_resources.append((self.PHYSICALLY_DELETED,resource_ids,res_consume_status,None))

        if updated_resources:
            if sortkey_func:
                updated_resources.sort(key=sortkey_func)
            if callback_per_resource :
                for updated_resource in updated_resources:
                    resource_status,resource_ids,res_consume_status,res_meta = updated_resource
                    resource_status_name = self.get_consume_status_name(resource_status)
                    try:
                        self._consume_resource(metadata,client_consume_status,resource_status,resource_ids,res_consume_status,res_meta,callback)
                        consume_result[0].append((resource_status,resource_status_name,resource_ids))
                    except exceptions.ResourceConsumeFailed as ex:
                        consume_result[1].append((resource_status,resource_status_name,resource_ids,str(ex)))
                        if stop_if_failed:
                            return consume_result
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
                        self._update_client_consume_status(metadata,client_consume_status,*updated_resource,commit=False)
                    #push client consume status to blob storage
                    self.push_resource(json.dumps(client_consume_status,cls=JSONEncoder,sort_keys=True,indent=4).encode(),metadata=metadata)
                finally:
                    #remote temporary files
                    for res_status,res_meta,res_file in callback_arguments:
                        remove_file(res_file)
                
        return consume_result


class HistoryDataConsumeClient(BasicConsumeClient):
    RECENT_RESOURCES_CONSUME_STATUS_KEY = "recent_resources_consume_status"
    
    def __init__(self,storage,resource_name,clientid,resource_base_path=None,max_saved_consumed_resources=None):
        """
        max_saved_consumed_resources: save all resources' consume status if it is None; or save up to max_save_consumed_resources' consume status by removing the oldest resouces' consume status
        """
        super().__init__(storage,resource_name,clientid,resource_base_path=resource_base_path)
        self._max_saved_consumed_resources = max_saved_consumed_resources if max_saved_consumed_resources > 0 else None

    @property
    def last_consumed_resource_id(self):
        consume_status = self.consume_status
        if not consume_status:
            return None

        if self.RECENT_RESOURCES_CONSUME_STATUS_KEY not in consume_status:
            return None

        recent_resources_consume_status = consume_status[self.RECENT_RESOURCES_CONSUME_STATUS_KEY]

        index = len(recent_resources_consume_status) - 1
        while index >= 0:
            if recent_resources_consume_status[index][1].get("consume_failed_msg"):
                index -= 1
                continue
            return recent_resources_consume_status[index][0]

        return None


    def get_resource_consume_status(self,*args,consume_status=None):
        """
        Get the resource consume status; return None if not consumed before.
        """
        consume_status = self.consume_status if consume_status is None else consume_status
        if not consume_status:
            return None
    
        if self.RECENT_RESOURCES_CONSUME_STATUS_KEY not in consume_status:
            return None

        recent_resources_consume_status = consume_status[self.RECENT_RESOURCES_CONSUME_STATUS_KEY]
        if len(self.resource_keys) == 1:
            index = find_resource_index(recent_resources_consume_status,args[0])
        else:
            index = find_resource_index(recent_resources_consume_status,args)

        if index == -1:
            return None
        else:
            return recent_resources_consume_status[index][1]

    def set_resource_consume_status(self,*args,res_consume_status,consume_status=None):
        """
        Set the resource consume status; 
        Return the updated consume status
        """
        consume_status = self.consume_status if consume_status is None else consume_status
        if consume_status is None:
            consume_status = {}
    
        if self.RECENT_RESOURCES_CONSUME_STATUS_KEY not in consume_status:
            consume_status[self.RECENT_RESOURCES_CONSUME_STATUS_KEY] = []

        recent_resources_consume_status = consume_status[self.RECENT_RESOURCES_CONSUME_STATUS_KEY]

        if len(self.resource_keys) == 1:
            index = find_resource_index(recent_resources_consume_status,args[0])
        else:
            index = find_resource_index(recent_resources_consume_status,args)

        if index == -1:
            #consume a new resource
            if len(self.resource_keys) == 1:
                recent_resources_consume_status.append([args[0],consume_status])
            else:
                recent_resources_consume_status.append([args,consume_status])

            if self._max_saved_consumed_resources and len(recent_resources_consume_status) > self._max_saved_consumed_resources:
                recent_resources_consume_status = recent_resources_consume_status[-1 * self._max_saved_consumed_resources:]

            if "first_consume_resource" not in consume_status:
                consume_status["first_consume_resource"] = args
                consume_status["first_consume_time"] = timezone.now()
            consume_status["consumed_resources"] = consume_status.get("consumed_resources",0) + 1
            
        else:
            #reconsume a existing resource 
            raise exception.OperationNotSupporat("Can't reconsume a history data({})".format(args))

        return consume_status


    def remove_resource_consume_status(self,*args,consume_status=None):
        """
        Remove the resource consume status; 
        return the updated consume status
        """
        raise exception.OperationNotSupporat("Can't consume a deleted history data({})".format(args))

    def is_behind(self):
        """
        Return True if some resource is changed after last consuming;otherwise return False
        """
        client_consume_status = self.consume_status

        resource_keys = self.resource_keys
        #find new and updated resources
        result = compare_resource_id(self.last_consumed_resource_id,self._resource_repository.last_resource_id)
        if result == -1:
            return True
        elif result == 0:
            return False
        else:
            raise exceptions.InvalidConsumeStatus("Last consumed resource id() is greater than the last resource id({}) in the resource repository".foramt(
                self.last_consumed_resource_id,self._resource_repository.last_resource_id
            ))


    def consume(self,callback):
        """
        callback: callback's parameters is : resource_status,res_meta,res_file
        Return a tuple([resource_status,resource_status_name,resource_ids],[resource_status,resource_status_name,resource_ids,str(ex)])
        """
        client_consume_status = self.consume_status

        resource_status = self.NOT_CHANGED
        metadata = {
            "resource_id":self._clientid,
            "last_consume_host":socket.getfqdn(),
            "last_consume_pid":os.getpid()
        }
        resource_keys = self._resource_repository._metadata_client.resource_keys
        consume_result = ([],[])

        #find new and updated resources
        metadata = self._resource_repository.metdata_client.json 
        index = find_resource_index(metadata,self.last_consumed_resource_id,policy=GREATER)
        if index == -1:
            return consume_result

        for resource_ids,res_meta in self._resource_repository.metadata_client.resources_in_range(self.last_consumed_resource_id,None,min_resource_included=False):
            res_consume_status = self.get_resource_consume_status(*resource_ids,consume_status=client_consume_status)

            if not res_consume_status:
                resource_status = self.NEW
            elif res_consume_status.get("consume_failed_msg"):
                if res_consume_status["resource_status"] == "New":
                    resource_status = self.NEW
                else:
                    raise exceptions.InvalidConsumeStatus("The resource({}), which is later than the last successfully consumed resource, was alread consumed.".format(resource_ids))
            else:
                #reosurce was consumed before
                logger.debug("The resource({},{}) is not changed after last consuming".format(resource_ids,res_meta["resource_path"]))
                raise exceptions.InvalidConsumeStatus("The resource({}), which is later than the last successfully consumed resource, was already consumed".format(resource_ids))
            
            resource_status_name = self.get_consume_status_name(resource_status)
            try:
                self._consume_resource(metadata,client_consume_status,resource_status,resource_ids,res_consume_status,res_meta,callback)
                consume_result[0].append((resource_status,resource_status_name,resource_ids))
            except exceptions.ResourceConsumeFailed as ex:
                consume_result[1].append((resource_status,resource_status_name,resource_ids,str(ex)))
                break

        return consume_result


