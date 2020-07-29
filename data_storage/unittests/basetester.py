import unittest
import json
import os
import time
import logging
from collections import OrderedDict

from data_storage import get_resource_repository,ResourceConstant,ResourceConsumeClient,ResourceConsumeClients
from data_storage.utils import timezone,JSONEncoder,remove_file,remove_folder
from data_storage import exceptions

from . import settings

logger = logging.getLogger(__name__)

class TestException(Exception):
    pass

class BaseTesterMixin(object):
    resource_name = "data_storage"
    resource_base_path = None

    archive=True
    cache=True
    logical_delete=False

    prefix = ""

    @property
    def resource_repository(self):
        """
        Return the resource repository to manage the resource
        """
        if not hasattr(self,"_resource_repository") or any(getattr(self._resource_repository,prop) != value for prop,value in [
            ("archive",self.archive),
            ("cache",self.cache),
            ("logical_delete",self.logical_delete)
        ]):
            self._resource_repository = self.create_resource_repository()
            self.prefix = "{}(archive={},logical_delete={}):".format(self.__class__.__name__,self.archive,self.logical_delete)
        return self._resource_repository

    def create_resource_repository(self):
        """
        Create the resource repository to manage the resource
        """
        raise NotImplementedError("Not Implemented")


    def clean_resources(self):
        """
        Clean all resources from the resource repository
        """
        #clean all resource in resource repository
        logger.debug("Clean all resources from the resource repository")
        try:
            get_resource_repository(self.storage,self.resource_name,resource_base_path=self.resource_base_path).delete_resources(permanent_delete=True)
            if hasattr(self,"_resource_repository"):
                delattr(self,"_resource_repository")
        except exceptions.MetaMetadataMissing as ex:
            pass

    def populate_test_data(self,resource_id):
        """
        Populate the test data for the resource specified by resource id
        """
        content_json = {'message':"{}: this is the test content for {}".format(timezone.now().strftime("%Y-%m-%d"),resource_id),"time":timezone.now()}
        content = json.dumps(content_json,cls=JSONEncoder)
        content_byte = content.encode()
        metadata = dict(zip(self.resource_repository.resource_keys,resource_id))
        return [metadata,content,content_json,content_byte]

    def populate_test_datas(self):
        resource_ids = self.get_test_data_keys()
        testdatas = OrderedDict()
        for resource_id in resource_ids:
            testdatas[resource_id] = self.populate_test_data(resource_id)

        return testdatas

    def populate_test_datas2(self):
        resource_ids = self.get_test_data_keys2()
        testdatas = OrderedDict()
        for resource_id in resource_ids:
            testdatas[resource_id] = self.populate_test_data(resource_id)

        return testdatas

    def prepare_test_datas(self):
        """
        Prepare the test datas in resource repository
        """
        if self.archive:
            logger.info("{}Prepare the test datas for archiving resource".format(self.prefix))
            metadatas = {}
            #publish multiple resource with multiple arcive to storage
            for i in range(0,3):
                current_metadatas = self.populate_test_datas()
                for resource_id,data in current_metadatas.items():
                    metadata,content,content_json,content_byte = data
                    if resource_id in metadatas:
                        if "histories" in metadatas[resource_id]:
                            metadatas[resource_id]["histories"].insert(0,metadatas[resource_id]["current"])
                        else:
                            metadatas[resource_id]["histories"] = [metadatas[resource_id]["current"]]
    
                        metadatas[resource_id]["current"] = data
                    else:
                        metadatas[resource_id] = {"current":data}
    
                    self.resource_repository.push_resource(content_byte,metadata)
    
                time.sleep(1)
        else:
            logger.info("Prepare the test datas for non-archiving resource")
            metadatas = self.populate_test_datas()
            for resource_id,data in metadatas.items():
                metadata,content,content_json,content_byte = data
                #test push_resource
                self.resource_repository.push_resource(content_byte,metadata)

        return metadatas

    def prepare_test_datas2(self):
        """
        Prepare the test datas in resource repository
        """
        if self.archive:
            logger.info("{}Prepare the test datas2 for archiving resource".format(self.prefix))
            metadatas = {}
            #publish multiple resource with multiple arcive to storage
            current_metadatas = self.populate_test_datas2()
            for resource_id,data in current_metadatas.items():
                metadata,content,content_json,content_byte = data
                if resource_id in metadatas:
                    if "histories" in metadatas[resource_id]:
                        metadatas[resource_id]["histories"].insert(0,metadatas[resource_id]["current"])
                    else:
                        metadatas[resource_id]["histories"] = [metadatas[resource_id]["current"]]
    
                    metadatas[resource_id]["current"] = data
                else:
                    metadatas[resource_id] = {"current":data}
    
                self.resource_repository.push_resource(content_byte,metadata)
    
        else:
            logger.info("Prepare the test datas2 for non-archiving resource")
            metadatas = self.populate_test_datas2()
            for resource_id,data in metadatas.items():
                metadata,content,content_json,content_byte = data
                #test push_resource
                self.resource_repository.push_resource(content_byte,metadata)

        return metadatas

    def get_resource_id(self,metadata):
        """
        Return the resource id from the resource's metadata
        metadata: a metadata of a archiving resource, or a metadata of a specific archive of a archiving resource, or a metadata of a non-archiving resource
        """
        if self.archive:
            if "current" in metadata:
                meta = metadata["current"]
            else:
                meta = metadata
        else:
            meta = metadata

        return tuple(meta[key] for key in self.resource_repository.resource_keys)
    
    def filter_testdatas(self,testdatas,**kwargs):
        """
        Return a filterd test data dict object from the testdatas dict object against the resource_id keyword arguments.
        """
        def _is_match(resource_id,filter_id):
            index = 0
            while index < len(filter_id):
                if resource_id[index] != filter_id[index]:
                    return False
                index += 1

            return True
        filter_id = tuple(kwargs[k] for k in self.resource_repository.resource_keys[0:len(kwargs)])
        return dict((k,v) for k,v in testdatas.items() if _is_match(k,filter_id))

    def get_filter_params(self,filter_id):
        """
        convert to filter_id(a tuple) to filter parmeters(a dict)
        """
        filter_params = {}
        index = 0
        while index < len(filter_id):
            filter_params[self.resource_repository.resource_keys[index]] = filter_id[index]
            index += 1

        return filter_params

    def republish_resources(self,testdatas,resource_ids=None):
        """
        publis all or selected test datas in testdatas dict object
        resource_ids :
           None: for all resource
           tuple : single resource
           list(tuple): list of resources
        """
        if resource_ids:
            logger.debug("{}Publish resources({})".format(self.prefix,resource_ids))   
        else:
            logger.debug("{}Publish resources({})".format(self.prefix,[d for d in testdatas.keys()]))   

        if resource_ids and not isinstance(resource_ids,list):
            resource_ids = [resource_ids]
        if self.archive:
            time.sleep(1)
            for resource_id,res_metadata in testdatas.items():
                if resource_ids and resource_id not in resource_ids:
                    continue
                data= self.populate_test_data(resource_id)
                metadata,content,content_json,content_byte = data
                if "histories" in res_metadata:
                    res_metadata["histories"].insert(0,res_metadata["current"])
                else:
                    res_metadata["histories"] = [res_metadata["current"]]
    
                res_metadata["current"] = data
    
                self.resource_repository.push_resource(content_byte,metadata)
                if ResourceConstant.DELETED_KEY in res_metadata:
                    del res_metadata[ResourceConstant.DELETED_KEY]
        else:
            for resource_id,data in testdatas.items():
                if resource_ids and resource_id not in resource_ids:
                    continue
                new_data= self.populate_test_data(resource_id)
                metadata,content,content_json,content_byte = new_data
                self.resource_repository.push_resource(content_byte,metadata)
                data[0] = metadata
                data[1] = content
                data[2] = content_json
                data[3] = content_byte

    def check_resources(self,testdatas):
        """
        Check whether the methods provided by resource repository are correct or not against all test datas in testdatas
        """
        if self.archive:
            logger.debug("{}Check achiving resource repository's methods against all test datas".format(self.prefix))   
            for resource_id,archive_testdata in testdatas.items():
                deleted = archive_testdata.get(ResourceConstant.DELETED_KEY)
                metadata,content,content_json,content_byte = archive_testdata["current"]
                self.check_resource(resource_id,metadata,content,content_json,content_byte,deleted=deleted,resource_file=metadata["resource_file"])
                self.check_resource(resource_id,metadata,content,content_json,content_byte,deleted=deleted,resource_file="current")


                for data in archive_testdata.get("histories",[]):
                    #test is_exist
                    metadata,content,content_json,content_byte = data
                    self.check_resource(resource_id,metadata,content,content_json,content_byte,deleted=deleted,resource_file=metadata["resource_file"])
        else:
            logger.debug("{}Check non-achiving resource repository's methods against all test datas".format(self.prefix))   
            for resource_id,testdata in testdatas.items():
                metadata,content,content_json,content_byte = testdata
                deleted = metadata.get(ResourceConstant.DELETED_KEY)
                self.check_resource(resource_id,metadata,content,content_json,content_byte,deleted=deleted,resource_file=metadata["resource_file"])

        self.check_resource_metadatas(testdatas)
        self.check_download_resources(testdatas)

    def check_resource(self,resource_id,metadata,content,content_json,content_byte,deleted=False,resource_file="current"):
        """
        Check whether  methods provided by resource repository are correct or not against a single test data
        """
        resource_status = ResourceConstant.DELETED_RESOURCE if deleted else ResourceConstant.NORMAL_RESOURCE
        resource_status_name = "deleted" if deleted else "pushed"
        self.check_is_exist(resource_id,deleted=deleted,resource_file=resource_file)

        self.check_get_resource_metadata(resource_id,metadata,deleted=deleted,resource_file=resource_file)

        self.check_get_content(resource_id,metadata,content_byte,deleted=deleted,resource_file=resource_file)

        self.check_get_text(resource_id,metadata,content,deleted=deleted,resource_file=resource_file)

        self.check_get_json(resource_id,metadata,content_json,deleted=deleted,resource_file=resource_file)

        self.check_download_resource(metadata,content_byte,deleted=deleted,resource_file=resource_file)

    def check_is_exist(self,resource_id,deleted=False,resource_file="current"):
        """
        Check the method "is_exist"
        """
        logger.debug("{}Check the method 'is_exist' against resource({})".format(self.prefix,resource_id))   
        resource_status = ResourceConstant.DELETED_RESOURCE if deleted else ResourceConstant.NORMAL_RESOURCE
        resource_status_name = "deleted" if deleted else "pushed"
        self.assertEqual(self.resource_repository.is_exist(*resource_id,resource_file=resource_file,resource_status=resource_status),True,"The {} resource({}) Not Found!".format(resource_status_name,resource_id))

    def check_get_resource_metadata(self,resource_id,metadata,deleted=False,resource_file="current"):
        """
        Check the method "get_resource_metadata"
        """
        logger.debug("{}Check the method 'get_resource_metadata' against resource({})".format(self.prefix,resource_id))   
        resource_status = ResourceConstant.DELETED_RESOURCE if deleted else ResourceConstant.NORMAL_RESOURCE
        resource_status_name = "deleted" if deleted else "pushed"
        res_metadata = self.resource_repository.get_resource_metadata(*resource_id,resource_status=resource_status,resource_file=resource_file)
        self.assertEqual(res_metadata,metadata,"The {} resource's metadata({}) is not equal with the original content's metadata({})".format(resource_status_name,res_metadata,metadata))
    
    def check_get_content(self,resource_id,metadata,content_byte,deleted=False,resource_file="current"):
        """
        Check the method "get_content"
        """
        logger.debug("{}Check the method 'get_content' against resource({})".format(self.prefix,resource_id))   
        resource_status = ResourceConstant.DELETED_RESOURCE if deleted else ResourceConstant.NORMAL_RESOURCE
        resource_status_name = "deleted" if deleted else "pushed"
        download_content_byte = self.resource_repository.get_content(*resource_id,resource_status=resource_status,resource_file=resource_file)[1]
        self.assertEqual(download_content_byte,content_byte,"The {} resource's content({}) is not equal with the original content({})".format(resource_status_name,download_content_byte,content_byte))
    
    def check_get_text(self,resource_id,metadata,content,deleted=False,resource_file="current"):
        """
        Check the method "get_text"
        """
        logger.debug("{}Check the method 'get_text' against resource({})".format(self.prefix,resource_id))   
        resource_status = ResourceConstant.DELETED_RESOURCE if deleted else ResourceConstant.NORMAL_RESOURCE
        resource_status_name = "deleted" if deleted else "pushed"
        download_content = self.resource_repository.get_text(*resource_id,resource_status=resource_status,resource_file=resource_file)[1]
        self.assertEqual(download_content,content,"The {} resource's content({}) is not equal with the original content({})".format(resource_status_name,download_content,content))
    
    def check_get_json(self,resource_id,metadata,content_json,deleted=False,resource_file="current"):
        """
        Check the method "get_json"
        """
        logger.debug("{}Check the method 'get_json' against resource({})".format(self.prefix,resource_id))   
        resource_status = ResourceConstant.DELETED_RESOURCE if deleted else ResourceConstant.NORMAL_RESOURCE
        resource_status_name = "deleted" if deleted else "pushed"
        download_content_json = self.resource_repository.get_json(*resource_id,resource_status=resource_status,resource_file=resource_file)[1]
        self.assertEqual(download_content_json,content_json,"The {} resource's content({}) is not equal with the original content({})".format(resource_status_name,download_content_json,content_json))
    
    def check_download_resource(self,metadata,content_byte,resource_file="current",deleted=False):
        #test download_resource without filename
        """
        check the method "download_resource"
        """
        resource_status = ResourceConstant.DELETED_RESOURCE if deleted else ResourceConstant.NORMAL_RESOURCE
        resource_status_name = "deleted" if deleted else "pushed"

        resource_id = self.get_resource_id(metadata)
        logger.debug("{}Check the method 'down_resource' against resource({})".format(self.prefix,resource_id))   
        filename = '/tmp/test1'
        remove_file(filename)
        download_filename = None
        for f,overwrite,throw_exception in [(None,False,False),(filename,False,False),(filename,True,False),(filename,False,True)]:
            try:
                res_metadata,download_filename = self.resource_repository.download_resource(*resource_id,filename=f,overwrite=overwrite,resource_file=resource_file,resource_status=resource_status)
                if f :
                    self.assertEqual(download_filename ,f,"Downloaded filename({}) is not the same file requested({})".format(download_filename,f))

                self.assertEqual(res_metadata ,metadata,"The {} resource's metadata({}) is not equal with the original content's metadata({})".format(resource_status_name,res_metadata,metadata))
                with open(download_filename,'rb') as f:
                    file_content = f.read()
                self.assertEqual(file_content ,content_byte,"The {} resource's content({}) is not equal with the original content's content({})".format(resource_status_name,file_content,content_byte))
            except Exception as ex:
                self.assertTrue(throw_exception ,"Download the {} resource({}) to file '{}' failed,{}".format(resource_status_name,resource_id,filename,str(ex)))
            finally:
                remove_file(download_filename)
            
    def check_resource_metadatas(self,testdatas):
        """
        Check the method "resource_metadatas against the test datas"
        """
        logger.debug("{}Check the method 'resource_metadatas' against test datas".format(self.prefix))   
        if self.logical_delete:
            resource_status_list = [
                (ResourceConstant.NORMAL_RESOURCE,"pushed",lambda meta:not meta.get(ResourceConstant.DELETED_KEY,False)),
                (ResourceConstant.DELETED_RESOURCE,"deleted",lambda meta:meta.get(ResourceConstant.DELETED_KEY,False)),
                (ResourceConstant.ALL_RESOURCE,"pushed/deleted",lambda meta:True)
            ]
        else:
            resource_status_list = [(ResourceConstant.ALL_RESOURCE,"pushed",lambda meta:True)]
        for resource_status,status_name,filter_func in resource_status_list:
            current_testdatas = dict([(k,v) for k,v in testdatas.items() if filter_func(v if self.archive else v[0])])
            if self.archive :
                #check latest archive
                res_metadatas = [m for m in self.resource_repository.resource_metadatas(throw_exception=False,resource_status=resource_status,current_resource=True)]
                self.assertEqual(len(res_metadatas) if res_metadatas else 0,len(current_testdatas),"The number of  metadata({}) returned by resource_metadatas is not equal with the the number of metadata({})".format(len(res_metadatas) if res_metadatas else 0,len(current_testdatas)))
                for res_metadata in res_metadatas:
                    self.check_metadata_equal(res_metadata,current_testdatas)

                #check all archives
                res_metadatas = [m for m in self.resource_repository.resource_metadatas(throw_exception=False,resource_status=resource_status,current_resource=False)]
                self.assertEqual(len(res_metadatas) if res_metadatas else 0,len(current_testdatas),"The number of  metadata({}) returned by resource_metadatas is not equal with the the number of metadata({})".format(len(res_metadatas) if res_metadatas else 0,len(current_testdatas)))
                for res_metadata in res_metadatas:
                    self.check_metadata_equal(res_metadata,current_testdatas)

            else:
                #test resource_metadata
                res_metadatas = [m for m in self.resource_repository.resource_metadatas(throw_exception=False,resource_status=resource_status)]
                self.assertEqual(len(res_metadatas) if res_metadatas else 0,len(current_testdatas),"The number of  metadata({}) returned by resource_metadatas is not equal with the the number of metadata({})".format(len(res_metadatas) if res_metadatas else 0,len(current_testdatas)))
                for res_metadata in res_metadatas:
                    self.check_metadata_equal(res_metadata,current_testdatas)

    def check_download_resources(self,testdatas):
        #test download_resources
        """
        Check the method "download_resources"
        """
        logger.debug("{}Check the method 'download_resources' against test datas".format(self.prefix))   
        if self.logical_delete:
            resource_status_list = [
                (ResourceConstant.NORMAL_RESOURCE,"pushed",lambda meta:not meta.get(ResourceConstant.DELETED_KEY,False)),
                (ResourceConstant.DELETED_RESOURCE,"deleted",lambda meta:meta.get(ResourceConstant.DELETED_KEY,False)),
                (ResourceConstant.ALL_RESOURCE,"pushed/deleted",lambda meta:True)
            ]
        else:
            resource_status_list = [(ResourceConstant.ALL_RESOURCE,"pushed",lambda meta:True)]

        folder = '/tmp/testdir'
        remove_folder(folder)
        for resource_status,status_name,filter_func in resource_status_list:
            download_folder = None
            current_testdatas = dict([(k,v) for k,v in testdatas.items() if filter_func(v if self.archive else v[0])])
            for f,overwrite,throw_exception in [(None,False,False),(folder,False,False),(folder,True,False),(folder,False,True)]:
                try:
                    res_metadatas,download_folder = self.resource_repository.download_resources(folder=f,overwrite=overwrite,resource_status=resource_status)
                    if f :
                        self.assertEqual(download_folder ,f,"Download folder({}) is not the same folder requested({})".format(download_folder,f))
                    self.assertEqual(len(res_metadatas) if res_metadatas else 0,len(current_testdatas),"The number of  metadata({}) returned by download_resources is not equal with the the number of metadata({})".format(len(res_metadatas) if res_metadatas else 0,len(current_testdatas)))
                    for res_metadata in res_metadatas:
                        self.check_metadata_equal(res_metadata,testdatas)
                        resource_id = self.get_resource_id(res_metadata)
                        if self.archive:
                            metadata,content,content_json,content_byte = current_testdatas[resource_id]["current"]
                        else:
                            metadata,content,content_json,content_byte = current_testdatas[resource_id]
                        file_content = None
                        file_path = self.resource_repository.get_download_path(res_metadata,download_folder)
                        if os.path.exists(file_path):
                            with open(file_path,'r') as f:
                                file_content = f.read()
                        self.assertEqual(file_content ,content,"The content of the resource({}) returned by download_resources is not equal with the expected content({})".format(file_content,content))
                except Exception as ex:
                    self.assertTrue(throw_exception ,"Download resources to folder '{}' failed,{}".format(f,str(ex)))
                finally:
                    remove_folder(download_folder)

    def get_metadata(self,repo_metadata,resource_id):
        """
        Return the resource's metadata from the repository's metadata
        """
        p_metadata = repo_metadata
        for res_id in resource_id[:-1]:
            p_metadata = p_metadata[res_id]

        return p_metadata[resource_id[-1]]

    def check_metadata_equal(self,res_metadata,testdatas):
        """
        Check whether the metatata of the resource is equal with the metadata in test datas
        """
        resource_id = self.get_resource_id(res_metadata)
        #logger.debug("{}Check whether the metatata of the resource({}) is equal with the metadata in test datas".format(self.prefix,resource_id))   
        if self.archive :
            if "current" not in res_metadata :
                res_testdata  = testdatas[resource_id]
                resource_file = res_metadata["resource_file"]
                if resource_file == "current" or resource_file == res_testdata["current"][0]["resource_file"]:
                    metadata,content,content_json,content_byte = res_testdata["current"]
                    self.assertEqual(res_metadata,metadata,"The metadata of the resource({}) returned by resource_testdatas is not equal with the expected metadata({})".format(res_metadata,metadata))
                else:
                    found = False
                    for metadata,content,content_json,content_byte in res_testdata.get("histories",[]):
                        if  resource_file == metadata["resource_file"]:
                            found = True
                            self.assertEqual(res_metadata,metadata,"The metadata of the resource({}) returned by resource_testdatas is not equal with the expected metadata({})".format(res_metadata,metadata))
                            break
                    self.assertTrue(found,"The metadata of the resource({}) returned by resource_testdatas is not expected".format(res_metadata))
            else:
                res_testdata  = testdatas[resource_id]
                self.assertEqual(res_metadata["current"],res_testdata["current"][0],"The metadata of the resource({}) returned by resource_testdatas is not equal with the expected metadata({})".format(res_metadata["current"],res_testdata["current"][0]))
                self.assertEqual(len(res_metadata["histories"]),len(res_testdata["histories"]),"The number of the resource's archives({}) returned by resource_testdatas is not equal with the expected archives({})".format(len(res_metadata["histories"]),len(res_testdata["histories"])))
                index = 0
                while index < len(res_metadata["histories"]):
                    metadata,content,content_json,content_byte = res_testdata["histories"][index]
                    self.assertEqual(res_metadata["histories"][index],metadata,"The metadata of the resource's history archive({}) returned by resource_testdatas is not equal with the expected metadata({})".format(res_metadata["histories"][index],metadata))
                    index += 1
        else:
            metadata,content,content_json,content_byte = testdatas[resource_id]
            self.assertEqual(res_metadata,metadata,"The metadata of the resource({}) returned by resource_testdatas is not equal with the original content's metadata({})".format(res_metadata,metadata))

    def check_delete_resources(self,testdatas):
        """
        Check the method "delete_resource" and "delete_resources"
        """
        logger.debug("{}Check the method 'delete_resource' and 'delete_resources' against test datas".format(self.prefix))   
        #test delete_resource
        resource_id = None
        for key in testdatas.keys():
            resource_id = key
        res_metadata = self.resource_repository.delete_resource(*resource_id,permanent_delete=True)
        self.check_metadata_equal(res_metadata,testdatas)
        del testdatas[resource_id]
        
        #test delete_resources with filter
        filter_len = len(self.resource_repository.resource_keys) - 1
        while filter_len > 0:
            filter_id = None
            for key in testdatas.keys():
                filter_id = key[:filter_len]

            filter_params = self.get_filter_params(filter_id)

            current_testdatas = self.filter_testdatas(testdatas,**filter_params)

            res_metadatas = self.resource_repository.delete_resources(permanent_delete=True,**filter_params)
            self.assertEqual(len(res_metadatas) if res_metadatas else 0,len(current_testdatas),"The number of  metadata({}) returned by delete_resources is not equal with the the number of metadata({})".format(len(res_metadatas) if res_metadatas else 0,len(current_testdatas)))
            for res_metadata in res_metadatas:
                resource_id = self.get_resource_id(res_metadata)
                self.check_metadata_equal(res_metadata,current_testdatas)
                del testdatas[resource_id]

            filter_len -= 1

            
        #test delete_resources
        res_metadatas = self.resource_repository.delete_resources(permanent_delete=True)
        self.assertEqual(len(res_metadatas) if res_metadatas else 0,len(testdatas),"The number of  metadata({}) returned by delete_resources is not equal with the the number of metadata({})".format(len(res_metadatas) if res_metadatas else 0,len(testdatas)))
        for res_metadata in res_metadatas:
            resource_id = self.get_resource_id(res_metadata)
            self.check_metadata_equal(res_metadata,testdatas)
            del testdatas[resource_id]

        res_metadatas = [m for m in self.resource_repository.resource_metadatas(throw_exception=False)]
        self.assertEqual(len(res_metadatas) if res_metadatas else 0,0,"The number of  metadata({}) should be 0 after deleting all resources".format(len(res_metadatas) if res_metadatas else 0,len(testdatas)))

    def check_storage_empty(self):
        """
        Check whether storage is empty or not
        """
        logger.debug("{}Check wheher the repository is returned to empty status after testing".format(self.prefix))   
        resources = self.storage.list_resources(path="data/")
        self.assertEqual(len(resources) if resources else 0,0,"The resources({}) are still in the storage".format(resources))

class TestResourceRepositoryMixin(BaseTesterMixin):
    def test_delete_4_logical_delete_resource(self):
        """
        Test delete  feature for logically delete supported non-archive resource
        """
        self.clean_resources()
        self.archive=False
        self.logical_delete=True

        repository = self.resource_repository
        logger.info("{}:Test delete features for logically delete supported non-archive resource".format(self.prefix))
        metadatas = self.prepare_test_datas()

        #logical delete two resources using method delete_resource 
        rows = 0
        for resource_id,data in metadatas.items():
            #delete them
            metadata,content,content_json,content_byte = data
            res_metadata = self.resource_repository.delete_resource(*resource_id)
            metadata[ResourceConstant.DELETED_KEY] = True
            rows += 1
            if rows >= 2:
                break

        #check the reources after logical deleting
        self.check_resources(metadatas)

        #logical delete some resources using the method delete_resources with filter 
        filter_len = len(self.resource_repository.resource_keys) - 1
        while filter_len > 0:
            filter_id = None
            for key in metadatas.keys():
                filter_id = key[:filter_len]

            filter_params = self.get_filter_params(filter_id)
            current_metadatas = self.filter_testdatas(metadatas,**filter_params)

            res_metadatas = self.resource_repository.delete_resources(permanent_delete=False,**filter_params)
            for resource_id,data in current_metadatas.items():
                metadata,content,content_json,content_byte = data
                metadata[ResourceConstant.DELETED_KEY] = True

            for res_metadata in res_metadatas:
                resource_id = self.get_resource_id(res_metadata)
                self.check_metadata_equal(res_metadata,current_metadatas)

            filter_len -= 1


        #check the reources after logical deleting
        self.check_resources(metadatas)

        #logically delete all resources using method delete_resources without filter
        res_metadatas = self.resource_repository.delete_resources(permanent_delete=False)
        for resource_id,data in metadatas.items():
            metadata,content,content_json,content_byte = data
            metadata[ResourceConstant.DELETED_KEY] = True


        #check the reources after logical deleting
        self.check_resources(metadatas)


        #republish the deleted resource 
        self.republish_resources(metadatas)

        #check the reources after logical deleting
        self.check_resources(metadatas)

        #delete the resources logically except the last one
        for resource_id,data in [(key,value) for key, value in metadatas.items()][:-1]:
            #delete them
            metadata,content,content_json,content_byte = data
            metadata[ResourceConstant.DELETED_KEY] = True
            res_metadata = self.resource_repository.delete_resource(*resource_id)
        
        #check permanently delete resource with delete_resource
        for resource_id,data in metadatas.items():
            res_metadata = self.resource_repository.delete_resource(*resource_id,permanent_delete=True)
            self.check_metadata_equal(res_metadata,metadatas)

        #check whether all resurces are deleted or not.
        self.check_storage_empty()

        #check permanently delete_resources
        metadatas = self.prepare_test_datas()

        #logically delete the resources except the last one
        for resource_id,data in [(key,value) for key, value in metadatas.items()][:-1]:
            #delete them
            metadata,content,content_json,content_byte = data
            metadata[ResourceConstant.DELETED_KEY] = True
            res_metadata = self.resource_repository.delete_resource(*resource_id)

        res_metadatas = self.resource_repository.delete_resources(permanent_delete=True)
        for res_metadata in res_metadatas:
            self.check_metadata_equal(res_metadata,metadatas)

        self.check_storage_empty()
            
    def test_delete_4_logical_delete_archive_resource(self):
        """
        Test delete  feature for logically delete supported archive resource
        """
        self.clean_resources()
        self.archive=True
        self.logical_delete=True

        repository = self.resource_repository
        logger.info("{}:Test delete features for logically delete supported archive resource".format(self.prefix))
        metadatas = self.prepare_test_datas()

        #logical delete resource using method delete_resource 
        rows = 0
        for resource_id,metadata in metadatas.items():
            #delete them
            res_metadata = self.resource_repository.delete_resource(*resource_id)
            metadata[ResourceConstant.DELETED_KEY] = True
            rows += 1
            if rows >= 2:
                break

        #check the reources after logical deleting
        self.check_resources(metadatas)

        #logical delete resources using method delete_resources with filter 
        filter_len = len(self.resource_repository.resource_keys) - 1
        while filter_len > 0:
            filter_id = None
            for key in metadatas.keys():
                filter_id = key[:filter_len]

            filter_params = self.get_filter_params(filter_id)

            current_metadatas = self.filter_testdatas(metadatas,**filter_params)

            res_metadatas = self.resource_repository.delete_resources(permanent_delete=False,**filter_params)
            for resource_id,metadata in current_metadatas.items():
                metadata[ResourceConstant.DELETED_KEY] = True

            for res_metadata in res_metadatas:
                resource_id = self.get_resource_id(res_metadata)
                self.check_metadata_equal(res_metadata,current_metadatas)

            filter_len -= 1


        #check the reources after logical deleting
        self.check_resources(metadatas)

        #logical delete all resources using method delete_resources without filter 
        res_metadatas = self.resource_repository.delete_resources(permanent_delete=False)
        for resource_id,metadata in metadatas.items():
            metadata[ResourceConstant.DELETED_KEY] = True


        #check the reources after logical deleting
        self.check_resources(metadatas)

        #republish the deleted resource 
        self.republish_resources(metadatas)

        #check the reources after logical deleting
        self.check_resources(metadatas)

        #delete the resources logically except the last one
        for resource_id,metadata in [(key,value) for key, value in metadatas.items()][:-1]:
            #delete them
            metadata[ResourceConstant.DELETED_KEY] = True
            metadata = self.resource_repository.delete_resource(*resource_id)
        
        #permanently delete_resource using method delete_resource
        for resource_id,metadata in metadatas.items():
            res_metadata = self.resource_repository.delete_resource(*resource_id,permanent_delete=True)
            self.check_metadata_equal(res_metadata,metadatas)

        self.check_storage_empty()

        #check permanently delete_resources
        metadatas = self.prepare_test_datas()

        #logically delete the resources except the last one
        for resource_id,metadata in [(key,value) for key, value in metadatas.items()][:-1]:
            #delete them
            metadata[ResourceConstant.DELETED_KEY] = True
            res_metadata = self.resource_repository.delete_resource(*resource_id)

        #permanently delete_resource using method delete_resources
        res_metadatas = self.resource_repository.delete_resources(permanent_delete=True)
        for res_metadata in res_metadatas:
            self.check_metadata_equal(res_metadata,metadatas)

        self.check_storage_empty()

    def test_archive(self):
        self.clean_resources()
        self.archive=True
        self.logical_delete=False

        repository = self.resource_repository
        logger.info("{}:Test archive resource".format(self.prefix))
        metadatas = self.prepare_test_datas()
        
        #check the current archive
        self.check_resources(metadatas)

        #check resource_metadatas
        self.check_delete_resources(metadatas)

        self.check_storage_empty()

    def test_push_resource(self):
        self.clean_resources()
        self.archive=False
        self.logical_delete=False

        repository = self.resource_repository
        logger.info("{}:Test push resource".format(self.prefix))
        #push a test content to blob storage
        metadatas = self.populate_test_datas()
        for resource_id,data in metadatas.items():
            metadata,content,content_json,content_byte = data
            #test push_resource
            repo_metadata = self.resource_repository.push_resource(content_byte,metadata)
            self.check_metadata_equal(self.get_metadata(repo_metadata,resource_id),metadatas)

        self.check_resources(metadatas)
        self.check_delete_resources(metadatas)
        self.check_storage_empty()

    def test_push_json(self):
        self.clean_resources()
        self.archive=False
        self.logical_delete=False

        repository = self.resource_repository
        logger.info("{}:Test push json".format(self.prefix))
        #push a test content to blob storage
        metadatas = self.populate_test_datas()
        for resource_id,data in metadatas.items():
            metadata,content,content_json,content_byte = data
            #test push_resource
            repo_metadata = self.resource_repository.push_json(content_json,metadata)
            self.check_metadata_equal(self.get_metadata(repo_metadata,resource_id),metadatas)

        self.check_resources(metadatas)
        self.check_delete_resources(metadatas)
        self.check_storage_empty()

    def test_push_file(self):
        self.clean_resources()
        self.archive=False
        self.logical_delete=False

        repository = self.resource_repository
        logger.info("{}:Test push file".format(self.prefix))
        #push a test content to blob storage
        metadatas = self.populate_test_datas()
        for resource_id,data in metadatas.items():
            metadata,content,content_json,content_byte = data
            #test push_resource
            with open("/tmp/test.json",'wb') as f:
                f.write(content_byte)
            repo_metadata = self.resource_repository.push_file("/tmp/test.json",metadata)
            self.check_metadata_equal(self.get_metadata(repo_metadata,resource_id),metadatas)


        self.check_resources(metadatas)
        self.check_delete_resources(metadatas)
        self.check_storage_empty()

class TestHistoryDataRepositoryMixin(BaseTesterMixin):
    f_earliest_id = None

    prop_f_earliest_id = "_f_earliest_resource_id"
    @property
    def resource_repository(self):
        """
        Return the resource repository to manage the resource
        """
        if not hasattr(self,"_resource_repository") or any(getattr(self._resource_repository,prop) != value for prop,value in [
            ("archive",self.archive),
            ("cache",self.cache),
            ("logical_delete",self.logical_delete),
            (self.prop_f_earliest_id,self.f_earliest_id)
        ]):
            self._resource_repository = self.create_resource_repository()
            self.prefix = "{}(archive={},logical_delete={}):".format(self.__class__.__name__,self.archive,self.logical_delete)
        return self._resource_repository

    def set_f_earliest_id(self,resource_id):
        """
        Set the f_earliest_id against resource id
        Return True if a new f_earliest_is is set; otherwise return False
        """
        return False

    def get_metadata(self,repo_metadata,resource_id):
        """
        Return the resource's metadata from the repository's metadata
        """
        for res_id,metadata in repo_metadata:
            if len(resource_id) == 1:
                if res_id == resource_id[0]:
                    return metadata
            elif isinstance(resource_id,tuple):
                if res_id == list(resource_id):
                    return metadata
            else:
                if res_id == resource_id:
                    return metadata

        return None

    def test_update(self):
        self.clean_resources()
        self.archive=False
        self.logical_delete=False
        self._f_earliest_id=None

        #try to publish an existed resource 
        testdatas = self.prepare_test_datas()
        for resource_id,data in testdatas.items():
            with self.assertRaises(exceptions.ResourceAlreadyExist,msg="Republishing an existing resource should throw ResourceAlreadyExist exception"):
                self.republish_resources(testdatas,resource_ids=resource_id)
            break

        #try to publish a resource with a smaller resource id 
        last_resource_id = self.resource_repository.last_resource_id
        if isinstance(last_resource_id,str):
            new_resource_id = [last_resource_id]
        else:
            new_resource_id = list(last_resource_id)
        new_resource_id[-1] = new_resource_id[-1][0:-1]
        metadata,content,content_json,content_byte = self.populate_test_data(new_resource_id)
        with self.assertRaises(exceptions.InvalidResource,msg="Publish a history data with smaller resource id should throw InvalidResource exception"):
            self.resource_repository.push_resource(content_byte,metadata)

        self.check_delete_resources(testdatas)
        self.check_storage_empty()

    def test_auto_clean(self):
        self.clean_resources()
        self.archive=False
        self.logical_delete=False
        self._f_earliest_id=None

        logger.info("{}:Test auto clean".format(self.prefix))
        #push a test content to blob storage
        metadatas = self.populate_test_datas()
        first_resource_id = None
        repo_first_resource_id = None
        for resource_id,data in metadatas.items():
            metadata,content,content_json,content_byte = data
            repository = self.resource_repository
            if self.set_f_earliest_id(resource_id):
                first_resource_id = resource_id
            elif not self._f_earliest_id and not first_resource_id:
                first_resource_id = resource_id

            #test push_resource
            repo_metadata = self.resource_repository.push_resource(content_byte,metadata)
            if len(self.resource_repository.resource_keys) == 1:
                for res_id,res_meta in self.resource_repository.metadata_client.resources_in_range(None,resource_id[0],max_resource_included=True):
                    repo_first_resource_id = (res_id,)
                    break
            else:
                for res_id,res_meta in self.resource_repository.metadata_client.resources_in_range(None,resource_id,max_resource_included=True):
                    repo_first_resource_id = tuple(res_id)
                    break
            self.assertEqual(repo_first_resource_id,first_resource_id,"{}The first resource id in repository is {}, but expect {}".format(self.prefix,repo_first_resource_id,first_resource_id))


    def test_push_resource(self):
        self.clean_resources()
        self.archive=False
        self.logical_delete=False
        self._f_earliest_id=None

        repository = self.resource_repository
        logger.info("{}:Test push resource".format(self.prefix))
        #push a test content to blob storage
        metadatas = self.populate_test_datas()
        for resource_id,data in metadatas.items():
            metadata,content,content_json,content_byte = data
            #test push_resource
            repo_metadata = self.resource_repository.push_resource(content_byte,metadata)
            self.assertEqual(
                self.resource_repository.last_resource_id,
                resource_id[0] if len(resource_id) == 1 else list(resource_id),
                "The last resource id({}) is not equal with the expected resource id({})".format(
                    self.resource_repository.last_resource_id,resource_id
                )
            )
            self.check_metadata_equal(self.get_metadata(repo_metadata,resource_id),metadatas)

        self.check_resources(metadatas)
        self.check_delete_resources(metadatas)
        self.check_storage_empty()

    def test_push_json(self):
        self.clean_resources()
        self.archive=False
        self.logical_delete=False
        self._f_earliest_id=None

        repository = self.resource_repository
        logger.info("{}:Test push json".format(self.prefix))
        #push a test content to blob storage
        metadatas = self.populate_test_datas()
        for resource_id,data in metadatas.items():
            metadata,content,content_json,content_byte = data
            #test push_resource
            repo_metadata = self.resource_repository.push_json(content_json,metadata)
            self.check_metadata_equal(self.get_metadata(repo_metadata,resource_id),metadatas)

        self.check_resources(metadatas)
        self.check_delete_resources(metadatas)
        self.check_storage_empty()

    def test_push_file(self):
        self.clean_resources()
        self.archive=False
        self.logical_delete=False
        self._f_earliest_id=None

        repository = self.resource_repository
        logger.info("{}:Test push file".format(self.prefix))
        #push a test content to blob storage
        metadatas = self.populate_test_datas()
        for resource_id,data in metadatas.items():
            metadata,content,content_json,content_byte = data
            #test push_resource
            with open("/tmp/test.json",'wb') as f:
                f.write(content_byte)
            repo_metadata = self.resource_repository.push_file("/tmp/test.json",metadata)
            self.check_metadata_equal(self.get_metadata(repo_metadata,resource_id),metadatas)


        self.check_resources(metadatas)
        self.check_delete_resources(metadatas)
        self.check_storage_empty()


class BaseClientTesterMixin(BaseTesterMixin):
    client_id = "testclinet_01"

    consume_parameters_test_cases = ((False,True,False),(False,True,True),(True,True,False),(True,False,False)) #negative test, stop_if_failed,batch
    @property
    def consume_client(self):
        """
        Return the client to consume resources from resource repository
        """
        if not hasattr(self,"_consume_client") or any(getattr(self._consume_client,prop) != value for prop,value in [
            ("clientid",self.client_id)
        ]):
            self._consume_client = ResourceConsumeClient(self.storage,self.resource_name,self.client_id,resource_base_path=self.resource_base_path)
        return self._consume_client

    @property
    def consume_clients(self):
        """
        Return the clients object for the resource repository
        """
        if not hasattr(self,"_consume_clients"):
            self._consume_clients = ResourceConsumeClients(self.storage,self.resource_name,resource_base_path=self.resource_base_path)
        return self._consume_clients

        
    def delete_all_clients(self):
        logger.info("Clean all existing consume clients")
        try:
            self.consume_clients.delete_clients()
        except exceptions.MetaMetadataMissing as ex:
            pass

    def delete_client(self):
        logger.info("{}Delete the consume client({})".format(self.prefix,self.client_id))
        try:
            self.consume_client.delete_clients(clientid=self.client_id)
        except exceptions.ResourceNotFound as ex:
            pass
        self.assertFalse(self.consume_client.is_client_exist(self.client_id),"{}The consume client({}) should be deleted, but it still exists".format(self.prefix,self.client_id))

    def get_init_consume_status(self,metadatas):
        """
        Return the initial resource consume status(ResourceConsumeClient.NEW)
        """
        logger.debug("Get initial resource consume status")
        consume_status = {}
        for resource_id,val in metadatas.items():
            consume_status[resource_id] = ResourceConsumeClient.NEW

        return consume_status

    def delete_resources(self,testdatas,consume_statuses,permanent_delete=False,resource_ids=None):
        """
        resource_id should a tuple(a single resource) ,or list of tuple(a list of resource),or None for all resources
        """
        logger.info("{} delete the resources({})".format("Physically" if permanent_delete else "Logically",resource_ids if resource_ids else [k for k in testdatas.keys()]))
        if resource_ids is None:
            for resource_id in testdatas.keys():
                self.delete_resources(testdatas,consume_statuses,permanent_delete=permanent_delete,resource_ids=resource_id)
        elif isinstance(resource_ids,list):
            for resource_id in resource_ids:
                self.delete_resources(testdatas,permanent_delete=permanent_delete,resource_ids=resource_id)
        else:
            self.assertIn(resource_ids,testdatas,"Can't find resource({}) in testdatas".format(resource_ids))
            self.resource_repository.delete_resource(*resource_ids,permanent_delete=permanent_delete)
            if not permanent_delete:
                if self.archive:
                    testdatas[resource_ids][ResourceConstant.DELETED_KEY] = True
                else:
                    testdatas[resource_ids][0][ResourceConstant.DELETED_KEY] = True

            if self.logical_delete and not permanent_delete:
                consume_statuses[resource_ids] = ResourceConsumeClient.LOGICALLY_DELETED
            else:
                consume_statuses[resource_ids] = ResourceConsumeClient.PHYSICALLY_DELETED
                
    def republish_resources(self,testdatas,consume_statuses,resource_ids=None):
        """
        publish all or selected resources. 
        testdatas: the resources to be published
        resource_id should a tuple(a single resource) ,or list of tuple(a list of resource),or None for all resources
        """
        super().republish_resources(testdatas,resource_ids=resource_ids)
        if resource_ids is None:
            for resource_id in testdatas.keys():
                if resource_id in consume_statuses and consume_statuses[resource_id] in (ResourceConsumeClient.UPDATED,ResourceConsumeClient.NOT_CHANGED):
                    consume_statuses[resource_id] = ResourceConsumeClient.UPDATED
                else:
                    consume_statuses[resource_id] = ResourceConsumeClient.NEW
        elif isinstance(resource_ids,list):
            for resource_id in resource_ids:
                if resource_id in consume_statuses and consume_statuses[resource_id] in (ResourceConsumeClient.UPDATED,ResourceConsumeClient.NOT_CHANGED):
                    consume_statuses[resource_id] = ResourceConsumeClient.UPDATED
                else:
                    consume_statuses[resource_id] = ResourceConsumeClient.NEW
        else:
            if resource_ids in consume_statuses and consume_statuses[resource_ids] in (ResourceConsumeClient.UPDATED,ResourceConsumeClient.NOT_CHANGED):
                consume_statuses[resource_ids] = ResourceConsumeClient.UPDATED
            else:
                consume_statuses[resource_ids] = ResourceConsumeClient.NEW

    def check_no_clients(self):
        logger.info("Check to guarantee that resource repository has no consume clients")
        client_metadatas = [m for m in self.consume_clients.get_client_metadatas()]
        self.assertEqual(len(client_metadatas),0,"The resource repository should have no clients, but found {} clients".format(len(client_metadatas)))

    def check_consume_result(self,consume_result,consumed_resources,failed_consumed_resources=[]):
        """
        Check  whether the consume result is matched with the expected consumed resources and failed consumed resources.
        """
        self.assertEqual(len(consume_result[1]),len(failed_consumed_resources),"{} {} resources are consumed failed, but only expect {}".format(
            self.prefix,len(consume_result[1]),len(failed_consumed_resources)
        ))
        index = 0
        while index < len(consume_result[1]):
            self.assertEqual(consume_result[1][index][2],failed_consumed_resources[index][1],"{}The {} resource({}) is not the expected resource({}) ".format(
                self.prefix,index,consume_result[1][index][2],failed_consumed_resources[index][1])
            )
            self.assertEqual(consume_result[1][index][0],failed_consumed_resources[index][0],"{}The status({}) of the {} resource({}) is not the expected status({}) ".format(
                self.prefix,consume_result[1][index][1],index,consume_result[1][index][2],self.consume_client.get_consume_status_name(failed_consumed_resources[index][0]))
            )
            index += 1

        self.assertEqual(len(consume_result[0]),len(consumed_resources),"{}{} resources are consumed successfully, but expect {} resources".format(
            self.prefix,len(consume_result[0]),len(consumed_resources))
        )
        index = 0
        while index < len(consume_result[0]):
            self.assertEqual(consume_result[0][index][2],consumed_resources[index][1],"{}The {} resource({}) is not the expected resource({}) ".format(
                self.prefix,index,consume_result[0][index][2],consumed_resources[index][1])
            )
            self.assertEqual(consume_result[0][index][0],consumed_resources[index][0],"{}The status({}) of the {} resource({}) is not the expected status({}) ".format(
                self.prefix,consume_result[0][index][1],index,consume_result[0][index][2],self.consume_client.get_consume_status_name(consumed_resources[index][0]))
            )
            index += 1

    def check_consumed_resource(self,consume_statuses,consumed_resources,expected_datas,is_sorted,failed):
        """
        Check whether the consumed resources(successfully consumed or faied consumed) is matched with the expected data and update the consume status to desired status if necessary
        """
        status_msg = "Failed" if failed else "Succeed"

        def _get_expected_data(index,resource_id):
            if is_sorted:
                expected_data = expected_datas[index]
            else:
                expected_data = None
                for data in expected_datas:
                    if data[1] == resource_id:
                        expected_data = data
                        break

            return expected_data

        self.assertEqual(len(consumed_resources),len(expected_datas),"{}{} to consume {} resources, but expect {} resources".format(self.prefix,status_msg,len(consumed_resources),len(expected_datas)))
        index = 0
        while index < len(consumed_resources):
            status,resource_id,res_metadata,res_content = consumed_resources[index]
            expected_data = _get_expected_data(index,resource_id)
            if is_sorted:
                self.assertEqual(resource_id,expected_data[1],"{}The {} {} consumed resource({}) is not equal with the expected resource({})".format(
                    self.prefix,index,status_msg,resource_id,expected_data[1])
                )
            else:
                self.assertIsNotNone(expected_data,"{}The {} {} consumed resource({}) is not expected".format(self.prefix,index,status_msg,resource_id))

            self.assertEqual(res_metadata,expected_data[2][0],"{}The {} {} consumed resource({})'s metadata({}) is not equal with the expected metadata({})".format(
                self.prefix,index,status_msg,resource_id,res_metadata,expected_data[2][0])
            )

            self.assertEqual(status,expected_data[0],"{}The {} {} consumed resource({})'s status({}) is not equal with the expected status({})".format(
                self.prefix,index,status_msg,resource_id,self.consume_client.get_consume_status_name(status),self.consume_client.get_consume_status_name(expected_data[0]))
            )

            if status == ResourceConsumeClient.PHYSICALLY_DELETED:
                self.assertIsNone(res_content,"{} The {} {} consumed resource({}) is physically deleted and its content should be none, but got the content '{}'".format(
                    self.prefix,index,status_msg,resource_id,res_content)
                )
            else:
                expected_content = expected_data[2][1]
                self.assertEqual(res_content,expected_content,"{} The {} {} consumed resource({})'s content({}) is not equal with the expected content({})".format(
                    self.prefix,index,status_msg,resource_id,res_content,expected_content)
                )
            if not failed:           
                if status in (ResourceConsumeClient.PHYSICALLY_DELETED,ResourceConsumeClient.LOGICALLY_DELETED):
                    del consume_statuses[resource_id]
                else:
                    consume_statuses[resource_id] = ResourceConsumeClient.NOT_CHANGED

            index += 1


    def get_expected_datas(self,testdatas,consume_statuses,resources=None,failed_resource_ids=[],stop_if_failed=True,batch=False,sortkey_func=None,reconsume=False,is_sorted=None):
        """
        Get the expected test datas in tuple (is_sorted,expected_datas,expected_failed_datas,expected_unconsumed_datas)
        the returned data can be the exact data expected or all  possible data expected
        """
        if resources and isinstance(resources,(tuple,list)):
            is_sorted = True
        elif sortkey_func:
            is_sorted = True
        else:
            is_sorted = True if is_sorted else False

        if batch and failed_resource_ids:
            return (is_sorted,[],[])

        expected_datas = []
        expected_failed_datas = []
        expected_unconsumed_datas = []
        if resources and isinstance(resources,(tuple,list)):
            for resource_id in resources:
                if resource_id not in testdatas:
                    continue
                val = testdatas[resource_id]["current"] if self.archive else testdatas[resource_id]
                if reconsume or consume_statuses[resource_id] != ResourceConsumeClient.NOT_CHANGED:
                    expected_datas.append((consume_statuses[resource_id],resource_id,val))

        else:
            for resource_id,val in testdatas.items():
                val = val["current"] if self.archive else val
                if resources and not resources(*resource_id):
                    continue
                if reconsume or consume_statuses[resource_id] != ResourceConsumeClient.NOT_CHANGED:
                    expected_datas.append((consume_statuses[resource_id],resource_id,val))

            if sortkey_func:
                expected_datas.sort(key=sortkey_func)
        if failed_resource_ids:
            if stop_if_failed:
                if is_sorted:
                    index = 0
                    failed_index = -1
                    while index < len(expected_datas):
                        if expected_datas[index][1] in failed_resource_ids:
                            failed_index = index
                            break
                        index += 1
                    if reconsume:
                        expected_unconsumed_datas = list(expected_datas)
                    if failed_index >= 0 :
                        if not reconsume:
                            expected_unconsumed_datas = expected_datas[failed_index:]
                        expected_failed_datas.append(expected_datas[failed_index])
                        expected_datas = expected_datas[0:failed_index]
                else:
                    #because resource is not sorted, so the datas returned are all possible resources.
                    expected_unconsumed_datas = list(expected_datas)
                    index = len(expected_datas) -1
                    while index >= 0:
                        if expected_datas[index][1] in failed_resource_ids:
                            expected_failed_datas.append(expected_datas[index])
                            del expected_datas[index]
                        index -= 1
            else:
                if reconsume:
                    expected_unconsumed_datas = list(expected_datas)
                index = len(expected_datas) - 1
                while index >= 0:
                    if expected_datas[index][1] in failed_resource_ids:
                        expected_failed_datas.insert(0,expected_datas[index])
                        del expected_datas[index]
                    index -= 1
                if not reconsume:
                    expected_unconsumed_datas = expected_failed_datas
        return (is_sorted,expected_datas,expected_failed_datas,expected_unconsumed_datas)

    def check_consume(self,negative_test,testdatas,consume_statuses,resources=None,reconsume=False,sortkey_func=None,stop_if_failed=True,batch=False,is_sorted=None):
        """
        Check the method "consume"
        """
        logger.debug("{}Check the method 'consume' in {} mode.resources={},reconsume={},sort={},stop_if_failed={},batch={}".format(self.prefix,"negative" if negative_test else "positive",resources,reconsume,is_sorted,stop_if_failed,batch))
        #got the failed resource ids
        if negative_test:
            if resources and isinstance(resources,(tuple,list)):
                failed_resource_ids = resources
                if len(resources) == 1:
                    failed_resource_ids = resources
                elif len(resources) == 2:
                    failed_resource_ids = resources[1:]
                else:
                    failed_resource_ids = resources[1:-1]
            else:
                failed_resource_ids = []
                for resource_id,val in testdatas.items():
                    if resources and not resources(*resource_id):
                        continue
                    failed_resource_ids.append(resource_id)
            if len(failed_resource_ids) == 1:
                failed_resource_ids = failed_resource_ids
            elif len(failed_resource_ids) == 2:
                failed_resource_ids = failed_resource_ids[1:]
            else:
                failed_resource_ids = failed_resource_ids[1:-1]
        else:
            failed_resource_ids = []

        consumed_resources = []
        failed_consumed_resources = []
        if batch:
            def _callback(resources):
                for status,res_metadata,res_file in resources: 
                    if res_file:
                        with open(res_file,'r') as f:
                            consumed_resources.append((status,self.get_resource_id(res_metadata),res_metadata,f.read()))
                    else:
                        consumed_resources.append((status,self.get_resource_id(res_metadata),res_metadata,None))
        else:
            def _callback(status,res_metadata,res_file):
                resource_id = self.get_resource_id(res_metadata)
                if resource_id in failed_resource_ids:
                    if res_file:
                        with open(res_file,'r') as f:
                            failed_consumed_resources.append((status,resource_id,res_metadata,f.read()))
                    else:
                        failed_consumed_resources.append((status,resource_id,res_metadata,None))
                    raise TestException("Failed")
                else:
                    if res_file:
                        with open(res_file,'r') as f:
                            consumed_resources.append((status,resource_id,res_metadata,f.read()))
                    else:
                        consumed_resources.append((status,resource_id,res_metadata,None))

        is_sorted,expected_datas,expected_failed_datas,expected_unconsumed_datas = self.get_expected_datas(testdatas,consume_statuses,
            resources=resources,
            failed_resource_ids=failed_resource_ids,
            stop_if_failed=stop_if_failed,
            batch=batch,
            sortkey_func=sortkey_func,
            reconsume=reconsume,
            is_sorted=is_sorted
        )
        if negative_test:
            if stop_if_failed:
                if is_sorted:
                    logger.debug("{}Check resource consumptions.expected_datas={},failed_resource_ids={},sorted={}".format(
                        self.prefix,
                        ["resource_id={0},status={1}".format(d[1],self.consume_client.get_consume_status_name(d[0])) for d in expected_datas],
                        failed_resource_ids,
                        is_sorted
                    ))
                else:
                    logger.debug("{}Check resource consumptions.failed_resource_ids={},sorted={}".format(
                        self.prefix,
                        failed_resource_ids,
                        is_sorted
                    ))

            else:
                logger.debug("{}Check resource consumptions.expected_datas={},expected_failed_datas={},failed_resource_ids={},sorted={}".format(
                    self.prefix,
                    ["resource_id={0},status={1}".format(d[1],self.consume_client.get_consume_status_name(d[0])) for d in expected_datas],
                    ["resource_id={0},status={1}".format(d[1],self.consume_client.get_consume_status_name(d[0])) for d in expected_failed_datas],
                    failed_resource_ids,
                    is_sorted
                ))
        else:
            logger.debug("{}Check resource consumptions.expected_datas={},sorted={}".format(
                self.prefix,
                ["resource_id={0},status={1}".format(d[1],self.consume_client.get_consume_status_name(d[0])) for d in expected_datas],
                is_sorted
            ))

        consume_result = self.consume(_callback,resources=resources,reconsume=reconsume,sortkey_func=sortkey_func,stop_if_failed=stop_if_failed)
        if expected_failed_datas:
            if stop_if_failed:
                self.assertEqual(len(consume_result[1]),1,"{}{} resources are consumed failed,but expect 1 in stop-if-failed mode".format(self.prefix,len(consume_result[1])))
            else:
                self.assertEqual(len(consume_result[1]),len(expected_failed_datas),"{}{} resources are consumed failed,but expect {} in non-stop-if-failed mode".format(self.prefix,len(consume_result[1]),len(expected_failed_datas)))

        self.check_consume_result(consume_result,consumed_resources,failed_consumed_resources)

        def _get_expected_datas(consumed_resources,expected_datas):
            if negative_test:
                if is_sorted:
                    return expected_datas
                else:
                    datas = []
                    for data in expected_datas:
                        for res in consumed_resources:
                            if res[1] == data[1]:
                                datas.append(data)
                                break

                    return datas
            else:
                return expected_datas


        self.check_consumed_resource(consume_statuses,consumed_resources,_get_expected_datas(consumed_resources,expected_datas),is_sorted,False)

        self.check_consumed_resource(consume_statuses,failed_consumed_resources,_get_expected_datas(failed_consumed_resources,expected_failed_datas),is_sorted,True)

        if negative_test and not reconsume and not is_sorted:
                index = len(expected_unconsumed_datas) - 1
                while index >= 0:
                    for res in consumed_resources:
                        if res[1] == expected_unconsumed_datas[index][1]:
                            del expected_unconsumed_datas[index]
                            break
                    index -= 1

        if expected_unconsumed_datas:
            #clean the failed resource ids, and consume again
            failed_resource_ids = []
            consumed_resources = []
            failed_consumed_resources = []

            logger.debug("Check the failed resource consumptions.expected_datas={},sorted={}".format(
                ["resource_id={0},status={1}".format(d[1],self.consume_client.get_consume_status_name(d[0])) for d in expected_unconsumed_datas],
                is_sorted
            ))

            consume_result = self.consume(_callback,resources=resources,reconsume=reconsume,sortkey_func=sortkey_func,stop_if_failed=stop_if_failed)
            self.check_consume_result(consume_result,consumed_resources,failed_consumed_resources)

            self.check_consumed_resource(consume_statuses,consumed_resources,expected_unconsumed_datas,is_sorted,False)
            self.check_consumed_resource(consume_statuses,failed_consumed_resources,[],is_sorted,True)


class TestResourceRepositoryClientMixin(BaseClientTesterMixin):
    def check_resouce_cosuming(self,resources=None,sortkey_func=None,is_sorted=None):
        """
        check resource cosuming feature
        """

        logger.info("{}Test resource consuming against the test datas,resources={},sort={}".format(self.prefix,resources,True if sortkey_func else False))
        for negative_test,stop_if_failed,batch in ((False,True,False),(False,True,True),(True,True,False),(True,False,False)):
            logger.debug("{}Prepare the testing data.negative test={},stop_if_failed={},batch={}".format(self.prefix,negative_test,stop_if_failed,batch))
            self.clean_resources()
            testdatas = self.prepare_test_datas()

            self.delete_client()
            consume_statuses = self.get_init_consume_status(testdatas)
            #consume all resources  without order
            logger.debug("{}Call the method 'consume', all published datas should be consumed .negative test={},stop_if_failed={},batch={}".format(self.prefix,negative_test,stop_if_failed,batch))
            self.assertTrue(self.consume_client.is_behind(resources=resources),"{}:some resources have been updated/created/deleted since last consuming,but can't find any resources".format(self.prefix))
            self.check_consume(negative_test,testdatas,consume_statuses,resources=resources,sortkey_func=sortkey_func,stop_if_failed=stop_if_failed,batch=batch)
    
            #consume all resource again, this time no resource should be consumed
            logger.debug("{}Call the method 'consume' again, no data should be consued this time.negative test={},stop_if_failed={},batch={}".format(self.prefix,negative_test,stop_if_failed,batch))
            self.assertFalse(self.consume_client.is_behind(resources=resources),"{}:no resource is updated/created/deleted since last consuming,but find some resources".format(self.prefix))
            self.check_consume(negative_test,testdatas,consume_statuses,resources=resources,sortkey_func=sortkey_func,stop_if_failed=stop_if_failed,batch=batch)
    
            #reconsume all resource again, 
            logger.debug("{}Call the method 'consume' to reconsume , all data should be consumed again.negative test={},stop_if_failed={},batch={}".format(self.prefix,negative_test,stop_if_failed,batch))
            self.assertFalse(self.consume_client.is_behind(resources=resources),"{}:no resource is updated/created/deleted since last consuming,but find some resources".format(self.prefix))
            self.check_consume(negative_test,testdatas,consume_statuses,resources=resources,sortkey_func=sortkey_func,stop_if_failed=stop_if_failed,batch=batch,reconsume=True)
    
            logger.debug("{}Republish some resources.negative test={},stop_if_failed={},batch={}".format(self.prefix,negative_test,stop_if_failed,batch))
            #republish one resource
            for resource_id in testdatas.keys():
                if resources and ((isinstance(resources,(list,tuple)) and  resource_id not in resources) or (callable(resources) and not resources(*resource_id))):
                    continue
                self.republish_resources(testdatas,consume_statuses,resource_ids=resource_id)
                break
    
            #consume all resource again, this time should conume a updated resource
            logger.debug("{}Call the method 'consume', republished datas should be consumed .negative test={},stop_if_failed={},batch={}".format(self.prefix,negative_test,stop_if_failed,batch))
            self.assertTrue(self.consume_client.is_behind(resources=resources),"{}:some resources have been updated/created/deleted since last consuming,but can't find anything".format(self.prefix))
            self.check_consume(negative_test,testdatas,consume_statuses,resources=resources,sortkey_func=sortkey_func,stop_if_failed=stop_if_failed,batch=batch)
            self.assertFalse(self.consume_client.is_behind(resources=resources),"{}:no resource is updated/created/deleted since last consuming,but find some resources".format(self.prefix))

            #publish some new resources
            logger.debug("{}Publish some resources.negative test={},stop_if_failed={},batch={}".format(self.prefix,negative_test,stop_if_failed,batch))
            testdatas2 = self.prepare_test_datas2()
            consume_statuses.update(self.get_init_consume_status(testdatas2))
            has_new_resource = False
            for resource_id,data in testdatas2.items():
                testdatas[resource_id] = data
                if resources:
                    if callable(resources):
                        if resources(*resource_id):
                            has_new_resource = True
                    elif resource_id in resources:
                        has_new_resource = True

                else:
                    has_new_resource = True

            if has_new_resource:
                #consume all resource again, this time should conume some new resources 
                logger.debug("{}Call the method 'consume', new published datas should be consumed .negative test={},stop_if_failed={},batch={}".format(self.prefix,negative_test,stop_if_failed,batch))
                self.assertTrue(self.consume_client.is_behind(resources=resources),"{}:some resources have been updated/created/deleted since last consuming,but can't find anything".format(self.prefix))
                self.check_consume(negative_test,testdatas,consume_statuses,resources=resources,sortkey_func=sortkey_func,stop_if_failed=stop_if_failed,batch=batch)
                self.assertFalse(self.consume_client.is_behind(resources=resources),"{}:no resource is updated/created/deleted since last consuming,but find some resources".format(self.prefix))
            else:
                #consume all resource again, this time should conume no resources because new resources is not in resources filter
                logger.debug("{}Call the method 'consume', no datas should be consumed .negative test={},stop_if_failed={},batch={}".format(self.prefix,negative_test,stop_if_failed,batch))
                self.assertFalse(self.consume_client.is_behind(resources=resources),"{}:no resources which is satisified by the resource filter have been updated/created/deleted since last consuming,but find anything".format(self.prefix))
                self.check_consume(negative_test,testdatas,consume_statuses,resources=resources,sortkey_func=sortkey_func,stop_if_failed=stop_if_failed,batch=batch)

    
            if self.logical_delete:
                #logically delete one resource
                logger.debug("{}Logically delete some resources .negative test={},stop_if_failed={},batch={}".format(self.prefix,negative_test,stop_if_failed,batch))
                deleted_resource_id = None
                for resource_id in testdatas.keys():
                    if resources and ((isinstance(resources,(list,tuple)) and  resource_id not in resources) or (callable(resources) and not resources(*resource_id))):
                        continue
                    self.delete_resources(testdatas,consume_statuses,permanent_delete=False,resource_ids=resource_id)
                    deleted_resource_id = resource_id
                    break
    
                #consume all resource again, this time should conume a logically deleted resource
                logger.debug("{}Call the method 'consume', logically deleted datas should be consumed .negative test={},stop_if_failed={},batch={}".format(self.prefix,negative_test,stop_if_failed,batch))
                self.assertTrue(self.consume_client.is_behind(resources=resources),"{}:some resources have been updated/created/deleted since last consuming,but can't find anything".format(self.prefix))
                self.check_consume(negative_test,testdatas,consume_statuses,resources=resources,sortkey_func=sortkey_func,stop_if_failed=stop_if_failed,batch=batch)
                self.assertFalse(self.consume_client.is_behind(resources=resources),"{}:no resource is updated/created/deleted since last consuming,but find some resources".format(self.prefix))
    
                #republish the logically deleted resource
                logger.debug("{}Republish the logically deleted resources .negative test={},stop_if_failed={},batch={}".format(self.prefix,negative_test,stop_if_failed,batch))
                self.republish_resources(testdatas,consume_statuses,resource_ids=deleted_resource_id)
    
                #consume all resource again, this time should conume an updated resource
                logger.debug("{}Call the method 'consume', republished datas should be consumed .negative test={},stop_if_failed={},batch={}".format(self.prefix,negative_test,stop_if_failed,batch))
                self.assertTrue(self.consume_client.is_behind(resources=resources),"{}:some resources have been updated/created/deleted since last consuming,but can't find anything".format(self.prefix))
                self.check_consume(negative_test,testdatas,consume_statuses,resources=resources,sortkey_func=sortkey_func,stop_if_failed=stop_if_failed,batch=batch)
                self.assertFalse(self.consume_client.is_behind(resources=resources),"{}:no resource is updated/created/deleted since last consuming,but find some resources".format(self.prefix))
    
            #permanently delete one resource
            logger.debug("{}Permanently delete some resources .negative test={},stop_if_failed={},batch={}".format(self.prefix,negative_test,stop_if_failed,batch))
            deleted_resource_id = None
            for resource_id in testdatas.keys():
                if resources and ((isinstance(resources,(list,tuple)) and  resource_id not in resources) or (callable(resources) and not resources(*resource_id))):
                    continue
                self.delete_resources(testdatas,consume_statuses,permanent_delete=True,resource_ids=resource_id)
                deleted_resource_id = resource_id
                break
    
            #consume all resource again, this time should conume a physically deleted resource
            logger.debug("{}Call the method 'consume', permanently deleted datas should be consumed .negative test={},stop_if_failed={},batch={}".format(self.prefix,negative_test,stop_if_failed,batch))
            self.assertTrue(self.consume_client.is_behind(resources=resources),"{}:some resources have been updated/created/deleted since last consuming,but can't find anything".format(self.prefix))
            self.check_consume(negative_test,testdatas,consume_statuses,resources=resources,sortkey_func=sortkey_func,stop_if_failed=stop_if_failed,batch=batch)
            self.assertFalse(self.consume_client.is_behind(resources=resources),"{}:no resource is updated/created/deleted since last consuming,but find some resources".format(self.prefix))
    
            #republish the physically deleted resource
            logger.debug("{}Republish the permanently deleted resources .negative test={},stop_if_failed={},batch={}".format(self.prefix,negative_test,stop_if_failed,batch))
            self.republish_resources(testdatas,consume_statuses,resource_ids=deleted_resource_id)
    
            #consume all resource again, this time should conume an updated resource
            logger.debug("{}Call the method 'consume', republished datas should be consumed .negative test={},stop_if_failed={},batch={}".format(self.prefix,negative_test,stop_if_failed,batch))
            self.assertTrue(self.consume_client.is_behind(resources=resources),"{}:some resources have been updated/created/deleted since last consuming,but can't find anything".format(self.prefix))
            self.check_consume(negative_test,testdatas,consume_statuses,resources=resources,sortkey_func=sortkey_func,stop_if_failed=stop_if_failed,batch=batch)
            self.assertFalse(self.consume_client.is_behind(resources=resources),"{}:no resource is updated/created/deleted since last consuming,but find some resources".format(self.prefix))

    def consume(self,callback,resources=None,reconsume=False,sortkey_func=None,stop_if_failed=True):
        return self.consume_client.consume(callback,resources=resources,reconsume=reconsume,sortkey_func=sortkey_func,stop_if_failed=stop_if_failed)

    def check(self):
        """
        Check method for different test cases
        """
        #clean the clients
        self.delete_all_clients()
        #check whether have no clients
        self.check_no_clients()

        #check the normal case
        print("======================================================")
        logger.info("{}Test normal use case ".format(self.prefix))
        self.check_resouce_cosuming()

        print("======================================================")
        logger.info("{}Test resource consuming with specified resource list".format(self.prefix))
        resource_ids = self.get_test_data_keys()
        del resource_ids[0]
        del resource_ids[-1]
        self.check_resouce_cosuming(resources=resource_ids)

        print("======================================================")
        logger.info("{}Test resource consuming with specified resource filter".format(self.prefix))
        resource_filter = lambda *resource_id: any((k in resource_id[-1]) for k in ("test3.txt","test5.txt","test1.txt"))
        self.check_resouce_cosuming(resources=resource_filter)

        def _sort_func(args):
            resource_id = args[1]
            if isinstance(resource_id,(tuple,list)):
                key =  resource_id[-1]
            else:
                key = resource_id
            return os.path.split(key)[1]

        print("======================================================")
        logger.info("{}Test sorted resource consuming ".format(self.prefix))
        self.check_resouce_cosuming(sortkey_func=_sort_func)


        print("======================================================")
        logger.info("{}Test sorted resource consuming with resource filter".format(self.prefix))
        resource_filter = lambda *resource_id: any((k in resource_id[-1]) for k in ("test3.txt","test5.txt","test1.txt","test6.txt"))
        self.check_resouce_cosuming(sortkey_func=_sort_func,resources=resource_filter)

        #clean the clients
        self.delete_all_clients()

        self.clean_resources()

    def test_no_archive(self):
        #prepare the data
        self.archive = False
        self.logical_delete = False
        self.check()

    def test_archive(self):
        #prepare the data
        self.archive = True
        self.logical_delete = False
        self.check()

    def test_logical_delete(self):
        #prepare the data
        self.archive = False
        self.logical_delete = True
        self.check()

    def test_logical_delete_with_archive(self):
        #prepare the data
        self.archive = True
        self.logical_delete = True
        self.check()

class TestHistoryDataRepositoryClientMixin(BaseClientTesterMixin):
    consume_parameters_test_cases = ((False,True,False),(True,True,False)) #negative test, stop_if_failed,batch

    def consume(self,callback,resources=None,reconsume=False,sortkey_func=None,stop_if_failed=True):
        return self.consume_client.consume(callback)

    def check_resouce_cosuming(self):
        """
        check resource cosuming feature
        """
        logger.info("{}Test history data consuming against the test datas".format(self.prefix))
        stop_if_failed = True
        batch=False
        for negative_test in (False,True):
            self.clean_resources()
            logger.debug("{}Prepare the testing data.negative test={}".format(self.prefix,negative_test))
            testdatas = self.prepare_test_datas()
            self.delete_client()

            consume_statuses = self.get_init_consume_status(testdatas)
            #consume all resources  without order
            logger.debug("{}Call the method 'consume', all published datas should be consumed .negative test={}".format(self.prefix,negative_test))
            self.assertTrue(self.consume_client.is_behind(),"{}:some resources have been updated/created/deleted since last consuming,but can't find any resources".format(self.prefix))
            self.check_consume(negative_test,testdatas,consume_statuses,stop_if_failed=stop_if_failed,batch=batch,is_sorted=True)
    
            #consume all resource again, this time no resource should be consumed
            logger.debug("{}Call the method 'consume' again, no data should be consued this time.negative test={}".format(self.prefix,negative_test))
            self.assertFalse(self.consume_client.is_behind(),"{}:no resource is updated/created/deleted since last consuming,but find some resources".format(self.prefix))
            self.check_consume(negative_test,testdatas,consume_statuses,stop_if_failed=stop_if_failed,batch=batch,is_sorted=True)
    
            #publish some new resources
            logger.debug("{}Publish some new test datas.negative test={}".format(self.prefix,negative_test))
            testdatas2 = self.prepare_test_datas2()
            consume_statuses.update(self.get_init_consume_status(testdatas2))
            for resource_id,data in testdatas2.items():
                testdatas[resource_id] = data

            #consume all resource again, this time should conume some new resources
            logger.debug("{}Call the method 'consume', all published datas should be consumed .negative test={}".format(self.prefix,negative_test))
            self.assertTrue(self.consume_client.is_behind(),"{}:some resources have been updated/created/deleted since last consuming,but can't find anything".format(self.prefix))
            self.check_consume(negative_test,testdatas,consume_statuses,stop_if_failed=stop_if_failed,batch=batch,is_sorted=True)
            self.assertFalse(self.consume_client.is_behind(),"{}:no resource is updated/created/deleted since last consuming,but find some resources".format(self.prefix))
    
            #consume all resource again, this time should conume a updated resource
            logger.debug("{}Call the method 'consume' again, no data should be consued this time.negative test={}".format(self.prefix,negative_test))
            self.assertFalse(self.consume_client.is_behind(),"{}:some resources have been updated/created/deleted since last consuming,but can't find anything".format(self.prefix))
            self.check_consume(negative_test,testdatas,consume_statuses,stop_if_failed=stop_if_failed,batch=batch,is_sorted=True)
    
    def test_consume(self):
        #prepare the data
        self.archive = False
        self.logical_delete = False

        #clean the clients
        self.delete_all_clients()
        #check whether have no clients
        self.check_no_clients()

        #check the normal case
        print("======================================================")
        logger.info("{}Test normal use case ".format(self.prefix))
        self.check_resouce_cosuming()

        #clean the clients
        self.delete_all_clients()

        self.clean_resources()

