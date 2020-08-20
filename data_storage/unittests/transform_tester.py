import unittest
import json
import os
import time
import logging
from collections import OrderedDict

from data_storage import LocalStorage,get_resource_repository,HistoryDataRepository,GroupHistoryDataRepository,ResourceConstant,IndexedHistoryDataRepository,IndexedGroupHistoryDataRepository
from data_storage.utils import timezone,JSONEncoder,remove_file,remove_folder
from data_storage import exceptions
from data_storage import transform

from . import settings
from .basetester import BaseTesterMixin,TestHistoryDataRepositoryMixin

logger = logging.getLogger(__name__)

class TestIndexedGroupHistoryDataRepository(BaseTesterMixin,unittest.TestCase):
    storage = LocalStorage(settings.LOCAL_STORAGE_ROOT_FOLDER)
    resource_base_path = "indexedgrouphistorydatarepository"
    prop_f_earliest_id = "_f_earliest_metaname"

    def create_resource_repository(self):
        return IndexedGroupHistoryDataRepository(
            self.storage,
            self.resource_name,
            "lambda resource_group:resource_group[0:4]",
            resource_base_path=self.resource_base_path,
            cache=self.cache,
            f_earliest_metaname=None
        )

    def get_test_data_keys(self):
        return [
            ("2018_01","2018_01_10_test1.txt",),
            ("2018_01","2018_01_20_test2.txt",),

            ("2018_02","2018_02_10_test3.txt",),
            ("2018_02","2018_02_20_test4.txt",),

            ("2019_01","2019_01_10_test5.txt",),
            ("2019_01","2019_01_20_test6.txt",),

            ("2019_02","2019_02_10_test7.txt",),
            ("2019_02","2019_02_20_test8.txt",),

            ("2020_01","2020_01_10_test9.txt",),
            ("2020_01","2020_01_20_test10.txt",),

            ("2020_02","2020_02_10_test11.txt",),
            ("2020_02","2020_02_20_test12.txt",)
        ]

    def test_change_metaindex(self):
        self.clean_resources()
        self.archive=False
        self.logical_delete=False

        repository = self.resource_repository
        logger.info("{}:Test change_metaindex for indexed resource repository".format(self.prefix))
        metadatas = self.prepare_test_datas()
        
        res_metadatas = [res_metadata for res_metadata in repository.metadata_client.resource_metadatas(throw_exception=False,resource_status=ResourceConstant.ALL_RESOURCE,resource_file=None)]
        new_repository_metadata = transform.change_metaindex(repository.metadata_client,"lambda resource_group:resource_group")
        new_res_metadatas = [res_metadata for res_metadata in new_repository_metadata.resource_metadatas(throw_exception=False,resource_status=ResourceConstant.ALL_RESOURCE,resource_file=None)]
        
        self.assertEqual(res_metadatas,new_res_metadatas,"{}The migrated metadatas({}) is not equal with original metadatas({})".format(self.prefix,res_metadatas,new_res_metadatas))
        


if __name__ == '__main__':
    unittest.main()
