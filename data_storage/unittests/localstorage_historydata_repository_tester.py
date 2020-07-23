import unittest
import json
import os
import time
import logging
from collections import OrderedDict

from data_storage import LocalStorage,get_resource_repository,HistoryDataRepository,GroupHistoryDataRepository,ResourceConstant,IndexedHistoryDataRepository,IndexedGroupHistoryDataRepository
from data_storage.utils import timezone,JSONEncoder,remove_file,remove_folder
from data_storage import exceptions

from . import settings
from .basetester import BaseTesterMixin,TestHistoryDataRepositoryMixin

logger = logging.getLogger(__name__)

class TestHistoryDataRepository(TestHistoryDataRepositoryMixin,unittest.TestCase):
    storage = LocalStorage(settings.LOCAL_STORAGE_ROOT_FOLDER)
    resource_base_path = "historydatapository"

    def create_resource_repository(self):
        return HistoryDataRepository(
            self.storage,
            self.resource_name,
            resource_base_path=self.resource_base_path,
            metaname="metadata",
            cache=self.cache
        )

    def get_test_data_keys(self):
        return [
            ("2018_01_10_test1.txt",),
            ("2018_01_20_test2.txt",),
            ("2019_01_10_test3.txt",),
            ("2019_01_20_test4.txt",),
            ("2020_01_10_test5.txt",),
            ("2020_01_20_test6.txt",)
        ]

class TestIndexedHistoryDataRepository(TestHistoryDataRepository):
    resource_base_path = "indexedhistorydatarepository"

    def create_resource_repository(self):
        return IndexedHistoryDataRepository(
            self.storage,
            self.resource_name,
            lambda resource_id:os.path.split(resource_id)[1][0:4],
            resource_base_path=self.resource_base_path,
            cache=self.cache
        )

class TestGroupHistoryDataRepository(TestHistoryDataRepositoryMixin,unittest.TestCase):
    storage = LocalStorage(settings.LOCAL_STORAGE_ROOT_FOLDER)
    resource_base_path = "grouphistorydatarepository"

    def create_resource_repository(self):
        return GroupHistoryDataRepository(
            self.storage,
            self.resource_name,
            resource_base_path=self.resource_base_path,
            metaname="metadata",
            cache=self.cache
        )

    def get_test_data_keys(self):
        return [
            ("2018_01","2018_01_10_test1.txt",),
            ("2018_01","2018_01_20_test2.txt",),
            ("2019_01","2019_01_10_test3.txt",),
            ("2019_01","2019_01_20_test4.txt",),
            ("2020_01","2020_01_10_test5.txt",),
            ("2020_01","2020_01_20_test6.txt",)
        ]

class TestIndexedGroupHistoryDataRepository(TestGroupHistoryDataRepository):
    resource_base_path = "indexedgrouphistorydatarepository"

    def create_resource_repository(self):
        return IndexedGroupHistoryDataRepository(
            self.storage,
            self.resource_name,
            lambda resource_group:resource_group[0:4],
            resource_base_path=self.resource_base_path,
            cache=self.cache
        )

if __name__ == '__main__':
    unittest.main()
