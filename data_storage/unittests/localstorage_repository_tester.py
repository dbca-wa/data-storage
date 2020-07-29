import unittest
import json
import os
import time
import logging

from data_storage import LocalStorage,get_resource_repository,ResourceRepository,GroupResourceRepository,ResourceConstant,IndexedResourceRepository,IndexedGroupResourceRepository
from data_storage.utils import timezone,JSONEncoder,remove_file,remove_folder
from data_storage import exceptions

from . import settings
from .basetester import BaseTesterMixin,TestResourceRepositoryMixin

logger = logging.getLogger(__name__)

class TestResourceRepository(TestResourceRepositoryMixin,unittest.TestCase):
    storage = LocalStorage(settings.LOCAL_STORAGE_ROOT_FOLDER)
    resource_base_path = "resourcerepository"

    def create_resource_repository(self):
        return ResourceRepository(
            self.storage,
            self.resource_name,
            resource_base_path=self.resource_base_path,
            archive=self.archive,
            metaname="metadata",
            cache=self.cache,
            logical_delete=self.logical_delete
        )

    def get_test_data_keys(self):
        return [
            ("/test/2018_05_02_test2.txt",),
            ("test2/2019_06_02_test4.txt",),
            ("2018_05_01_test1.txt",),
            ("test/2020_07_01_test5.txt",),
            ("test/2019_06_01_test3.txt",),
            ("test2/2020_07_02_test6.txt",)
        ]

class TestIndexedResourceRepository(TestResourceRepository):
    resource_base_path = "indexedresourcerepository"

    def create_resource_repository(self):
        return IndexedResourceRepository(
            self.storage,
            self.resource_name,
            lambda resource_group:resource_group[0:4],
            resource_base_path=self.resource_base_path,
            archive=self.archive,
            cache=self.cache,
            logical_delete=self.logical_delete
        )

class TestGroupResourceRepository(TestResourceRepositoryMixin,unittest.TestCase):
    storage = LocalStorage(settings.LOCAL_STORAGE_ROOT_FOLDER)
    resource_base_path = "groupresourcerepository"

    def create_resource_repository(self):
        return GroupResourceRepository(
            self.storage,
            self.resource_name,
            resource_base_path=self.resource_base_path,
            archive=self.archive,
            metaname="metadata",
            cache=self.cache,
            logical_delete=self.logical_delete
        )

    def get_test_data_keys(self):
        return [
            ("2019_06","2019_06_01_test3.txt"),
            ("2019_06","2019_06_02_test4.txt"),
            ("2018_05","2018_05_01_test1.txt"),
            ("2018_05","2018_05_02_test2.txt"),
            ("2020_07","2020_07_01_test5.txt"),
            ("2020_07","2020_07_02_test6.txt")
        ]

class TestIndexedGroupResourceRepository(TestGroupResourceRepository):
    resource_base_path = "indexedgroupresourcerepository"

    def create_resource_repository(self):
        return IndexedGroupResourceRepository(
            self.storage,
            self.resource_name,
            lambda resource_group:resource_group[0:4],
            resource_base_path=self.resource_base_path,
            archive=self.archive,
            cache=self.cache,
            logical_delete=self.logical_delete
        )

if __name__ == '__main__':
    unittest.main()
