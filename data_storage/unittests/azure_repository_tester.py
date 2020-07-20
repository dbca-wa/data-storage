import unittest
import json
import os
import time
import logging

from data_storage import AzureBlobStorage,get_resource_repository,ResourceRepository,GroupResourceRepository,ResourceConstant,IndexedResourceRepository,IndexedGroupResourceRepository
from data_storage.utils import timezone,JSONEncoder,remove_file,remove_folder
from data_storage import exceptions

from . import settings
from .basetester import BaseTesterMixin,TestResourceRepositoryMixin

logger = logging.getLogger(__name__)

class TestResourceRepository(TestResourceRepositoryMixin,unittest.TestCase):
    storage = AzureBlobStorage(settings.AZURE_CONNECTION_STRING,settings.AZURE_CONTAINER)
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

    def populate_test_datas(self):
        resource_ids = [
            ("/test/2018_05_02_test2.txt",),
            ("test2/2019_06_02_test4.txt",),
            ("2018_05_01_test1.txt",),
            ("test/2020_07_01_test5.txt",),
            ("test/2019_06_01_test3.txt",),
            ("test2/2020_07_02_test6.txt",)
        ]
        testdatas = {}
        for resource_id in resource_ids:
            testdatas[resource_id] = self.populate_test_data(resource_id)

        return testdatas

class TestIndexedResourceRepository(TestResourceRepository):
    resource_base_path = "indexedresourcerepository"

    def create_resource_repository(self):
        return IndexedResourceRepository(
            self.storage,
            self.resource_name,
            lambda resource_id:os.path.split(resource_id)[1][0:4],
            resource_base_path=self.resource_base_path,
            archive=self.archive,
            cache=self.cache,
            logical_delete=self.logical_delete
        )

    def populate_test_datas(self):
        resource_ids = [
            ("2018_05_01_test1.txt",),
            ("/test/2018_05_02_test2.txt",),
            ("test/2019_06_01_test3.txt",),
            ("test2/2019_06_02_test4.txt",),
            ("test/2020_07_01_test5.txt",),
            ("test2/2020_07_02_test6.txt",)
        ]
        testdatas = {}
        for resource_id in resource_ids:
            testdatas[resource_id] = self.populate_test_data(resource_id)

        return testdatas


class TestGroupResourceRepository(TestResourceRepositoryMixin,unittest.TestCase):
    storage = AzureBlobStorage(settings.AZURE_CONNECTION_STRING,settings.AZURE_CONTAINER)
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

    def populate_test_datas(self):
        resource_ids = [
            ("2019_06","2019_06_01_test3.txt"),
            ("2019_06","2019_06_02_test4.txt"),
            ("2018_05","2018_05_01_test1.txt"),
            ("2018_05","2018_05_02_test2.txt"),
            ("2020_07","2020_07_01_test5.txt"),
            ("2020_07","2020_07_02_test6.txt")
        ]
        testdatas = {}
        for resource_id in resource_ids:
            testdatas[resource_id] = self.populate_test_data(resource_id)

        return testdatas

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
