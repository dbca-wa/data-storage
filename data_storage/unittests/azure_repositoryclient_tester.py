import unittest
import os
import logging

from data_storage import (AzureBlobStorage,ResourceRepository,GroupResourceRepository,ResourceConstant,IndexedResourceRepository,IndexedGroupResourceRepository)

from . import settings
from .basetester import TestResourceRepositoryClientMixin

logger = logging.getLogger(__name__)

class TestResourceRepositoryClient(TestResourceRepositoryClientMixin,unittest.TestCase):
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

            

if __name__ == '__main__':
    unittest.main()
