import unittest
import os
import time
import logging

from data_storage import (AzureBlobStorage,ResourceRepository,exceptions)

from . import settings
from .basetester import TestRepositoryLockMixin

logger = logging.getLogger(__name__)

class TestResourceRepositoryClient(TestRepositoryLockMixin,unittest.TestCase):
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

    def get_test_data_keys(self):
        return [
            ("/test/2018_05_02_test2.txt",),
            ("test2/2019_06_02_test4.txt",),
            ("2018_05_01_test1.txt",),
            ("test/2020_07_01_test5.txt",),
            ("test/2019_06_01_test3.txt",),
            ("test2/2020_07_02_test6.txt",)
        ]


if __name__ == '__main__':
    unittest.main()
            
