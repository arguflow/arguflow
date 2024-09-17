# coding: utf-8

"""
    Trieve API

    Trieve OpenAPI Specification. This document describes all of the operations available through the Trieve API.

    The version of the OpenAPI document: 0.11.8
    Contact: developers@trieve.ai
    Generated by OpenAPI Generator (https://openapi-generator.tech)

    Do not edit the class manually.
"""  # noqa: E501


import unittest

from trieve_py_client.models.upload_file_result import UploadFileResult

class TestUploadFileResult(unittest.TestCase):
    """UploadFileResult unit test stubs"""

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def make_instance(self, include_optional) -> UploadFileResult:
        """Test UploadFileResult
            include_option is a boolean, when False only required
            params are included, when True both required and
            optional params are included """
        # uncomment below to create an instance of `UploadFileResult`
        """
        model = UploadFileResult()
        if include_optional:
            return UploadFileResult(
                file_metadata = {"created_at":"2021-01-01 00:00:00.000","dataset_id":"e3e3e3e3-e3e3-e3e3-e3e3-e3e3e3e3e3e3","file_name":"file.txt","id":"e3e3e3e3-e3e3-e3e3-e3e3-e3e3e3e3e3e3","link":"https://trieve.ai","metadata":{"key":"value"},"size":1000,"tag_set":"tag1,tag2","time_stamp":"2021-01-01 00:00:00.000","updated_at":"2021-01-01 00:00:00.000"}
            )
        else:
            return UploadFileResult(
                file_metadata = {"created_at":"2021-01-01 00:00:00.000","dataset_id":"e3e3e3e3-e3e3-e3e3-e3e3-e3e3e3e3e3e3","file_name":"file.txt","id":"e3e3e3e3-e3e3-e3e3-e3e3-e3e3e3e3e3e3","link":"https://trieve.ai","metadata":{"key":"value"},"size":1000,"tag_set":"tag1,tag2","time_stamp":"2021-01-01 00:00:00.000","updated_at":"2021-01-01 00:00:00.000"},
        )
        """

    def testUploadFileResult(self):
        """Test UploadFileResult"""
        # inst_req_only = self.make_instance(include_optional=False)
        # inst_req_and_optional = self.make_instance(include_optional=True)

if __name__ == '__main__':
    unittest.main()
