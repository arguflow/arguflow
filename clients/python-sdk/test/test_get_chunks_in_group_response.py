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

from trieve_py_client.models.get_chunks_in_group_response import GetChunksInGroupResponse

class TestGetChunksInGroupResponse(unittest.TestCase):
    """GetChunksInGroupResponse unit test stubs"""

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def make_instance(self, include_optional) -> GetChunksInGroupResponse:
        """Test GetChunksInGroupResponse
            include_option is a boolean, when False only required
            params are included, when True both required and
            optional params are included """
        # uncomment below to create an instance of `GetChunksInGroupResponse`
        """
        model = GetChunksInGroupResponse()
        if include_optional:
            return GetChunksInGroupResponse(
                chunks = [
                    {"created_at":"2021-01-01 00:00:00.000","dataset_id":"e3e3e3e3-e3e3-e3e3-e3e3-e3e3e3e3e3e3","id":"e3e3e3e3-e3e3-e3e3-e3e3-e3e3e3e3e3e3","link":"https://trieve.ai","metadata":{"key":"value"},"tag_set":"tag1,tag2","time_stamp":"2021-01-01 00:00:00.000","tracking_id":"e3e3e3e3-e3e3-e3e3-e3e3-e3e3e3e3e3e3","updated_at":"2021-01-01 00:00:00.000","weight":0.5}
                    ],
                group = {"created_at":"2021-01-01 00:00:00.000","dataset_id":"e3e3e3e3-e3e3-e3e3-e3e3-e3e3e3e3e3e3","description":"A group of chunks","file_id":"e3e3e3e3-e3e3-e3e3-e3e3-e3e3e3e3e3e3","id":"e3e3e3e3-e3e3-e3e3-e3e3-e3e3e3e3e3e3","name":"Trieve","tracking_id":"3","updated_at":"2021-01-01 00:00:00.000"},
                total_pages = 0
            )
        else:
            return GetChunksInGroupResponse(
                chunks = [
                    {"created_at":"2021-01-01 00:00:00.000","dataset_id":"e3e3e3e3-e3e3-e3e3-e3e3-e3e3e3e3e3e3","id":"e3e3e3e3-e3e3-e3e3-e3e3-e3e3e3e3e3e3","link":"https://trieve.ai","metadata":{"key":"value"},"tag_set":"tag1,tag2","time_stamp":"2021-01-01 00:00:00.000","tracking_id":"e3e3e3e3-e3e3-e3e3-e3e3-e3e3e3e3e3e3","updated_at":"2021-01-01 00:00:00.000","weight":0.5}
                    ],
                group = {"created_at":"2021-01-01 00:00:00.000","dataset_id":"e3e3e3e3-e3e3-e3e3-e3e3-e3e3e3e3e3e3","description":"A group of chunks","file_id":"e3e3e3e3-e3e3-e3e3-e3e3-e3e3e3e3e3e3","id":"e3e3e3e3-e3e3-e3e3-e3e3-e3e3e3e3e3e3","name":"Trieve","tracking_id":"3","updated_at":"2021-01-01 00:00:00.000"},
                total_pages = 0,
        )
        """

    def testGetChunksInGroupResponse(self):
        """Test GetChunksInGroupResponse"""
        # inst_req_only = self.make_instance(include_optional=False)
        # inst_req_and_optional = self.make_instance(include_optional=True)

if __name__ == '__main__':
    unittest.main()
