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

from trieve_py_client.models.chunk_group_and_file_id import ChunkGroupAndFileId

class TestChunkGroupAndFileId(unittest.TestCase):
    """ChunkGroupAndFileId unit test stubs"""

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def make_instance(self, include_optional) -> ChunkGroupAndFileId:
        """Test ChunkGroupAndFileId
            include_option is a boolean, when False only required
            params are included, when True both required and
            optional params are included """
        # uncomment below to create an instance of `ChunkGroupAndFileId`
        """
        model = ChunkGroupAndFileId()
        if include_optional:
            return ChunkGroupAndFileId(
                created_at = datetime.datetime.strptime('2013-10-20 19:20:30.00', '%Y-%m-%d %H:%M:%S.%f'),
                dataset_id = '',
                description = '',
                file_id = '',
                id = '',
                metadata = None,
                name = '',
                tag_set = [
                    ''
                    ],
                tracking_id = '',
                updated_at = datetime.datetime.strptime('2013-10-20 19:20:30.00', '%Y-%m-%d %H:%M:%S.%f')
            )
        else:
            return ChunkGroupAndFileId(
                created_at = datetime.datetime.strptime('2013-10-20 19:20:30.00', '%Y-%m-%d %H:%M:%S.%f'),
                dataset_id = '',
                description = '',
                id = '',
                name = '',
                updated_at = datetime.datetime.strptime('2013-10-20 19:20:30.00', '%Y-%m-%d %H:%M:%S.%f'),
        )
        """

    def testChunkGroupAndFileId(self):
        """Test ChunkGroupAndFileId"""
        # inst_req_only = self.make_instance(include_optional=False)
        # inst_req_and_optional = self.make_instance(include_optional=True)

if __name__ == '__main__':
    unittest.main()
