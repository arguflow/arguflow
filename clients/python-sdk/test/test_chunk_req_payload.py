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

from trieve_py_client.models.chunk_req_payload import ChunkReqPayload

class TestChunkReqPayload(unittest.TestCase):
    """ChunkReqPayload unit test stubs"""

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def make_instance(self, include_optional) -> ChunkReqPayload:
        """Test ChunkReqPayload
            include_option is a boolean, when False only required
            params are included, when True both required and
            optional params are included """
        # uncomment below to create an instance of `ChunkReqPayload`
        """
        model = ChunkReqPayload()
        if include_optional:
            return ChunkReqPayload(
                chunk_html = '',
                convert_html_to_text = True,
                fulltext_boost = trieve_py_client.models.full_text_boost.FullTextBoost(
                    boost_factor = 1.337, 
                    phrase = '', ),
                group_ids = [
                    ''
                    ],
                group_tracking_ids = [
                    ''
                    ],
                image_urls = [
                    ''
                    ],
                link = '',
                location = trieve_py_client.models.geo_info.GeoInfo(
                    lat = null, 
                    lon = null, ),
                metadata = None,
                num_value = 1.337,
                semantic_boost = trieve_py_client.models.semantic_boost.SemanticBoost(
                    distance_factor = 1.337, 
                    phrase = '', ),
                semantic_content = '',
                split_avg = True,
                tag_set = [
                    ''
                    ],
                time_stamp = '',
                tracking_id = '',
                upsert_by_tracking_id = True,
                weight = 1.337
            )
        else:
            return ChunkReqPayload(
        )
        """

    def testChunkReqPayload(self):
        """Test ChunkReqPayload"""
        # inst_req_only = self.make_instance(include_optional=False)
        # inst_req_and_optional = self.make_instance(include_optional=True)

if __name__ == '__main__':
    unittest.main()
