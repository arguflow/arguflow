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

from trieve_py_client.models.deprecated_search_over_groups_response_body import DeprecatedSearchOverGroupsResponseBody

class TestDeprecatedSearchOverGroupsResponseBody(unittest.TestCase):
    """DeprecatedSearchOverGroupsResponseBody unit test stubs"""

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def make_instance(self, include_optional) -> DeprecatedSearchOverGroupsResponseBody:
        """Test DeprecatedSearchOverGroupsResponseBody
            include_option is a boolean, when False only required
            params are included, when True both required and
            optional params are included """
        # uncomment below to create an instance of `DeprecatedSearchOverGroupsResponseBody`
        """
        model = DeprecatedSearchOverGroupsResponseBody()
        if include_optional:
            return DeprecatedSearchOverGroupsResponseBody(
                corrected_query = '',
                group_chunks = [
                    trieve_py_client.models.v1.V1(
                        file_id = '', 
                        group_created_at = datetime.datetime.strptime('2013-10-20 19:20:30.00', '%Y-%m-%d %H:%M:%S.%f'), 
                        group_dataset_id = '', 
                        group_description = '', 
                        group_id = '', 
                        group_metadata = null, 
                        group_name = '', 
                        group_tag_set = [
                            ''
                            ], 
                        group_tracking_id = '', 
                        group_updated_at = datetime.datetime.strptime('2013-10-20 19:20:30.00', '%Y-%m-%d %H:%M:%S.%f'), 
                        metadata = [
                            {"highlights":["highlight is two tokens: high, light","whereas hello is only one token: hello"],"metadata":[{"chunk_html":"<p>Some HTML content</p>","content":"Some content","id":"d290f1ee-6c54-4b01-90e6-d701748f0851","link":"https://example.com","metadata":{"key1":"value1","key2":"value2"},"time_stamp":"2021-01-01 00:00:00.000","weight":0.5}],"score":0.5}
                            ], )
                    ],
                total_chunk_pages = 56
            )
        else:
            return DeprecatedSearchOverGroupsResponseBody(
                group_chunks = [
                    trieve_py_client.models.v1.V1(
                        file_id = '', 
                        group_created_at = datetime.datetime.strptime('2013-10-20 19:20:30.00', '%Y-%m-%d %H:%M:%S.%f'), 
                        group_dataset_id = '', 
                        group_description = '', 
                        group_id = '', 
                        group_metadata = null, 
                        group_name = '', 
                        group_tag_set = [
                            ''
                            ], 
                        group_tracking_id = '', 
                        group_updated_at = datetime.datetime.strptime('2013-10-20 19:20:30.00', '%Y-%m-%d %H:%M:%S.%f'), 
                        metadata = [
                            {"highlights":["highlight is two tokens: high, light","whereas hello is only one token: hello"],"metadata":[{"chunk_html":"<p>Some HTML content</p>","content":"Some content","id":"d290f1ee-6c54-4b01-90e6-d701748f0851","link":"https://example.com","metadata":{"key1":"value1","key2":"value2"},"time_stamp":"2021-01-01 00:00:00.000","weight":0.5}],"score":0.5}
                            ], )
                    ],
                total_chunk_pages = 56,
        )
        """

    def testDeprecatedSearchOverGroupsResponseBody(self):
        """Test DeprecatedSearchOverGroupsResponseBody"""
        # inst_req_only = self.make_instance(include_optional=False)
        # inst_req_and_optional = self.make_instance(include_optional=True)

if __name__ == '__main__':
    unittest.main()
