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

from trieve_py_client.models.search_queries_with_clicks_ctr_response import SearchQueriesWithClicksCTRResponse

class TestSearchQueriesWithClicksCTRResponse(unittest.TestCase):
    """SearchQueriesWithClicksCTRResponse unit test stubs"""

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def make_instance(self, include_optional) -> SearchQueriesWithClicksCTRResponse:
        """Test SearchQueriesWithClicksCTRResponse
            include_option is a boolean, when False only required
            params are included, when True both required and
            optional params are included """
        # uncomment below to create an instance of `SearchQueriesWithClicksCTRResponse`
        """
        model = SearchQueriesWithClicksCTRResponse()
        if include_optional:
            return SearchQueriesWithClicksCTRResponse(
                clicked_chunks = [
                    {"chunk_html":"<p>Hello, world!</p>","created_at":"2021-01-01 00:00:00.000","dataset_id":"e3e3e3e3-e3e3-e3e3-e3e3-e3e3e3e3e3e3","id":"e3e3e3e3-e3e3-e3e3-e3e3-e3e3e3e3e3e3","link":"https://trieve.ai","metadata":{"key":"value"},"tag_set":"[tag1,tag2]","time_stamp":"2021-01-01 00:00:00.000","tracking_id":"e3e3e3e3-e3e3-e3e3-e3e3-e3e3e3e3e3e3","updated_at":"2021-01-01 00:00:00.000","weight":0.5}
                    ],
                created_at = '',
                positions = [
                    56
                    ],
                query = ''
            )
        else:
            return SearchQueriesWithClicksCTRResponse(
                clicked_chunks = [
                    {"chunk_html":"<p>Hello, world!</p>","created_at":"2021-01-01 00:00:00.000","dataset_id":"e3e3e3e3-e3e3-e3e3-e3e3-e3e3e3e3e3e3","id":"e3e3e3e3-e3e3-e3e3-e3e3-e3e3e3e3e3e3","link":"https://trieve.ai","metadata":{"key":"value"},"tag_set":"[tag1,tag2]","time_stamp":"2021-01-01 00:00:00.000","tracking_id":"e3e3e3e3-e3e3-e3e3-e3e3-e3e3e3e3e3e3","updated_at":"2021-01-01 00:00:00.000","weight":0.5}
                    ],
                created_at = '',
                positions = [
                    56
                    ],
                query = '',
        )
        """

    def testSearchQueriesWithClicksCTRResponse(self):
        """Test SearchQueriesWithClicksCTRResponse"""
        # inst_req_only = self.make_instance(include_optional=False)
        # inst_req_and_optional = self.make_instance(include_optional=True)

if __name__ == '__main__':
    unittest.main()
