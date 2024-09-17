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

from trieve_py_client.models.sort_options import SortOptions

class TestSortOptions(unittest.TestCase):
    """SortOptions unit test stubs"""

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def make_instance(self, include_optional) -> SortOptions:
        """Test SortOptions
            include_option is a boolean, when False only required
            params are included, when True both required and
            optional params are included """
        # uncomment below to create an instance of `SortOptions`
        """
        model = SortOptions()
        if include_optional:
            return SortOptions(
                location_bias = trieve_py_client.models.geo_info_with_bias.GeoInfoWithBias(
                    bias = 1.337, 
                    location = trieve_py_client.models.geo_info.GeoInfo(
                        lat = null, 
                        lon = null, ), ),
                sort_by = None,
                tag_weights = {
                    'key' : 1.337
                    },
                use_weights = True
            )
        else:
            return SortOptions(
        )
        """

    def testSortOptions(self):
        """Test SortOptions"""
        # inst_req_only = self.make_instance(include_optional=False)
        # inst_req_and_optional = self.make_instance(include_optional=True)

if __name__ == '__main__':
    unittest.main()
