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

from trieve_py_client.models.get_datasets_pagination import GetDatasetsPagination

class TestGetDatasetsPagination(unittest.TestCase):
    """GetDatasetsPagination unit test stubs"""

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def make_instance(self, include_optional) -> GetDatasetsPagination:
        """Test GetDatasetsPagination
            include_option is a boolean, when False only required
            params are included, when True both required and
            optional params are included """
        # uncomment below to create an instance of `GetDatasetsPagination`
        """
        model = GetDatasetsPagination()
        if include_optional:
            return GetDatasetsPagination(
                limit = 56,
                offset = 56
            )
        else:
            return GetDatasetsPagination(
        )
        """

    def testGetDatasetsPagination(self):
        """Test GetDatasetsPagination"""
        # inst_req_only = self.make_instance(include_optional=False)
        # inst_req_and_optional = self.make_instance(include_optional=True)

if __name__ == '__main__':
    unittest.main()
