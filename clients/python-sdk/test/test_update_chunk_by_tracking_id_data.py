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

from trieve_py_client.models.update_chunk_by_tracking_id_data import UpdateChunkByTrackingIdData

class TestUpdateChunkByTrackingIdData(unittest.TestCase):
    """UpdateChunkByTrackingIdData unit test stubs"""

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def make_instance(self, include_optional) -> UpdateChunkByTrackingIdData:
        """Test UpdateChunkByTrackingIdData
            include_option is a boolean, when False only required
            params are included, when True both required and
            optional params are included """
        # uncomment below to create an instance of `UpdateChunkByTrackingIdData`
        """
        model = UpdateChunkByTrackingIdData()
        if include_optional:
            return UpdateChunkByTrackingIdData(
                chunk_html = '',
                convert_html_to_text = True,
                group_ids = [
                    ''
                    ],
                group_tracking_ids = [
                    ''
                    ],
                link = '',
                metadata = None,
                time_stamp = '',
                tracking_id = '',
                weight = 1.337
            )
        else:
            return UpdateChunkByTrackingIdData(
                tracking_id = '',
        )
        """

    def testUpdateChunkByTrackingIdData(self):
        """Test UpdateChunkByTrackingIdData"""
        # inst_req_only = self.make_instance(include_optional=False)
        # inst_req_and_optional = self.make_instance(include_optional=True)

if __name__ == '__main__':
    unittest.main()
