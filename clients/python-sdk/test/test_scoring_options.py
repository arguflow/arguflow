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

from trieve_py_client.models.scoring_options import ScoringOptions

class TestScoringOptions(unittest.TestCase):
    """ScoringOptions unit test stubs"""

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def make_instance(self, include_optional) -> ScoringOptions:
        """Test ScoringOptions
            include_option is a boolean, when False only required
            params are included, when True both required and
            optional params are included """
        # uncomment below to create an instance of `ScoringOptions`
        """
        model = ScoringOptions()
        if include_optional:
            return ScoringOptions(
                fulltext_boost = trieve_py_client.models.full_text_boost.FullTextBoost(
                    boost_factor = 1.337, 
                    phrase = '', ),
                semantic_boost = trieve_py_client.models.semantic_boost.SemanticBoost(
                    distance_factor = 1.337, 
                    phrase = '', )
            )
        else:
            return ScoringOptions(
        )
        """

    def testScoringOptions(self):
        """Test ScoringOptions"""
        # inst_req_only = self.make_instance(include_optional=False)
        # inst_req_and_optional = self.make_instance(include_optional=True)

if __name__ == '__main__':
    unittest.main()
