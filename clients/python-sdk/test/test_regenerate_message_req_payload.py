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

from trieve_py_client.models.regenerate_message_req_payload import RegenerateMessageReqPayload

class TestRegenerateMessageReqPayload(unittest.TestCase):
    """RegenerateMessageReqPayload unit test stubs"""

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def make_instance(self, include_optional) -> RegenerateMessageReqPayload:
        """Test RegenerateMessageReqPayload
            include_option is a boolean, when False only required
            params are included, when True both required and
            optional params are included """
        # uncomment below to create an instance of `RegenerateMessageReqPayload`
        """
        model = RegenerateMessageReqPayload()
        if include_optional:
            return RegenerateMessageReqPayload(
                concat_user_messages_query = True,
                filters = {"must":[{"field":"tag_set","match_all":["A","B"]},{"field":"num_value","range":{"gte":10,"lte":25}}]},
                highlight_options = trieve_py_client.models.highlight_options.HighlightOptions(
                    highlight_delimiters = [
                        ''
                        ], 
                    highlight_max_length = 0, 
                    highlight_max_num = 0, 
                    highlight_results = True, 
                    highlight_strategy = null, 
                    highlight_threshold = 1.337, 
                    highlight_window = 0, ),
                llm_options = trieve_py_client.models.llm_options.LLMOptions(
                    completion_first = True, 
                    frequency_penalty = 1.337, 
                    max_tokens = 0, 
                    presence_penalty = 1.337, 
                    stop_tokens = [
                        ''
                        ], 
                    stream_response = True, 
                    system_prompt = '', 
                    temperature = 1.337, ),
                page_size = 0,
                score_threshold = 1.337,
                search_query = '',
                search_type = 'fulltext',
                topic_id = '',
                user_id = ''
            )
        else:
            return RegenerateMessageReqPayload(
                topic_id = '',
        )
        """

    def testRegenerateMessageReqPayload(self):
        """Test RegenerateMessageReqPayload"""
        # inst_req_only = self.make_instance(include_optional=False)
        # inst_req_and_optional = self.make_instance(include_optional=True)

if __name__ == '__main__':
    unittest.main()
