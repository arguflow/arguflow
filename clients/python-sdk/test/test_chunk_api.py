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

from trieve_py_client.api.chunk_api import ChunkApi


class TestChunkApi(unittest.TestCase):
    """ChunkApi unit test stubs"""

    def setUp(self) -> None:
        self.api = ChunkApi()

    def tearDown(self) -> None:
        pass

    def test_autocomplete(self) -> None:
        """Test case for autocomplete

        Autocomplete
        """
        pass

    def test_count_chunks(self) -> None:
        """Test case for count_chunks

        Count chunks above threshold
        """
        pass

    def test_create_chunk(self) -> None:
        """Test case for create_chunk

        Create or Upsert Chunk or Chunks
        """
        pass

    def test_delete_chunk(self) -> None:
        """Test case for delete_chunk

        Delete Chunk
        """
        pass

    def test_delete_chunk_by_tracking_id(self) -> None:
        """Test case for delete_chunk_by_tracking_id

        Delete Chunk By Tracking Id
        """
        pass

    def test_generate_off_chunks(self) -> None:
        """Test case for generate_off_chunks

        RAG on Specified Chunks
        """
        pass

    def test_get_chunk_by_id(self) -> None:
        """Test case for get_chunk_by_id

        Get Chunk By Id
        """
        pass

    def test_get_chunk_by_tracking_id(self) -> None:
        """Test case for get_chunk_by_tracking_id

        Get Chunk By Tracking Id
        """
        pass

    def test_get_chunks_by_ids(self) -> None:
        """Test case for get_chunks_by_ids

        Get Chunks By Ids
        """
        pass

    def test_get_chunks_by_tracking_ids(self) -> None:
        """Test case for get_chunks_by_tracking_ids

        Get Chunks By Tracking Ids
        """
        pass

    def test_get_recommended_chunks(self) -> None:
        """Test case for get_recommended_chunks

        Get Recommended Chunks
        """
        pass

    def test_get_suggested_queries(self) -> None:
        """Test case for get_suggested_queries

        Generate suggested queries
        """
        pass

    def test_scroll_dataset_chunks(self) -> None:
        """Test case for scroll_dataset_chunks

        Scroll Chunks
        """
        pass

    def test_search_chunks(self) -> None:
        """Test case for search_chunks

        Search
        """
        pass

    def test_update_chunk(self) -> None:
        """Test case for update_chunk

        Update Chunk
        """
        pass

    def test_update_chunk_by_tracking_id(self) -> None:
        """Test case for update_chunk_by_tracking_id

        Update Chunk By Tracking Id
        """
        pass


if __name__ == '__main__':
    unittest.main()
