# coding: utf-8

"""
    Trieve API

    Trieve OpenAPI Specification. This document describes all of the operations available through the Trieve API.

    The version of the OpenAPI document: 0.12.0
    Contact: developers@trieve.ai
    Generated by OpenAPI Generator (https://openapi-generator.tech)

    Do not edit the class manually.
"""  # noqa: E501


from __future__ import annotations
import json
from enum import Enum
from typing_extensions import Self


class DistanceMetric(str, Enum):
    """
    DistanceMetric
    """

    """
    allowed enum values
    """
    EUCLIDEAN = 'euclidean'
    COSINE = 'cosine'
    MANHATTAN = 'manhattan'
    DOT = 'dot'

    @classmethod
    def from_json(cls, json_str: str) -> Self:
        """Create an instance of DistanceMetric from a JSON string"""
        return cls(json.loads(json_str))


