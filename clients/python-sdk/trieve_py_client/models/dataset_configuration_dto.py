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
import pprint
import re  # noqa: F401
import json

from pydantic import BaseModel, ConfigDict, Field, StrictBool, StrictFloat, StrictInt, StrictStr
from typing import Any, ClassVar, Dict, List, Optional, Union
from typing_extensions import Annotated
from trieve_py_client.models.distance_metric import DistanceMetric
from trieve_py_client.models.public_dataset_options import PublicDatasetOptions
from typing import Optional, Set
from typing_extensions import Self

class DatasetConfigurationDTO(BaseModel):
    """
    Lets you specify the configuration for a dataset
    """ # noqa: E501
    bm25_avg_len: Optional[Union[StrictFloat, StrictInt]] = Field(default=None, description="The average length of the chunks in the index for BM25", alias="BM25_AVG_LEN")
    bm25_b: Optional[Union[StrictFloat, StrictInt]] = Field(default=None, description="The BM25 B parameter", alias="BM25_B")
    bm25_enabled: Optional[StrictBool] = Field(default=None, description="Whether to use BM25", alias="BM25_ENABLED")
    bm25_k: Optional[Union[StrictFloat, StrictInt]] = Field(default=None, description="The BM25 K parameter", alias="BM25_K")
    distance_metric: Optional[DistanceMetric] = Field(default=None, alias="DISTANCE_METRIC")
    embedding_base_url: Optional[StrictStr] = Field(default=None, description="The base URL for the embedding API", alias="EMBEDDING_BASE_URL")
    embedding_model_name: Optional[StrictStr] = Field(default=None, description="The name of the embedding model to use", alias="EMBEDDING_MODEL_NAME")
    embedding_query_prefix: Optional[StrictStr] = Field(default=None, description="The prefix to use for the embedding query", alias="EMBEDDING_QUERY_PREFIX")
    embedding_size: Optional[Annotated[int, Field(strict=True, ge=0)]] = Field(default=None, description="The size of the embeddings", alias="EMBEDDING_SIZE")
    frequency_penalty: Optional[Union[StrictFloat, StrictInt]] = Field(default=None, description="The frequency penalty to use", alias="FREQUENCY_PENALTY")
    fulltext_enabled: Optional[StrictBool] = Field(default=None, description="Whether to use fulltext search", alias="FULLTEXT_ENABLED")
    indexed_only: Optional[StrictBool] = Field(default=None, description="Whether to only use indexed chunks", alias="INDEXED_ONLY")
    llm_base_url: Optional[StrictStr] = Field(default=None, description="The base URL for the LLM API", alias="LLM_BASE_URL")
    llm_default_model: Optional[StrictStr] = Field(default=None, description="The default model to use for the LLM", alias="LLM_DEFAULT_MODEL")
    locked: Optional[StrictBool] = Field(default=None, description="Whether the dataset is locked to prevent changes or deletion", alias="LOCKED")
    max_limit: Optional[Annotated[int, Field(strict=True, ge=0)]] = Field(default=None, description="The maximum limit for the number of chunks for counting", alias="MAX_LIMIT")
    max_tokens: Optional[Annotated[int, Field(strict=True, ge=0)]] = Field(default=None, description="The maximum number of tokens to use in LLM Response", alias="MAX_TOKENS")
    message_to_query_prompt: Optional[StrictStr] = Field(default=None, description="The prompt to use for converting a message to a query", alias="MESSAGE_TO_QUERY_PROMPT")
    n_retrievals_to_include: Optional[Annotated[int, Field(strict=True, ge=0)]] = Field(default=None, description="The number of retrievals to include with the RAG model", alias="N_RETRIEVALS_TO_INCLUDE")
    presence_penalty: Optional[Union[StrictFloat, StrictInt]] = Field(default=None, description="The presence penalty to use", alias="PRESENCE_PENALTY")
    public_dataset: Optional[PublicDatasetOptions] = Field(default=None, alias="PUBLIC_DATASET")
    rag_prompt: Optional[StrictStr] = Field(default=None, description="The prompt to use for the RAG model", alias="RAG_PROMPT")
    reranker_base_url: Optional[StrictStr] = Field(default=None, description="The base URL for the reranker API", alias="RERANKER_BASE_URL")
    semantic_enabled: Optional[StrictBool] = Field(default=None, description="Whether to use semantic search", alias="SEMANTIC_ENABLED")
    stop_tokens: Optional[List[StrictStr]] = Field(default=None, description="The stop tokens to use", alias="STOP_TOKENS")
    system_prompt: Optional[StrictStr] = Field(default=None, description="The system prompt to use for the LLM", alias="SYSTEM_PROMPT")
    temperature: Optional[Union[StrictFloat, StrictInt]] = Field(default=None, description="The temperature to use", alias="TEMPERATURE")
    use_message_to_query_prompt: Optional[StrictBool] = Field(default=None, description="Whether to use the message to query prompt", alias="USE_MESSAGE_TO_QUERY_PROMPT")
    __properties: ClassVar[List[str]] = ["BM25_AVG_LEN", "BM25_B", "BM25_ENABLED", "BM25_K", "DISTANCE_METRIC", "EMBEDDING_BASE_URL", "EMBEDDING_MODEL_NAME", "EMBEDDING_QUERY_PREFIX", "EMBEDDING_SIZE", "FREQUENCY_PENALTY", "FULLTEXT_ENABLED", "INDEXED_ONLY", "LLM_BASE_URL", "LLM_DEFAULT_MODEL", "LOCKED", "MAX_LIMIT", "MAX_TOKENS", "MESSAGE_TO_QUERY_PROMPT", "N_RETRIEVALS_TO_INCLUDE", "PRESENCE_PENALTY", "PUBLIC_DATASET", "RAG_PROMPT", "RERANKER_BASE_URL", "SEMANTIC_ENABLED", "STOP_TOKENS", "SYSTEM_PROMPT", "TEMPERATURE", "USE_MESSAGE_TO_QUERY_PROMPT"]

    model_config = ConfigDict(
        populate_by_name=True,
        validate_assignment=True,
        protected_namespaces=(),
    )


    def to_str(self) -> str:
        """Returns the string representation of the model using alias"""
        return pprint.pformat(self.model_dump(by_alias=True))

    def to_json(self) -> str:
        """Returns the JSON representation of the model using alias"""
        # TODO: pydantic v2: use .model_dump_json(by_alias=True, exclude_unset=True) instead
        return json.dumps(self.to_dict())

    @classmethod
    def from_json(cls, json_str: str) -> Optional[Self]:
        """Create an instance of DatasetConfigurationDTO from a JSON string"""
        return cls.from_dict(json.loads(json_str))

    def to_dict(self) -> Dict[str, Any]:
        """Return the dictionary representation of the model using alias.

        This has the following differences from calling pydantic's
        `self.model_dump(by_alias=True)`:

        * `None` is only added to the output dict for nullable fields that
          were set at model initialization. Other fields with value `None`
          are ignored.
        """
        excluded_fields: Set[str] = set([
        ])

        _dict = self.model_dump(
            by_alias=True,
            exclude=excluded_fields,
            exclude_none=True,
        )
        # override the default output from pydantic by calling `to_dict()` of public_dataset
        if self.public_dataset:
            _dict['PUBLIC_DATASET'] = self.public_dataset.to_dict()
        # set to None if bm25_avg_len (nullable) is None
        # and model_fields_set contains the field
        if self.bm25_avg_len is None and "bm25_avg_len" in self.model_fields_set:
            _dict['BM25_AVG_LEN'] = None

        # set to None if bm25_b (nullable) is None
        # and model_fields_set contains the field
        if self.bm25_b is None and "bm25_b" in self.model_fields_set:
            _dict['BM25_B'] = None

        # set to None if bm25_enabled (nullable) is None
        # and model_fields_set contains the field
        if self.bm25_enabled is None and "bm25_enabled" in self.model_fields_set:
            _dict['BM25_ENABLED'] = None

        # set to None if bm25_k (nullable) is None
        # and model_fields_set contains the field
        if self.bm25_k is None and "bm25_k" in self.model_fields_set:
            _dict['BM25_K'] = None

        # set to None if distance_metric (nullable) is None
        # and model_fields_set contains the field
        if self.distance_metric is None and "distance_metric" in self.model_fields_set:
            _dict['DISTANCE_METRIC'] = None

        # set to None if embedding_base_url (nullable) is None
        # and model_fields_set contains the field
        if self.embedding_base_url is None and "embedding_base_url" in self.model_fields_set:
            _dict['EMBEDDING_BASE_URL'] = None

        # set to None if embedding_model_name (nullable) is None
        # and model_fields_set contains the field
        if self.embedding_model_name is None and "embedding_model_name" in self.model_fields_set:
            _dict['EMBEDDING_MODEL_NAME'] = None

        # set to None if embedding_query_prefix (nullable) is None
        # and model_fields_set contains the field
        if self.embedding_query_prefix is None and "embedding_query_prefix" in self.model_fields_set:
            _dict['EMBEDDING_QUERY_PREFIX'] = None

        # set to None if embedding_size (nullable) is None
        # and model_fields_set contains the field
        if self.embedding_size is None and "embedding_size" in self.model_fields_set:
            _dict['EMBEDDING_SIZE'] = None

        # set to None if frequency_penalty (nullable) is None
        # and model_fields_set contains the field
        if self.frequency_penalty is None and "frequency_penalty" in self.model_fields_set:
            _dict['FREQUENCY_PENALTY'] = None

        # set to None if fulltext_enabled (nullable) is None
        # and model_fields_set contains the field
        if self.fulltext_enabled is None and "fulltext_enabled" in self.model_fields_set:
            _dict['FULLTEXT_ENABLED'] = None

        # set to None if indexed_only (nullable) is None
        # and model_fields_set contains the field
        if self.indexed_only is None and "indexed_only" in self.model_fields_set:
            _dict['INDEXED_ONLY'] = None

        # set to None if llm_base_url (nullable) is None
        # and model_fields_set contains the field
        if self.llm_base_url is None and "llm_base_url" in self.model_fields_set:
            _dict['LLM_BASE_URL'] = None

        # set to None if llm_default_model (nullable) is None
        # and model_fields_set contains the field
        if self.llm_default_model is None and "llm_default_model" in self.model_fields_set:
            _dict['LLM_DEFAULT_MODEL'] = None

        # set to None if locked (nullable) is None
        # and model_fields_set contains the field
        if self.locked is None and "locked" in self.model_fields_set:
            _dict['LOCKED'] = None

        # set to None if max_limit (nullable) is None
        # and model_fields_set contains the field
        if self.max_limit is None and "max_limit" in self.model_fields_set:
            _dict['MAX_LIMIT'] = None

        # set to None if max_tokens (nullable) is None
        # and model_fields_set contains the field
        if self.max_tokens is None and "max_tokens" in self.model_fields_set:
            _dict['MAX_TOKENS'] = None

        # set to None if message_to_query_prompt (nullable) is None
        # and model_fields_set contains the field
        if self.message_to_query_prompt is None and "message_to_query_prompt" in self.model_fields_set:
            _dict['MESSAGE_TO_QUERY_PROMPT'] = None

        # set to None if n_retrievals_to_include (nullable) is None
        # and model_fields_set contains the field
        if self.n_retrievals_to_include is None and "n_retrievals_to_include" in self.model_fields_set:
            _dict['N_RETRIEVALS_TO_INCLUDE'] = None

        # set to None if presence_penalty (nullable) is None
        # and model_fields_set contains the field
        if self.presence_penalty is None and "presence_penalty" in self.model_fields_set:
            _dict['PRESENCE_PENALTY'] = None

        # set to None if public_dataset (nullable) is None
        # and model_fields_set contains the field
        if self.public_dataset is None and "public_dataset" in self.model_fields_set:
            _dict['PUBLIC_DATASET'] = None

        # set to None if rag_prompt (nullable) is None
        # and model_fields_set contains the field
        if self.rag_prompt is None and "rag_prompt" in self.model_fields_set:
            _dict['RAG_PROMPT'] = None

        # set to None if reranker_base_url (nullable) is None
        # and model_fields_set contains the field
        if self.reranker_base_url is None and "reranker_base_url" in self.model_fields_set:
            _dict['RERANKER_BASE_URL'] = None

        # set to None if semantic_enabled (nullable) is None
        # and model_fields_set contains the field
        if self.semantic_enabled is None and "semantic_enabled" in self.model_fields_set:
            _dict['SEMANTIC_ENABLED'] = None

        # set to None if stop_tokens (nullable) is None
        # and model_fields_set contains the field
        if self.stop_tokens is None and "stop_tokens" in self.model_fields_set:
            _dict['STOP_TOKENS'] = None

        # set to None if system_prompt (nullable) is None
        # and model_fields_set contains the field
        if self.system_prompt is None and "system_prompt" in self.model_fields_set:
            _dict['SYSTEM_PROMPT'] = None

        # set to None if temperature (nullable) is None
        # and model_fields_set contains the field
        if self.temperature is None and "temperature" in self.model_fields_set:
            _dict['TEMPERATURE'] = None

        # set to None if use_message_to_query_prompt (nullable) is None
        # and model_fields_set contains the field
        if self.use_message_to_query_prompt is None and "use_message_to_query_prompt" in self.model_fields_set:
            _dict['USE_MESSAGE_TO_QUERY_PROMPT'] = None

        return _dict

    @classmethod
    def from_dict(cls, obj: Optional[Dict[str, Any]]) -> Optional[Self]:
        """Create an instance of DatasetConfigurationDTO from a dict"""
        if obj is None:
            return None

        if not isinstance(obj, dict):
            return cls.model_validate(obj)

        _obj = cls.model_validate({
            "BM25_AVG_LEN": obj.get("BM25_AVG_LEN"),
            "BM25_B": obj.get("BM25_B"),
            "BM25_ENABLED": obj.get("BM25_ENABLED"),
            "BM25_K": obj.get("BM25_K"),
            "DISTANCE_METRIC": obj.get("DISTANCE_METRIC"),
            "EMBEDDING_BASE_URL": obj.get("EMBEDDING_BASE_URL"),
            "EMBEDDING_MODEL_NAME": obj.get("EMBEDDING_MODEL_NAME"),
            "EMBEDDING_QUERY_PREFIX": obj.get("EMBEDDING_QUERY_PREFIX"),
            "EMBEDDING_SIZE": obj.get("EMBEDDING_SIZE"),
            "FREQUENCY_PENALTY": obj.get("FREQUENCY_PENALTY"),
            "FULLTEXT_ENABLED": obj.get("FULLTEXT_ENABLED"),
            "INDEXED_ONLY": obj.get("INDEXED_ONLY"),
            "LLM_BASE_URL": obj.get("LLM_BASE_URL"),
            "LLM_DEFAULT_MODEL": obj.get("LLM_DEFAULT_MODEL"),
            "LOCKED": obj.get("LOCKED"),
            "MAX_LIMIT": obj.get("MAX_LIMIT"),
            "MAX_TOKENS": obj.get("MAX_TOKENS"),
            "MESSAGE_TO_QUERY_PROMPT": obj.get("MESSAGE_TO_QUERY_PROMPT"),
            "N_RETRIEVALS_TO_INCLUDE": obj.get("N_RETRIEVALS_TO_INCLUDE"),
            "PRESENCE_PENALTY": obj.get("PRESENCE_PENALTY"),
            "PUBLIC_DATASET": PublicDatasetOptions.from_dict(obj["PUBLIC_DATASET"]) if obj.get("PUBLIC_DATASET") is not None else None,
            "RAG_PROMPT": obj.get("RAG_PROMPT"),
            "RERANKER_BASE_URL": obj.get("RERANKER_BASE_URL"),
            "SEMANTIC_ENABLED": obj.get("SEMANTIC_ENABLED"),
            "STOP_TOKENS": obj.get("STOP_TOKENS"),
            "SYSTEM_PROMPT": obj.get("SYSTEM_PROMPT"),
            "TEMPERATURE": obj.get("TEMPERATURE"),
            "USE_MESSAGE_TO_QUERY_PROMPT": obj.get("USE_MESSAGE_TO_QUERY_PROMPT")
        })
        return _obj


