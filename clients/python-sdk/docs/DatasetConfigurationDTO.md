# DatasetConfigurationDTO

Lets you specify the configuration for a dataset

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**bm25_avg_len** | **float** | The average length of the chunks in the index for BM25 | [optional] 
**bm25_b** | **float** | The BM25 B parameter | [optional] 
**bm25_enabled** | **bool** | Whether to use BM25 | [optional] 
**bm25_k** | **float** | The BM25 K parameter | [optional] 
**disable_analytics** | **bool** | Whether to disable analytics | [optional] 
**distance_metric** | [**DistanceMetric**](DistanceMetric.md) |  | [optional] 
**embedding_base_url** | **str** | The base URL for the embedding API | [optional] 
**embedding_model_name** | **str** | The name of the embedding model to use | [optional] 
**embedding_query_prefix** | **str** | The prefix to use for the embedding query | [optional] 
**embedding_size** | **int** | The size of the embeddings | [optional] 
**frequency_penalty** | **float** | The frequency penalty to use | [optional] 
**fulltext_enabled** | **bool** | Whether to use fulltext search | [optional] 
**indexed_only** | **bool** | Whether to only use indexed chunks | [optional] 
**llm_base_url** | **str** | The base URL for the LLM API | [optional] 
**llm_default_model** | **str** | The default model to use for the LLM | [optional] 
**locked** | **bool** | Whether the dataset is locked to prevent changes or deletion | [optional] 
**max_limit** | **int** | The maximum limit for the number of chunks for counting | [optional] 
**max_tokens** | **int** | The maximum number of tokens to use in LLM Response | [optional] 
**message_to_query_prompt** | **str** | The prompt to use for converting a message to a query | [optional] 
**n_retrievals_to_include** | **int** | The number of retrievals to include with the RAG model | [optional] 
**pagefind_enabled** | **bool** | Whether to enable pagefind indexing | [optional] 
**presence_penalty** | **float** | The presence penalty to use | [optional] 
**public_dataset** | [**PublicDatasetOptions**](PublicDatasetOptions.md) |  | [optional] 
**qdrant_only** | **bool** | Whether or not to insert chunks into Postgres | [optional] 
**rag_prompt** | **str** | The prompt to use for the RAG model | [optional] 
**reranker_base_url** | **str** | The base URL for the reranker API | [optional] 
**reranker_model_name** | **str** | The model name for the Reranker API | [optional] 
**semantic_enabled** | **bool** | Whether to use semantic search | [optional] 
**stop_tokens** | **List[str]** | The stop tokens to use | [optional] 
**system_prompt** | **str** | The system prompt to use for the LLM | [optional] 
**temperature** | **float** | The temperature to use | [optional] 
**use_message_to_query_prompt** | **bool** | Whether to use the message to query prompt | [optional] 
**task_definition** | **string** | A statement to specify the domain of the context documents for AIMon reranker model. | [optional] 


## Example

```python
from trieve_py_client.models.dataset_configuration_dto import DatasetConfigurationDTO

# TODO update the JSON string below
json = "{}"
# create an instance of DatasetConfigurationDTO from a JSON string
dataset_configuration_dto_instance = DatasetConfigurationDTO.from_json(json)
# print the JSON string representation of the object
print(DatasetConfigurationDTO.to_json())

# convert the object into a dict
dataset_configuration_dto_dict = dataset_configuration_dto_instance.to_dict()
# create an instance of DatasetConfigurationDTO from a dict
dataset_configuration_dto_form_dict = dataset_configuration_dto.from_dict(dataset_configuration_dto_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


