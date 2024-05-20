from transformers import AutoModel
from fastapi import FastAPI, responses
from typing import Union, Annotated, Literal, List
from pydantic import BaseModel
from numpy.linalg import norm
from dotenv import load_dotenv
from os import environ

load_dotenv()

model_name = environ["MODEL"]
model = AutoModel.from_pretrained(model_name, trust_remote_code=True, cache_dir="./models", device_map = 'cuda')

app = FastAPI()

class OpenAIEmbeddingInput(BaseModel):
    input: Union[
        List[str],
        str
    ]

class OpenAIEmbeddingResult(BaseModel):
    data: list[list[float]]

@app.post(
    "/embeddings",
    response_model=OpenAIEmbeddingResult,
    response_class=responses.ORJSONResponse,
)
def embed(data: OpenAIEmbeddingInput):
    print(data.input)
    embeddings = model.encode(
        data.input
    )

    print(embeddings)
    if type(data.input) == str:
        return {
            "data": [embeddings.tolist()]
        }
    else:
        return {
            "data": embeddings.tolist()
        }

