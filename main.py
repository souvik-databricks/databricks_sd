SD_TARGET_PORT = 8000

from fastapi import FastAPI
from fastapi.responses import PlainTextResponse
import uvicorn
import json

app = FastAPI()

@app.get("/clusters")
async def get_payload():
    with open('src/clusters.json', 'r') as f:
        payload = json.load(f)
    return payload


@app.get("/cluster_metrics", response_class=PlainTextResponse)
async def read_metrics():
    with open("src/cluster_metrics.txt", "r") as f:
        contents = f.read()
    return contents
