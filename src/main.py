SD_TARGET_PORT = 8000

from fastapi import FastAPI
from fastapi.responses import PlainTextResponse
import uvicorn
import json

app = FastAPI()

@app.get("/clusters")
async def get_payload():
    with open('clusters.json', 'r') as f:
        payload = json.load(f)
    return payload


@app.get("/metrics", response_class=PlainTextResponse)
async def read_metrics():
    with open("metrics.txt", "r") as f:
        contents = f.read()
    return contents


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=SD_TARGET_PORT)
