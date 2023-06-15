SD_TARGET_PORT = 8000

from fastapi import FastAPI
import uvicorn
import json

app = FastAPI()

@app.get("/payload")
async def get_payload():
    with open('payload.json', 'r') as f:
        payload = json.load(f)
    return payload

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=SD_TARGET_PORT)
