import uvicorn
from databricks_cli.clusters.api import ClusterApi
from databricks_cli.sdk.api_client import ApiClient
from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import JSONResponse

from src.models import EndpointSpec, RouteConfig
from src.supporters import parse_url

app = FastAPI()

dynamic_routes = {}
endpoint_names = []


def __config_gen(spec: EndpointSpec):
    _driver_proxy, org_id, workspacehost = parse_url(spec.workspace_host)
    api_client = ApiClient(
        host=workspacehost,
        token=spec.token
    )
    clusters_api = ClusterApi(api_client)
    clusters_list = clusters_api.list_clusters()
    clusters_list = [{"cluster_id": i.get("cluster_id")} for i in clusters_list["clusters"] if
                     i.get("driver") is not None]
    targets = []
    if clusters_list:
        for i in clusters_list:
            c_id = i.get("cluster_id")
            driver_proxy_uri = f"{_driver_proxy}/{c_id}/8100"
            targets.append(driver_proxy_uri)
    labels_dictionary = {
        "workspace": f"{workspacehost}",
        "orgId": f"{org_id}"
    }
    labels_dictionary.update(spec.labels)
    data = [{
        "labels": labels_dictionary,
        "targets": targets
    }]
    return data


def create_route(spec: EndpointSpec):
    async def dynamic_route():
        value = __config_gen(spec)
        return value

    return dynamic_route


@app.post("/configure")
async def configure_routes(config: RouteConfig):
    seen_names = set()
    new_endpoints = []
    for spec in config.specs:
        if spec.endpoint_name in seen_names:
            raise HTTPException(status_code=400, detail=f"Duplicate endpoint_name: {spec.endpoint_name}")
        seen_names.add(spec.endpoint_name)

        if spec.endpoint_name not in dynamic_routes:
            dynamic_routes[spec.endpoint_name] = create_route(spec)
            app.get(f"/{spec.endpoint_name}")(dynamic_routes[spec.endpoint_name])
            new_endpoints.append(f"/{spec.endpoint_name}")
            endpoint_names.append(spec.endpoint_name)
    return {"message": "Configuration Successful", "new_endpoints": new_endpoints}


@app.get("/")
async def root():
    return {
        "message": "Welcome to the App!",
        "documentation": {
            "/configure": "POST endpoint. Accepts a JSON body with a list of endpoint specifications. Creates a GET endpoint for each specification.",
            "/{name}": "GET endpoint. Returns a JSON object with the specification of the endpoint. The available endpoints are dynamically created based on the configuration."
        },
        "endpoints": [f"/{name}" for name in endpoint_names]
    }


@app.middleware("http")
async def check_dynamic_routes(request: Request, call_next):
    if request.method == "GET":
        path = request.url.path.strip("/")
        if path not in dynamic_routes and path not in ["docs", "openapi.json", "redoc", ""]:
            return JSONResponse(status_code=404, content={"message": "Not Found"})
    return await call_next(request)


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
