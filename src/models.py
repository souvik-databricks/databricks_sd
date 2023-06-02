from typing import Dict

from pydantic import BaseModel


class EndpointSpec(BaseModel):
    workspace_host: str
    token: str
    endpoint_name: str
    labels: Dict[str, str]


class RouteConfig(BaseModel):
    specs: list[EndpointSpec]
