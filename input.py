import json
from pydantic import BaseModel, parse_obj_as, validator
from collections import Counter
from urllib.parse import urlparse
from pathlib import Path
from databricks.sdk.service.iam import CurrentUserAPI
from databricks.sdk.core import Config, ApiClient

# input contract
"""
    - workspace URL/ID
    - workspace-level auth - Databricks OAuth/PAT Token
    - path of output file
    - (optional) (later) labels
    - (optional) (later) cluster filtering
    - (optional) (later) format of output file - JSON/YAML - JSON only for now
    - (optional) (later) executor IP flag - in case driver metrics aren't enough.
"""

# rough steps
# 1. read input
# 2. validate input
#    output paths should be distinct
# 3. outputs should be available as Python objects

j = """
{
    "specs": [
        {
            "workspace_host": "xxxx",
            "token": "xxxx",
            "output_path": "/path/to/blah1.json"
        }
     ]
}
"""

class DatabricksServiceDiscoveryConfig(BaseModel):
    workspace_host: str
    output_path: str
    token: str

    @validator('output_path')
    def validate_output_path(cls, v) -> str:
        try:
            Path(v).resolve()  # this is best-effort
        except (OSError, RuntimeError):
            raise ValueError(f"Invalid path: {v}. Please check the spelling and file write permissions.")
        return v

    @validator('token')
    def validate_token(cls, v, values):
        host = values["workspace_host"]
        cfg = Config(host=host, token=v)
        api_client = ApiClient(cfg)
        try:
            CurrentUserAPI(api_client).me()
        except Exception as e:
            raise ValueError(
                f"Error occurred: {e}. Please check the provided token and workspace host.")
        return v

d = json.loads(j)

specs = d["specs"]

sd_configs = parse_obj_as(list[DatabricksServiceDiscoveryConfig], specs)

# TODO cgrant: this can be much more efficient. do all "wide" validations together
# Validate distinct output paths
def validate_distinct_paths(sd_configs):
    output_paths = (config.output_path for config in sd_configs)
    output_path_counter = Counter(output_paths)

    duplicates = [element for element, count in output_path_counter.items() if count > 1]

    if duplicates:
        raise ValueError(f"Detected duplicate output paths [{', '.join(duplicates)}]. Please edit the configuration to "
                         f"make the paths distinct.")


validate_distinct_paths(sd_configs)


print(sd_configs)


