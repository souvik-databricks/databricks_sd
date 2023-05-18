import json
from pydantic import BaseModel, parse_obj_as
from collections import Counter

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


# output contract

# same as input, just as some Python objects or series of Python objects


# rough steps

# 1. read input
# 2. validate input
#   output paths should be distinct
# 3.

j = """
{
    "specs": [
        {
            "workspace_url": "https://blah1.com",
            "token": "dapiblah1",
            "output_path": "s3://bucket/prefix/blah1.json"
        },
        {
            "workspace_url": "https://blah2.com",
            "token": "dapiblah2",
            "output_path": "s3://bucket/prefix/blah2.json"
        }
     ]
}
"""

class MetricsConfig(BaseModel):
    workspace_url: str
    token: str
    output_path: str

d = json.loads(j)

specs = d["specs"]

workspace_configs = parse_obj_as(list[MetricsConfig], specs)

output_paths = (config.output_path for config in workspace_configs)
output_path_counter = Counter(output_paths)

duplicates = [element for element, count in output_path_counter.items() if count > 1]

if duplicates:
    raise ValueError(f"Detected duplicate output paths [{', '.join(duplicates)}]. Please edit the configuration to "
                     f"make the paths distinct.")

print(workspace_configs)


