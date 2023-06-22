# Databricks notebook source
# DBTITLE 1,Installs
# MAGIC %pip install uvicorn fastapi databricks-sdk ruamel.yaml

# COMMAND ----------

api_url = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiUrl().getOrElse(None)
token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().getOrElse(None)
workspace_id = dbutils.notebook.entry_point.getDbutils().notebook().getContext().workspaceId().getOrElse(None)
host = dbutils.notebook.entry_point.getDbutils().notebook().getContext().browserHostName().getOrElse(None)

from dataclasses import dataclass

@dataclass
class NotebookContext:
    api_url: str
    token: str
    workspace_id: str
    host: str
    cluster_id: str
    cloud: str = None

    def __post_init__(self):
        self.cloud = self.detect_cloud(self.host)

    def detect_cloud(self, host):
        suffix_url_settings = {
            "aws": "cloud.databricks.com",
            "azure": "azuredatabricks.net",
        }

        for cloud, suffix in suffix_url_settings.items():
            if host and host.endswith(suffix):
                return cloud
        else:
            raise ValueError(f"Cloud not detected given host={host}")


context = NotebookContext(
  api_url=dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiUrl().getOrElse(None),
  token=dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().getOrElse(None),
  workspace_id=dbutils.notebook.entry_point.getDbutils().notebook().getContext().workspaceId().getOrElse(None),
  host=dbutils.notebook.entry_point.getDbutils().notebook().getContext().browserHostName().getOrElse(None),
  cluster_id=dbutils.notebook.entry_point.getDbutils().notebook().getContext().clusterId().getOrElse(None)
)

# COMMAND ----------

# DBTITLE 1,Debug links
workspace_id = dbutils.notebook.entry_point.getDbutils().notebook().getContext().workspaceId().getOrElse(None)
host = dbutils.notebook.entry_point.getDbutils().notebook().getContext().browserHostName().getOrElse(None)
def generate_html_link(url, text, new_tab=False):
    target = ' target="_blank"' if new_tab else ""
    return f'<a href="{url}"{target}>{text}</a>'
  
def generate_target_url(context):
  cluster_id = context.cluster_id
  displayHTML(generate_html_link(f"https://{host}/driver-proxy/o/{workspace_id}/{cluster_id}/8000/cluster_metrics", "Cluster Metrics", )) 
  displayHTML(generate_html_link(f"https://{host}/driver-proxy/o/{workspace_id}/{cluster_id}/8000/clusters", "Clusters", ))
  displayHTML(generate_html_link(f"https://{host}/driver-proxy/o/{workspace_id}/{cluster_id}/8001/metrics", "Metrics (very meta)", )) 
generate_target_url(context)

# COMMAND ----------

# DBTITLE 1,YAML generation
import json
import ruamel.yaml
import sys


def static_configs(context, port=8000, endpoint="cluster_metrics/"):
    return {
        "metrics_path": f"/driver-proxy-api/o/{context.workspace_id}/{context.cluster_id}/{port}/{endpoint}",
        "static_configs": [{"targets": [f"{context.host}"]}],
        "scheme": "https",
    }


def basic_configs(token, name="mama_mia"):
    return {"job_name": name, "bearer_token": token}


def http_sd_configs(context, token, port=8000, endpoint="clusters/"):
    return {
        "http_sd_configs": [
            {
                "url": f"https://{context.host}/driver-proxy-api/o/{context.workspace_id}/{context.cluster_id}/{port}/{endpoint}",
                "authorization": {"type": "Bearer", "credentials": token},
            }
        ]
    }


def relabel_configs(endpoint="metrics/executors/prometheus"):
    source_labels = ruamel.yaml.comments.CommentedSeq(
        ["workspace_id", "cluster_id", "__port"]
    )
    source_labels.fa.set_flow_style()  # setting flow style
    return {
        "relabel_configs": [
            {
                "source_labels": source_labels,
                "target_label": "__metrics_path__",
                "separator": "/",
                "replacement": f"driver-proxy-api/o/${1}${2}${3}/{endpoint}",
            }
        ]
    }


def get_confs(token, confs=[("databricks_sd", "sd"), ("databricks_static", "static")]):
    type_conf_mapping = {
        "sd": lambda name: {
            **basic_configs(token, name),
            **http_sd_configs(context, token),
            **relabel_configs(),
        },
        "static": lambda name: {
            **basic_configs(token, name),
            **static_configs(context),
        },
    }

    return [type_conf_mapping[conf_type](name) for name, conf_type in confs]


def print_as_yaml(d, fresh=False):
    # Create YAML object
    yaml = ruamel.yaml.YAML()
    yaml.Representer.add_representer(
        ruamel.yaml.comments.CommentedSeq,
        ruamel.yaml.representer.RoundTripRepresenter.represent_list,
    )
    yaml.indent(mapping=2, sequence=4, offset=2)
    # Print YAML
    if fresh:
        d = {"scrape_configs": d}
    yaml.dump(d, sys.stdout)


token = "PLUG_HERE"
print_as_yaml(get_confs(token), fresh=True)

# COMMAND ----------

# DBTITLE 1,App runner
# MAGIC %sh
# MAGIC uvicorn main:app --host 0.0.0.0 --port 8000 --reload
