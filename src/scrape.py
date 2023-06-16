# Databricks notebook source
# MAGIC %pip install databricks-sdk

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

SD_TARGET_PORT = 8000
POLLING_INTERVAL = 30

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
  host=dbutils.notebook.entry_point.getDbutils().notebook().getContext().browserHostName().getOrElse(None)
)



# COMMAND ----------

def generate_driver_proxy_url(context: NotebookContext, cluster_id, port):
    
    if context.cloud == "azure":
      magic_number = int(context.workspace_id) % 20
      prefix = "https://adb"
      new_url = f"{prefix}-{workspace_id}.{magic_number}.azuredatabricks.net/driver-proxy-api/o/{context.workspace_id}/{cluster_id}/{port}"
    elif context.cloud == "aws":
      new_url = f"https://{context.host}/driver-proxy-api/o/{workspace_id}/{cluster_id}/{port}"
    return new_url
  
generate_driver_proxy_url(context, dbutils.notebook.entry_point.getDbutils().notebook().getContext().clusterId().getOrElse(None), 8000)

# COMMAND ----------

def generate_driver_proxy_url(context: NotebookContext, cluster, port) -> dict:
    
    result = dict()
    if context.cloud == "azure":
      magic_number = int(context.workspace_id) % 20
      prefix = "https://adb"
      #new_url = f"{prefix}-{workspace_id}.{magic_number}.azuredatabricks.net/driver-proxy-api/o/{context.workspace_id}/{cluster_id}/{port}"
      result["targets"] = [f"{prefix}-{workspace_id}.{magic_number}.azuredatabricks.net"]
      result["labels"] = {
        "__port": str(port),
        "cluster_id": cluster.cluster_id,
        "workspace_id": str(context.workspace_id),
        "cluster_name": cluster.default_tags["ClusterName"],
        "creator_username": cluster.creator_user_name
      }
    elif context.cloud == "aws":
      #new_url = f"https://{context.host}/driver-proxy-api/o/{workspace_id}/{cluster_id}/{port}"
      result["targets"] = [context.host]
      result["labels"] = {
        "__port": str(port),
        "cluster_id": cluster.cluster_id,
        "workspace_id": str(context.workspace_id),
        "cluster_name": cluster.default_tags["ClusterName"],
        "creator_username": cluster.creator_user_name
      }
    return result
  
#generate_driver_proxy_url(context, dbutils.notebook.entry_point.getDbutils().notebook().getContext().clusterId().getOrElse(None), 8000)

# COMMAND ----------

import json

# TODO, add optional path var?
def output_to_json(targets: list[dict[str,any]]) -> None:

  with open('clusters.json', 'w') as f:
      json.dump(targets, f, indent=4)

# COMMAND ----------

import time

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.compute import State
import requests
from requests.auth import HTTPBasicAuth
import re

def scrape_metrics():
  url = f"https://{host}/api/2.0/cluster-metrics/metrics"

  running_clusters = (c for c in w.clusters.list() if is_running_cluster(c))
  cluster_ids = list(cluster.cluster_id for cluster in running_clusters)

  data = {
      "cluster_ids": cluster_ids,
      "metric_names": []
  }
  headers = {"Authorization": f"Bearer {token}"}


  response = requests.get(url, headers=headers, json=data)

  text = response.text

  # Split metrics into lines
  lines = text.split('\n')

  # Prepare pattern
  pattern = re.compile('# TYPE (.+?) (.+?)')

  # Transform lines using list comprehension
  lines = [
     re.sub('# TYPE (.+?) unknown', '# TYPE \\1 untyped', line)  # Change 'unknown' type to 'untyped'
     for line in lines
  ]

  # Join lines back into a single string
  metrics = '\n'.join(lines)

  with open("metrics.txt", "w") as f:
    f.write(metrics)
    

def is_running_cluster(cluster):
    return cluster.state in (
        State.RUNNING, # if you add a comma, Python interprets this as a tuple
    )

w = WorkspaceClient(host=api_url, token=token)

while True:
  running_clusters = (c for c in w.clusters.list() if is_running_cluster(c))
  targets = (generate_driver_proxy_url(context, cluster=cluster, port=40001) for cluster in running_clusters)
  output_to_json(list(targets))
  scrape_metrics()
  time.sleep(POLLING_INTERVAL)  # Wait for 5 seconds


# COMMAND ----------


