# Databricks notebook source
# MAGIC %pip install databricks-sdk fastapi uvicorn nest_asyncio

# COMMAND ----------

SD_TARGET_PORT = 8000
POLLING_INTERVAL = 15

# COMMAND ----------

host = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiUrl().getOrElse(None)
token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().getOrElse(None)
workspace_id = dbutils.notebook.entry_point.getDbutils().notebook().getContext().workspaceId().getOrElse(None)

# COMMAND ----------

def detect_cloud(host):

    suffix_url_settings = {
        "aws": "cloud.databricks.com",
        "azure": "azuredatabricks.net",
    }

    for cloud, suffix in suffix_url_settings.items():
        if host.endswith(suffix):
            return cloud
    else:
        raise ValueError(f"Cloud not detected given host={host}")


def get_url_prefix(cloud):
    cloud_prefix_mapping = {
        "aws": "https://dbc-dp-",
        "azure": "https://adb",
    }
    return cloud_prefix_mapping[cloud]


def generate_driver_proxy_url(host, workspace_id, cluster_id, port, endpoint):
    magic_number = int(workspace_id) % 20
    
    cloud = detect_cloud(host)
    prefix = get_url_prefix(cloud)

    new_url = f"{prefix}-{workspace_id}.{magic_number}.azuredatabricks.net/driver-proxy-api/o/{workspace_id}/{cluster_id}/{port}/{endpoint}"
    return new_url


api_endpoint = generate_driver_proxy_url(
    host, workspace_id, cluster_id, 40001, "metrics/json"
)
print(api_endpoint)

# COMMAND ----------

import requests

url = api_endpoint

headers = {"Authorization": f"Bearer {token}"}

response = requests.get(url, headers=headers)

print(response.json())

# COMMAND ----------

import json

def output_to_json(urls):

  url_list = list(urls)  # Your list of URLs

  label_dict = {}  # Your label dictionary

  payload = [
      {
          "targets": url_list,
          "labels": label_dict
      }
  ]

  #json_payload = json.dumps(payload)  # Convert the payload to a JSON string


  with open('payload.json', 'w') as f:
      json.dump(payload, f, indent=4)  # Use indent for pretty-printing

# COMMAND ----------

import time

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.compute import State

def is_running_cluster(cluster):
    return cluster.state in (
        State.RUNNING, # if you add a comma, Python interprets it as a tuple
    )

w = WorkspaceClient(host=host, token=token)

while True:
  running_clusters = (c for c in w.clusters.list() if is_running_cluster(c))
  urls = (generate_driver_proxy_url(host=host, workspace_id=workspace_id, cluster_id=cluster.cluster_id, port=40001, endpoint="metrics/json") for cluster in running_clusters)
  output_to_json(urls)
  time.sleep(POLLING_INTERVAL)  # Wait for 5 seconds


# COMMAND ----------

import time

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.compute import State

def is_running_cluster(cluster):
    return cluster.state in (
        State.RUNNING, # if you add a comma, Python interprets it as a tuple
    )

w = WorkspaceClient(host=host, token=token)

running_clusters = (c for c in w.clusters.list() if is_running_cluster(c))
cluster_ids = (cluster.cluster_id for cluster in running_clusters)
#print(list(cluster_ids))


# COMMAND ----------

import requests
from requests.auth import HTTPBasicAuth

#url = f"https://{host}/api/2.0/cluster-metrics/metrics"
url = "https://adb-984752964297111.11.azuredatabricks.net/api/2.0/cluster-metrics/metrics"
data = {
    "cluster_ids": list(cluster_ids),
    "metric_names": []
}

headers = {"Authorization": f"Bearer {token}"}


response = requests.get(url, headers=headers, json=data)

print(response.json())


# COMMAND ----------

print(url)

# COMMAND ----------


