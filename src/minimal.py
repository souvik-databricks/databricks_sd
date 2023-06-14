# Databricks notebook source
# MAGIC %pip install databricks-sdk

# COMMAND ----------

SD_TARGET_PORT = 8000

# COMMAND ----------

host = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiUrl().getOrElse(None)
token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().getOrElse(None)

# COMMAND ----------

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.compute import State

def is_running_cluster(cluster):
    return cluster.state in (
        State.RUNNING, # if you add a comma, Python interprets it as a tuple
    )

w = WorkspaceClient(host=host, token=token)

running_clusters = (c for c in w.clusters.list() if is_running_cluster(c))

# COMMAND ----------

def construct_driverproxy_url(host, cluster_id, port, endpoint):
  return f"{host}/driver-proxy-api/{cluster_id}/{port}/{endpoint}"

# COMMAND ----------

urls = [construct_driverproxy_url(host, cluster.cluster_id, 80, "metrics/json") for cluster in running_clusters]
urls

# COMMAND ----------


