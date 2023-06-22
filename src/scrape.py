# Databricks notebook source
# MAGIC %pip install databricks-sdk~=0.1.10 ruamel.yaml

# COMMAND ----------

import threading
from dataclasses import dataclass, field
import time
import json
import re
import requests
from requests.auth import HTTPBasicAuth
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.compute import State
import logging
from prometheus_client import Summary, Counter, start_http_server as start_prometheus_http_server

# please do not go below 60
METRICS_POLLING_INTERVAL = 60

# please do not go below 30
TARGETS_POLLING_INTERVAL = 30

# Define the metrics
PROCESSING_TIME = Summary('processing_seconds', 'Time spent processing request', ['job'])
SCRAPE_STATUS = Counter('scrape_status_total', 'Scrape Status', ['job', 'status'])

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(module)s.%(funcName)s: %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
    handlers=[logging.StreamHandler()]
)

@dataclass
class NotebookContext:
    # The variables are initialized to None as we will provide the 
    # real values in the __post_init__ method. This also allows for 
    # optional parameters in the constructor.
    api_url: str = None
    token: str = None
    workspace_id: str = None
    host: str = None
    cluster_id: str = None
    
    # _cloud is a private attribute. We are making it private and 
    # providing a getter (but no setter) to enforce read-only access.
    _cloud: str = field(init=False, default=None)

    def __post_init__(self):
        # For each of the instance variables, if the user doesn't provide a value 
        # when creating an instance, we will call the appropriate getter to retrieve the value.
        self.api_url = self.api_url or self.get_api_url()
        self.token = self.token or self.get_token()
        self.workspace_id = self.workspace_id or self.get_workspace_id()
        self.host = self.host or self.get_host()
        self.cluster_id = self.cluster_id or self.get_cluster_id()
        
        # We calculate the _cloud attribute based on the provided host. This happens only once, 
        # at initialization, which makes it efficient as we avoid recalculating this attribute each time it's accessed.
        self._cloud = self.detect_cloud(self.host)

    @property
    def cloud(self):
        # We provide a public getter for the _cloud attribute. 
        # This is the only way to access the _cloud attribute from outside the class. 
        # By not providing a setter, we are making it read-only.
        return self._cloud

    def detect_cloud(self, host):
        # This method checks the host suffix to determine the cloud. 
        # If the host ends with a known cloud suffix, we return that cloud. 
        # If the host doesn't match any known cloud suffix, we raise an error.
        suffix_url_settings = {
            "aws": "cloud.databricks.com",
            "azure": "azuredatabricks.net",
        }

        for cloud, suffix in suffix_url_settings.items():
            if host and host.endswith(suffix):
                return cloud
        else:
            raise ValueError(f"Cloud not detected given host={host}")

    def get_api_url(self):
        return dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiUrl().getOrElse(None)

    def get_token(self):
        return dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().getOrElse(None)

    def get_workspace_id(self):
        return dbutils.notebook.entry_point.getDbutils().notebook().getContext().workspaceId().getOrElse(None)

    def get_host(self):
        return dbutils.notebook.entry_point.getDbutils().notebook().getContext().browserHostName().getOrElse(None)

    def get_cluster_id(self):
        return dbutils.notebook.entry_point.getDbutils().notebook().getContext().clusterId().getOrElse(None)

def scrape_metrics_to_text(w, context):
    start_time = time.time()
    try:
        url = f"https://{context.host}/api/2.0/cluster-metrics/metrics"
        running_clusters = (c for c in w.clusters.list() if is_running_cluster(c))
        cluster_ids = list(cluster.cluster_id for cluster in running_clusters)
        data = {
            "cluster_ids": cluster_ids,
            "metric_names": []
        }
        headers = {"Authorization": f"Bearer {context.token}"}

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

        with open("cluster_metrics.txt", "w") as f:
            f.write(metrics)

        elapsed_time = time.time() - start_time
        PROCESSING_TIME.labels('cluster_metrics').observe(elapsed_time)
        SCRAPE_STATUS.labels('cluster_metrics', 'success').inc()
        logging.info(f"Successfully scraped cluster metrics in {elapsed_time}")
    except Exception as e:
        elapsed_time = time.time() - start_time
        PROCESSING_TIME.labels('cluster_metrics').observe(elapsed_time)
        SCRAPE_STATUS.labels('cluster_metrics', 'failure').inc()
        logging.error(f"An error occurred while scraping cluster metrics: {str(e)} in {elapsed_time}")

def generate_driver_proxy_url(context: NotebookContext, cluster, port) -> dict:
    result = dict()
    if context.cloud == "azure":
      result["targets"] = [context.host]
      result["labels"] = {
        "__port": str(port),
        "cluster_id": cluster.cluster_id,
        "workspace_id": str(context.workspace_id),
        "cluster_name": cluster.default_tags["ClusterName"],
        "creator_username": cluster.creator_user_name
      }
    elif context.cloud == "aws":
      result["targets"] = [context.host]
      result["labels"] = {
        "__port": str(port),
        "cluster_id": cluster.cluster_id,
        "workspace_id": str(context.workspace_id),
        "cluster_name": cluster.default_tags["ClusterName"],
        "creator_username": cluster.creator_user_name
      }
    return result

def output_to_json_file(targets: list[dict[str,any]]) -> None:
    with open('clusters.json', 'w') as f:
        json.dump(targets, f, indent=4)

def is_running_cluster(cluster):
    return cluster.state in (
        State.RUNNING,  # if you add a comma, Python interprets this as a tuple
    )

def process_targets(context, w):
    while True:
        start_time = time.time()
        try:
            running_clusters = (c for c in w.clusters.list() if is_running_cluster(c))
            targets = (generate_driver_proxy_url(context, cluster=cluster, port=40001) for cluster in running_clusters)
            output_to_json_file(list(targets))
            elapsed_time = time.time() - start_time
            PROCESSING_TIME.labels('process_targets').observe(elapsed_time)
            SCRAPE_STATUS.labels('process_targets', 'success').inc()
            logging.info(f"Successfully processed targets in {elapsed_time}")
        except Exception as e:
            elapsed_time = time.time() - start_time
            PROCESSING_TIME.labels('process_targets').observe(elapsed_time)
            SCRAPE_STATUS.labels('process_targets', 'failure').inc()
            logging.error(f"An error occurred while processing targets: {str(e)} in {elapsed_time}")
        finally:
            time.sleep(TARGETS_POLLING_INTERVAL)

def scrape_metrics(context, w):
    while True:
        scrape_metrics_to_text(w, context)
        time.sleep(METRICS_POLLING_INTERVAL)

def main():
    context = NotebookContext()
    w = WorkspaceClient() # we use notebook built-in auth, starting in SDK v0.1.10

    start_prometheus_http_server(port=8001)
    targets_thread = threading.Thread(target=process_targets, args=(context, w), daemon=True)
    metrics_thread = threading.Thread(target=scrape_metrics, args=(context, w), daemon=True)

    targets_thread.start()
    metrics_thread.start()

    targets_thread.join()
    metrics_thread.join()

if __name__ == '__main__':
    main()

# COMMAND ----------

w = WorkspaceClient()
w.serving_endpoints.export_metrics(w.serving_endpoints.list()[0].name)

# COMMAND ----------

endpoints = w.serving_endpoints.list()

# COMMAND ----------

endpoints[0].name

# COMMAND ----------


