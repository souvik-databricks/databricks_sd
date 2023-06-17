# Databricks notebook source
# MAGIC %pip install databricks-sdk ruamel.yaml

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# import threading
# from dataclasses import dataclass
# import time
# import json
# import re
# import requests
# from requests.auth import HTTPBasicAuth
# from databricks.sdk import WorkspaceClient
# from databricks.sdk.service.compute import State
# import logging


# # please do not go below 60
# METRICS_POLLING_INTERVAL = 60
# # please do not go below 30
# TARGETS_POLLING_INTERVAL = 30

# @dataclass
# class NotebookContext:
#     api_url: str
#     token: str
#     workspace_id: str
#     host: str
#     cluster_id: str
#     cloud: str = None

#     def __post_init__(self):
#         self.cloud = self.detect_cloud(self.host)

#     def detect_cloud(self, host):
#         suffix_url_settings = {
#             "aws": "cloud.databricks.com",
#             "azure": "azuredatabricks.net",
#         }

#         for cloud, suffix in suffix_url_settings.items():
#             if host and host.endswith(suffix):
#                 return cloud
#         else:
#             raise ValueError(f"Cloud not detected given host={host}")


# def get_notebook_context():
#     context = NotebookContext(
#         api_url = get_api_url(),
#         token = get_token(),
#         workspace_id = get_workspace_id(),
#         host = get_host(),
#         cluster_id = get_cluster_id()
#     )
#     return context


# def get_api_url():
#     return dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiUrl().getOrElse(None)


# def get_token():
#     return dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().getOrElse(None)


# def get_workspace_id():
#     return dbutils.notebook.entry_point.getDbutils().notebook().getContext().workspaceId().getOrElse(None)


# def get_host():
#     return dbutils.notebook.entry_point.getDbutils().notebook().getContext().browserHostName().getOrElse(None)


# def get_cluster_id():
#     return dbutils.notebook.entry_point.getDbutils().notebook().getContext().clusterId().getOrElse(None)

# def cluster_metrics_to_text(w, context):
#     url = f"https://{context.host}/api/2.0/cluster-metrics/metrics"
#     running_clusters = (c for c in w.clusters.list() if is_running_cluster(c))
#     cluster_ids = list(cluster.cluster_id for cluster in running_clusters)
#     data = {
#         "cluster_ids": cluster_ids,
#         "metric_names": []
#     }
#     headers = {"Authorization": f"Bearer {context.token}"}

#     response = requests.get(url, headers=headers, json=data)
#     text = response.text

#     # Split metrics into lines
#     lines = text.split('\n')

#     # Prepare pattern
#     pattern = re.compile('# TYPE (.+?) (.+?)')

#     # Transform lines using list comprehension
#     lines = [
#         re.sub('# TYPE (.+?) unknown', '# TYPE \\1 untyped', line)  # Change 'unknown' type to 'untyped'
#         for line in lines
#     ]

#     # Join lines back into a single string
#     metrics = '\n'.join(lines)

#     with open("cluster_metrics.txt", "w") as f:
#         f.write(metrics)

        
# def generate_driver_proxy_url(context: NotebookContext, cluster, port) -> dict:
    
#     result = dict()
#     if context.cloud == "azure":
#       magic_number = int(context.workspace_id) % 20
#       prefix = "https://adb"
#       result["targets"] = [f"{prefix}-{workspace_id}.{magic_number}.azuredatabricks.net"]
#       result["labels"] = {
#         "__port": str(port),
#         "cluster_id": cluster.cluster_id,
#         "workspace_id": str(context.workspace_id),
#         "cluster_name": cluster.default_tags["ClusterName"],
#         "creator_username": cluster.creator_user_name
#       }
#     elif context.cloud == "aws":
#       result["targets"] = [context.host]
#       result["labels"] = {
#         "__port": str(port),
#         "cluster_id": cluster.cluster_id,
#         "workspace_id": str(context.workspace_id),
#         "cluster_name": cluster.default_tags["ClusterName"],
#         "creator_username": cluster.creator_user_name
#       }
#     return result
        
        
# def output_to_json_file(targets: list[dict[str,any]]) -> None:
#     with open('clusters.json', 'w') as f:
#         json.dump(targets, f, indent=4)


# def is_running_cluster(cluster):
#     return cluster.state in (
#         State.RUNNING,  # if you add a comma, Python interprets this as a tuple
#     )

# def driver_proxy_targets(context, w):
#     while True:
#         try:
#             running_clusters = (c for c in w.clusters.list() if is_running_cluster(c))
#             targets = (generate_driver_proxy_url(context, cluster=cluster, port=40001) for cluster in running_clusters)
#             output_to_json_file(list(targets))
#         except Exception as e:
#             logging.error(f"An error occurred while processing targets: {str(e)}")
#         finally:
#             time.sleep(TARGETS_POLLING_INTERVAL)

# def cluster_metrics(context, w):
#     while True:
#         try:
#             cluster_metrics_to_text(w, context)
#         except Exception as e:
#             logging.error(f"An error occurred while scraping metrics: {str(e)}")
#         finally:
#             time.sleep(METRICS_POLLING_INTERVAL)


# def main():
#     context = get_notebook_context()
#     w = WorkspaceClient(host=context.api_url, token=context.token)

#     targets_thread = threading.Thread(target=driver_proxy_targets, args=(context, w), daemon=True)
#     metrics_thread = threading.Thread(target=cluster_metrics, args=(context, w), daemon=True)

#     targets_thread.start()
#     metrics_thread.start()

#     targets_thread.join()
#     metrics_thread.join()

# if __name__ == '__main__':
#     main()


# COMMAND ----------

# import threading
# from dataclasses import dataclass
# import time
# import json
# import re
# import requests
# from requests.auth import HTTPBasicAuth
# from databricks.sdk import WorkspaceClient
# from databricks.sdk.service.compute import State
# import logging
# from prometheus_client import Counter, Summary, start_http_server

# # please do not go below 60
# METRICS_POLLING_INTERVAL = 60
# # please do not go below 30
# TARGETS_POLLING_INTERVAL = 30

# # Create metrics to track time spent and requests made.
# CLUSTER_METRICS_TIME = Summary('cluster_metrics_processing_seconds', '')
# CLUSTER_METRICS_SUCCESS_COUNT = Counter('cluster_metrics_requests_success__total', '')
# CLUSTER_METRICS_FAILURE_COUNT = Counter('cluster_metrics_failure_total', '')

# DRIVER_PROXY_TARGETS_TIME = Summary('driver_proxy_targets_processing_seconds', '')
# DRIVER_PROXY_TARGETS_SUCCESS_COUNT = Counter('driver_proxy_targets_success_total', '')
# DRIVER_PROXY_TARGETS_FAILURE_COUNT = Counter('driver_proxy_targets_failure_total', '')

# @dataclass
# class NotebookContext:
#     api_url: str
#     token: str
#     workspace_id: str
#     host: str
#     cluster_id: str
#     cloud: str = None

#     def __post_init__(self):
#         self.cloud = self.detect_cloud(self.host)

#     def detect_cloud(self, host):
#         suffix_url_settings = {
#             "aws": "cloud.databricks.com",
#             "azure": "azuredatabricks.net",
#         }

#         for cloud, suffix in suffix_url_settings.items():
#             if host and host.endswith(suffix):
#                 return cloud
#         else:
#             raise ValueError(f"Cloud not detected given host={host}")


# def get_notebook_context():
#     context = NotebookContext(
#         api_url = get_api_url(),
#         token = get_token(),
#         workspace_id = get_workspace_id(),
#         host = get_host(),
#         cluster_id = get_cluster_id()
#     )
#     return context


# def get_api_url():
#     return dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiUrl().getOrElse(None)


# def get_token():
#     return dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().getOrElse(None)


# def get_workspace_id():
#     return dbutils.notebook.entry_point.getDbutils().notebook().getContext().workspaceId().getOrElse(None)


# def get_host():
#     return dbutils.notebook.entry_point.getDbutils().notebook().getContext().browserHostName().getOrElse(None)


# def get_cluster_id():
#     return dbutils.notebook.entry_point.getDbutils().notebook().getContext().clusterId().getOrElse(None)

# def cluster_metrics_to_text(w, context):
#     url = f"https://{context.host}/api/2.0/cluster-metrics/metrics"
#     running_clusters = (c for c in w.clusters.list() if is_running_cluster(c))
#     cluster_ids = list(cluster.cluster_id for cluster in running_clusters)
#     data = {
#         "cluster_ids": cluster_ids,
#         "metric_names": []
#     }
#     headers = {"Authorization": f"Bearer {context.token}"}

#     response = requests.get(url, headers=headers, json=data)
#     text = response.text

#     # Split metrics into lines
#     lines = text.split('\n')

#     # Prepare pattern
#     pattern = re.compile('# TYPE (.+?) (.+?)')

#     # Transform lines using list comprehension
#     lines = [
#         re.sub('# TYPE (.+?) unknown', '# TYPE \\1 untyped', line)  # Change 'unknown' type to 'untyped'
#         for line in lines
#     ]

#     # Join lines back into a single string
#     metrics = '\n'.join(lines)

#     with open("cluster_metrics.txt", "w") as f:
#         f.write(metrics)

        
# def generate_driver_proxy_url(context: NotebookContext, cluster, port) -> dict:
    
#     result = dict()
#     if context.cloud == "azure":
#       magic_number = int(context.workspace_id) % 20
#       prefix = "https://adb"
#       result["targets"] = [f"{prefix}-{workspace_id}.{magic_number}.azuredatabricks.net"]
#       result["labels"] = {
#         "__port": str(port),
#         "cluster_id": cluster.cluster_id,
#         "workspace_id": str(context.workspace_id),
#         "cluster_name": cluster.default_tags["ClusterName"],
#         "creator_username": cluster.creator_user_name
#       }
#     elif context.cloud == "aws":
#       result["targets"] = [context.host]
#       result["labels"] = {
#         "__port": str(port),
#         "cluster_id": cluster.cluster_id,
#         "workspace_id": str(context.workspace_id),
#         "cluster_name": cluster.default_tags["ClusterName"],
#         "creator_username": cluster.creator_user_name
#       }
#     return result
        
        
# def output_to_json_file(targets: list[dict[str,any]]) -> None:
#     with open('clusters.json', 'w') as f:
#         json.dump(targets, f, indent=4)


# def is_running_cluster(cluster):
#     return cluster.state in (
#         State.RUNNING,  # if you add a comma, Python interprets this as a tuple
#     )
# @CLUSTER_METRICS_TIME.time()
# def cluster_metrics(context, w):
    
#     while True:
#         try:
#             cluster_metrics_to_text(w, context)
#             CLUSTER_METRICS_SUCCESS_COUNT.inc()
#         except Exception as e:
#             logging.error(f"An error occurred while scraping metrics: {str(e)}")
#             CLUSTER_METRICS_FAILURE_COUNT.inc()
#         finally:
#           time.sleep(METRICS_POLLING_INTERVAL)

# @DRIVER_PROXY_TARGETS_TIME.time()
# def driver_proxy_targets(context, w):
    
#     while True:
#         try:
#             running_clusters = (c for c in w.clusters.list() if is_running_cluster(c))
#             targets = (generate_driver_proxy_url(context, cluster=cluster, port=40001) for cluster in running_clusters)
#             output_to_json_file(list(targets))
#             DRIVER_PROXY_TARGETS_SUCCESS_COUNT.inc()
#         except Exception as e:
#             DRIVER_PROXY_TARGETS_FAILURE_COUNT.inc()
#             logging.error(f"An error occurred while processing targets: {str(e)}")
#         finally:
          
#           time.sleep(TARGETS_POLLING_INTERVAL)

# def main():
#     # Start up the server to expose the metrics.
#     start_http_server(8001)
#     context = get_notebook_context()
#     w = WorkspaceClient(host=context.api_url, token=context.token)

#     targets_thread = threading.Thread(target=driver_proxy_targets, args=(context, w), daemon=True)
#     metrics_thread = threading.Thread(target=cluster_metrics, args=(context, w), daemon=True)

#     targets_thread.start()
#     metrics_thread.start()

#     targets_thread.join()
#     metrics_thread.join()

# if __name__ == '__main__':
#   main()


# COMMAND ----------

import threading
from dataclasses import dataclass
import time
import json
import re
import requests
from requests.auth import HTTPBasicAuth
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.compute import State
import logging
from prometheus_client import Summary, Counter
import prometheus_client

# please do not go below 60
METRICS_POLLING_INTERVAL = 60
# please do not go below 30
TARGETS_POLLING_INTERVAL = 30

# Define the metrics
PROCESSING_TIME = Summary('processing_seconds', 'Time spent processing request', ['job'])
SCRAPE_STATUS = Counter('scrape_status_total', 'Scrape Status', ['job', 'status'])

# Setup logging
logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(module)s:%(filename)s:%(lineno)d %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
    level=logging.INFO,
)


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


def get_notebook_context():
    context = NotebookContext(
        api_url = get_api_url(),
        token = get_token(),
        workspace_id = get_workspace_id(),
        host = get_host(),
        cluster_id = get_cluster_id()
    )
    return context


def get_api_url():
    return dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiUrl().getOrElse(None)


def get_token():
    return dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().getOrElse(None)


def get_workspace_id():
    return dbutils.notebook.entry_point.getDbutils().notebook().getContext().workspaceId().getOrElse(None)


def get_host():
    return dbutils.notebook.entry_point.getDbutils().notebook().getContext().browserHostName().getOrElse(None)


def get_cluster_id():
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
        logging.info("Successfully scraped cluster metrics.")
    except Exception as e:
        elapsed_time = time.time() - start_time
        PROCESSING_TIME.labels('cluster_metrics').observe(elapsed_time)
        SCRAPE_STATUS.labels('cluster_metrics', 'failure').inc()
        logging.error(f"An error occurred while scraping cluster metrics: {str(e)}")

def generate_driver_proxy_url(context: NotebookContext, cluster, port) -> dict:
    result = dict()
    if context.cloud == "azure":
      magic_number = int(context.workspace_id) % 20
      prefix = "https://adb"
      result["targets"] = [f"{prefix}-{workspace_id}.{magic_number}.azuredatabricks.net"]
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
            logging.info("Successfully processed targets.")
        except Exception as e:
            elapsed_time = time.time() - start_time
            PROCESSING_TIME.labels('process_targets').observe(elapsed_time)
            SCRAPE_STATUS.labels('process_targets', 'failure').inc()
            logging.error(f"An error occurred while processing targets: {str(e)}")
        finally:
            time.sleep(TARGETS_POLLING_INTERVAL)

def scrape_metrics(context, w):
    while True:
        scrape_metrics_to_text(w, context)
        time.sleep(METRICS_POLLING_INTERVAL)

def main():
    context = get_notebook_context()
    w = WorkspaceClient(host=context.api_url, token=context.token)

    prometheus_client.start_http_server(8001)
    targets_thread = threading.Thread(target=process_targets, args=(context, w), daemon=True)
    metrics_thread = threading.Thread(target=scrape_metrics, args=(context, w), daemon=True)

    targets_thread.start()
    metrics_thread.start()

    targets_thread.join()
    metrics_thread.join()

    # Start up the server to expose the metrics.
    

if __name__ == '__main__':
    main()


# COMMAND ----------


