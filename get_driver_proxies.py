import argparse
from databricks_cli.sdk.api_client import ApiClient
from databricks_cli.clusters.api import ClusterApi
import json

parser = argparse.ArgumentParser(description="Databricks Service Discovery Application")

parser.add_argument('-t','--token', help='PAT for authentication against workspace', required=True)
parser.add_argument('-w','--workspace', help='Workspace instance name', required=True)
parser.add_argument('-p','--port', help='Port on which databricks prometheus endpoint has been exposed', required=True)
args = vars(parser.parse_args())

token = args["token"]
instancename = args["workspace"]

port = args["port"]

api_client = ApiClient(
    host=f"https://{instancename}",
    token=token
)

clusters_api = ClusterApi(api_client)

clusters_list = clusters_api.list_clusters()
clusters_list = [{"cluster_id": i.get("cluster_id")} for i in clusters_list["clusters"] if i.get("driver") is not None]

if clusters_list:
    targets = []
    for i in clusters_list:
        c_id = i.get("cluster_id")
        targets.append(f"https://{instancename}/driver-proxy/o/0/{c_id}/{port}/")

json_file = json.dumps([{
    "labels": {
        "workspace": f"{instancename}",
    },
    "targets": targets
}])


with open("sample.json", "w") as outfile:
    outfile.write(json_file)
