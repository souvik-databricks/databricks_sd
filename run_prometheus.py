# Databricks notebook source
# MAGIC %sh
# MAGIC cat << 'EOF' > prometheus.yml
# MAGIC <PASTE_HERE>
# MAGIC EOF
# MAGIC

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

workspace_id = dbutils.notebook.entry_point.getDbutils().notebook().getContext().workspaceId().getOrElse(None)
host = dbutils.notebook.entry_point.getDbutils().notebook().getContext().browserHostName().getOrElse(None)
def generate_html_link(url, text, new_tab=False):
    target = ' target="_blank"' if new_tab else ""
    return f'<a href="{url}"{target}>{text}</a>'
  
def generate_target_url(context):
  cluster_id = context.cluster_id
  displayHTML(generate_html_link(f"https://{host}/driver-proxy/o/{workspace_id}/{cluster_id}/9090/", "Prometheus (please wait)", )) 
generate_target_url(context)

# COMMAND ----------

# MAGIC %sh
# MAGIC #!/bin/bash
# MAGIC
# MAGIC # Prometheus version to be downloaded
# MAGIC PROM_VERSION="2.32.1"
# MAGIC
# MAGIC # Determine architecture
# MAGIC ARCH="$(uname -m)"
# MAGIC case "$ARCH" in
# MAGIC     "x86_64") ARCH="amd64";;
# MAGIC     "armv7l") ARCH="armv7";;
# MAGIC     "aarch64") ARCH="arm64";;
# MAGIC     *) echo "Unsupported architecture $ARCH"; exit 1;;
# MAGIC esac
# MAGIC
# MAGIC # Download and untar
# MAGIC curl -LO "https://github.com/prometheus/prometheus/releases/download/v${PROM_VERSION}/prometheus-${PROM_VERSION}.linux-${ARCH}.tar.gz" \
# MAGIC && tar xvf "prometheus-${PROM_VERSION}.linux-${ARCH}.tar.gz" \
# MAGIC && rm "prometheus-${PROM_VERSION}.linux-${ARCH}.tar.gz"
# MAGIC
# MAGIC echo "Prometheus downloaded and untarred successfully."
# MAGIC
# MAGIC # You can now run Prometheus with
# MAGIC ./prometheus-${PROM_VERSION}.linux-${ARCH}/prometheus

# COMMAND ----------


