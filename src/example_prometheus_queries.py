# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC We encourage you to try them and see if you can grok the syntax, otherwise, use an LLM for an explanation. 
# MAGIC
# MAGIC Remember that adding the phrase "please be concise" to the end of a prompt can save a lot of time.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC
# MAGIC __\# Major Spark versions__
# MAGIC
# MAGIC count by (major_version) (label_replace(spark_info, "major_version", "$1", "version", "([0-9])\\..*"))
# MAGIC
# MAGIC
# MAGIC __\# Major-minor Spark versions__
# MAGIC
# MAGIC count by (major_minor_version) (label_replace(spark_info, "major_minor_version", "$1", "version", "([0-9]+\\.[0-9]+)\\..*"))
# MAGIC
# MAGIC __\# Major-minor-patch Spark versions__
# MAGIC
# MAGIC count by (version) (spark_info)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC __\# Task failures - rate__
# MAGIC
# MAGIC sum(rate(metrics_executor_failedTasks_total[5m])) by (cluster_id)

# COMMAND ----------

# MAGIC %md
# MAGIC __\# Number of cores per cluster__
# MAGIC
# MAGIC sum(metrics_executor_totalCores) by (cluster_id)

# COMMAND ----------


