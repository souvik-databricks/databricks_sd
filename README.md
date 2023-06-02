# databricks_sd


### Usage


* First Configure with POST call to the `/configure` endpoint
```commandline
curl -X 'POST' \
  'http://127.0.0.1:8000/configure' \
  -H 'accept: application/json' \
  -H 'Content-Type: application/json' \
  -d '{
  "specs": [
    {
      "workspace_host": "https://cse2.cloud.databricks.com/?o=8194341531897276",
      "token": "dapiXXXXXXX",
      "endpoint_name": "cse2",
      "labels": {
        "additionalProp1": "string",
        "additionalProp2": "string",
        "additionalProp3": "string"
      }
    },
    {
      "workspace_host": "https://adb-984752964297111.11.azuredatabricks.net/?o=984752964297111",
      "token": "dapiXXXXXXX",
      "endpoint_name": "adb98",
      "labels": {
        "additionalProp1": "string",
        "additionalProp2": "string",
        "additionalProp3": "string"
      }
    }
  ]
}'
```

* Get configurations for service discovery in the newly configured GET endpoints
* And always hit the root page `/` for a list of all of the available GET endpoints
