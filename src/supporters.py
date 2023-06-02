from urllib.parse import urlparse, parse_qs

prefix_url_settings = {
    "aws": "https://dbc-dp-",
    "azure": "https://adb-dp-",
}
suffix_url_settings = {
    "aws": "cloud.databricks.com",
    "azure": "azuredatabricks.net",
}


def parse_url(url: str):
    parsed_url = urlparse(url)
    workspaceHost = parsed_url.scheme + "://" + parsed_url.netloc
    query_params = parse_qs(parsed_url.query)
    orgID = query_params.get('o')[0] if query_params.get('o') else None
    if "azuredatabricks.net" in parsed_url.netloc:
        split_host = parsed_url.netloc.split(".", 1)
        new_host = f"https://adb-dp-{orgID}.{split_host[1]}"
    elif "cloud.databricks.com" in parsed_url.netloc:
        split_host = parsed_url.netloc.split(".", 1)
        new_host = f"https://dbc-dp-{orgID}.{split_host[1]}"
    driver_proxy_url = f"{new_host}/driver-proxy/o/{orgID}"
    return driver_proxy_url, orgID, workspaceHost
