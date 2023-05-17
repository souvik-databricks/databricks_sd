package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"strings"
)

type Cluster struct {
	ClusterID string `json:"cluster_id"`
	Driver    string `json:"driver"`
}

type Clusters struct {
	Clusters []Cluster `json:"clusters"`
}

type Target struct {
	Labels  map[string]string `json:"labels"`
	Targets []string          `json:"targets"`
}

func main() {
	token := flag.String("t", "", "PAT for authentication against workspace")
	workspace := flag.String("w", "", "Workspace instance name")
	port := flag.String("p", "", "Port on which databricks prometheus endpoint has been exposed")

	flag.Parse()

	orgID := strings.Split(strings.Split(*workspace, ".")[0], "-")[1]

	url := fmt.Sprintf("https://%s/api/2.0/clusters/list", *workspace)

	req, _ := http.NewRequest("GET", url, nil)
	req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", *token))
	res, _ := http.DefaultClient.Do(req)
	defer res.Body.Close()

	body, _ := ioutil.ReadAll(res.Body)

	clusters := Clusters{}
	json.Unmarshal(body, &clusters)

	targets := make([]string, 0)

	for _, cluster := range clusters.Clusters {
		if cluster.Driver != "" {
			target := fmt.Sprintf("https://%s/driver-proxy-api/o/%s/%s/%s/metrics/json/", *workspace, orgID, cluster.ClusterID, *port)
			targets = append(targets, target)
		}
	}

	targetJson := []Target{
		{
			Labels: map[string]string{
				"workspace": *workspace,
			},
			Targets: targets,
		},
	}

	file, _ := json.MarshalIndent(targetJson, "", " ")

	_ = ioutil.WriteFile("sample.json", file, 0644)
}
