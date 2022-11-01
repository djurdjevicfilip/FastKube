package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"math/rand"
	"net/http"
	"os"
	"time"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

var listOnly bool

func main() {
	flag.BoolVar(&listOnly, "l", false, "List current annotations and exit")
	flag.Parse()

	prices := []string{"0.05", "0.10", "0.20"}
	colors := []string{"blue", "red", "red"}
	resp, err := http.Get("http://127.0.0.1:8001/api/v1/nodes")
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	if resp.StatusCode != 200 {
		fmt.Println("Invalid status code", resp.Status)
		os.Exit(1)
	}

	var nodes []*v1.Node
	decoder := json.NewDecoder(resp.Body)
	err = decoder.Decode(&nodes)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	if listOnly {
		for _, node := range nodes {
			price := node.Annotations["node/placement/resource"]
			color := node.Annotations["constraint/color"]
			fmt.Printf("%s %s %s\n", node.Name, price, color)
		}
		os.Exit(0)
	}

	rand.Seed(time.Now().Unix())
	for _, node := range nodes {
		price := prices[rand.Intn(len(prices))]
		color := colors[rand.Intn(len(colors))]
		annotations := map[string]string{
			"node/placement/resource": price,
			"constraint/color":        color,
		}
		patch := v1.Node{
			ObjectMeta: metav1.ObjectMeta{
				Annotations: annotations},
		}

		var b []byte
		body := bytes.NewBuffer(b)
		err := json.NewEncoder(body).Encode(patch)
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}

		url := "http://127.0.0.1:8001/api/v1/nodes/" + node.Name
		request, err := http.NewRequest("PATCH", url, body)
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
		request.Header.Set("Content-Type", "application/strategic-merge-patch+json")
		request.Header.Set("Accept", "application/json, */*")

		resp, err := http.DefaultClient.Do(request)
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}

		if resp.StatusCode != 200 {
			fmt.Println(err)
			os.Exit(1)
		}
		fmt.Printf("%s %s\n", node.Name, price)
	}
}
