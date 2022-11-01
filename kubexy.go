package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"

	log "github.com/sirupsen/logrus"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
)

var (
	apiHost           = "127.0.0.1:8001"
	bindingsEndpoint  = "/api/v1/namespaces/default/pods/%s/binding/"
	eventsEndpoint    = "/api/v1/namespaces/default/events"
	nodesEndpoint     = "/api/v1/nodes"
	podsEndpoint      = "/api/v1/pods"
	watchPodsEndpoint = "/api/v1/watch/pods"
)

// Kubernetes clientset contains the clients for each resource group
type Clientset struct {
	clientset *kubernetes.Clientset
}

// Creates and returns new Clientset struct
func CreateClientset() Clientset {

	rules := clientcmd.NewDefaultClientConfigLoadingRules()
	kubeconfig := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(rules, &clientcmd.ConfigOverrides{})
	config, err := kubeconfig.ClientConfig()

	if err != nil {
		log.Fatal(err)
	}

	clientset := kubernetes.NewForConfigOrDie(config)

	return Clientset{
		clientset: clientset,
	}
}

// Create Http request helper function
// We have to communicate with the Kubernetes API in order to get entity info and tell Kubernetes what to do
func CreateHttpRequest(methodType string, endpoint string, body *bytes.Buffer, rawQuery url.Values) *http.Request {
	if body != nil {
		// Post request
		return &http.Request{
			Body:          ioutil.NopCloser(body),
			ContentLength: int64(body.Len()),
			Header:        make(http.Header),
			Method:        methodType,
			URL: &url.URL{
				Host: apiHost,
				Path: endpoint,
				RawQuery: func() string {
					if rawQuery != nil {
						return rawQuery.Encode()
					} else {
						return ""
					}
				}(),
				Scheme: "http",
			},
		}
	} else {
		// Get request
		return &http.Request{
			Header: make(http.Header),
			Method: methodType,
			URL: &url.URL{
				Host: apiHost,
				Path: endpoint,
				RawQuery: func() string {
					if rawQuery != nil {
						return rawQuery.Encode()
					} else {
						return ""
					}
				}(),
				Scheme: "http",
			},
		}
	}
}

func EncodeJSON(binding Binding) *bytes.Buffer {
	var b []byte
	body := bytes.NewBuffer(b)
	err := json.NewEncoder(body).Encode(binding)

	if err != nil {
		return nil
	}

	return body
}

func BindPod(pod *v1.Pod, node *v1.Node) error {

	// Create Binding
	binding := Binding{
		ApiVersion: "v1",
		Kind:       "Binding",
		Metadata:   Metadata{Name: pod.Name},
		Target: Target{
			ApiVersion: "v1",
			Kind:       "Node",
			Name:       node.Name,
		},
	}

	body := EncodeJSON(binding)

	request := CreateHttpRequest(http.MethodPost, fmt.Sprintf(bindingsEndpoint, pod.Name), body, nil)

	if request == nil {
		return nil
	}

	resp, err := http.DefaultClient.Do(request)

	if err != nil {
		return err
	}

	if resp.StatusCode != 201 {
		return errors.New("Binding: Unexpected HTTP status code" + resp.Status)
	}

	return nil
}

// Utility function to bind a pod to a specific node
func (c *Clientset) BindPod(p *Pod, n Node) error {
	return c.clientset.CoreV1().Pods(p.Metadata.Namespace).Bind(context.Background(), &v1.Binding{
		ObjectMeta: metav1.ObjectMeta{
			Name: p.Metadata.Name,
		},
		Target: v1.ObjectReference{
			APIVersion: "v1",
			Kind:       "Node",
			Name:       n.Metadata.Name,
		},
	}, metav1.CreateOptions{})
}
