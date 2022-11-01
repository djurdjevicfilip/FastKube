If "%~1"=="-d" (
    kubectl delete --all pods
    kubectl delete --all deployments
)

kubectl create -f deployments/nginx.yaml

go build .
.\main.exe