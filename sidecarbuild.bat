docker buildx build --platform linux/arm64 -f sidecar.Dockerfile -t sidecarnew:1.0.1 .

kind load docker-image sidecarnew:1.0.1