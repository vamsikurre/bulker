@REM docker buildx build --platform linux/arm64 -f sidecar.Dockerfile -t sidecarnew:1.0.1 .
docker build -f sidecar.Dockerfile -t sidecarnew:1.0.1 .
IF ERRORLEVEL 1 EXIT /B %ERRORLEVEL%
kind load docker-image sidecarnew:1.0.1