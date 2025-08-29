@REM docker buildx build --platform linux/amd64 -f syncctl.Dockerfile -t syncctlnew:1.0.1 .
docker build -f syncctl.Dockerfile -t syncctlnew:1.0.1 .
IF ERRORLEVEL 1 EXIT /B %ERRORLEVEL%
kind load docker-image syncctlnew:1.0.1