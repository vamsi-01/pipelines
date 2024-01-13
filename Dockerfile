FROM golang:latest
RUN apt-get update && apt-get install -y gcsfuse
CMD ["gcsfuse", "[BUCKET_NAME]", "/mount/path"]
