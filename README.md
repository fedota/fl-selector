# FL Selector
Selector and Aggregator (SA) for the Federated Learning system

- Compile protobuf needed in fl-misc by `fl-proto.sh` script
- Create go modules dependencies files by `go mod init`
- Build the docker image:
`docker build -t fl-selector .`

- Run the container using:
`docker run --rm --name fl-selector -p 50051:50051 fl-selector`

- To inspect the container, open bash using:
`docker run -it fl-selector bash`