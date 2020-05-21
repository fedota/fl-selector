ARG GO_SELECTOR_PATH="/go/src/fedota/fl-selector"

################## 1st Build Stage ####################
FROM golang:alpine AS builder
LABEL stage=builder

ARG GO_SELECTOR_PATH

# Adding source files
ADD . ${GO_SELECTOR_PATH}
WORKDIR ${GO_SELECTOR_PATH}

ENV GO111MODULE=on

# Cache go mods based on go.sum/go.mod files
RUN go mod download

# Build the GO program
RUN CGO_ENABLED=0 GOOS=linux go build -a -o server

################## 2nd Build Stage ####################
FROM tensorflow/tensorflow:latest-py3

# Install dependencies
RUN pip3 install keras

RUN mkdir -p /server_dir

ARG GO_SELECTOR_PATH

# Copy from builder the GO executable file
COPY --from=builder ${GO_SELECTOR_PATH}/server /server_dir
COPY --from=builder ${GO_SELECTOR_PATH}/config.yaml /server_dir
COPY --from=builder ${GO_SELECTOR_PATH}/mid_averaging.py /server_dir

WORKDIR /server_dir

# Execute the program upon start 
CMD [ "./server" ]