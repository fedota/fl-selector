ARG GO_SELECTOR_PATH="/go/src/federated-learning/fl-selector"

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
FROM tensorflow/tensorflow:latest-py3 AS final

RUN pip3 install --upgrade keras

ARG GO_SELECTOR_PATH
WORKDIR ${GO_SELECTOR_PATH}

# Copy from builder the GO executable file
COPY --from=builder ${GO_SELECTOR_PATH}/server .
COPY --from=builder ${GO_SELECTOR_PATH}/config.yaml .

# Execute the program upon start 
CMD [ "./server" ]