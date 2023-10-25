FROM golang:latest

WORKDIR /go/src
COPY go.mod go.sum ./
RUN go mod download && go mod verify
COPY . .
RUN apt update
RUN apt-get install build-essential librdkafka-dev -y

RUN go build ./cmd/app/main.go -o ./build/app

# CMD [ "tail", "-f", "/dev/null" ]
CMD [ "go", "run", "./build/app" ]