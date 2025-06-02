FROM golang:1.21
WORKDIR /app
COPY go.mod ./  # e go.sum
RUN go mod download
COPY . .
RUN go build -o main ./cmd
EXPOSE 8080
CMD ["./main"]
