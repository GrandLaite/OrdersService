FROM golang:1.23 as builder

WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download

COPY . ./
RUN CGO_ENABLED=0 GOOS=linux go build -o main ./cmd/main.go

FROM alpine:3.18
WORKDIR /app

COPY --from=builder /app/main .
COPY --from=builder /app/config.env .
COPY --from=builder /app/db/migrations ./db/migrations

EXPOSE 8080
CMD ["./main"]
