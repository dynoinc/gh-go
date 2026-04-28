# Build stage
FROM golang:1.26-alpine AS builder
WORKDIR /src
COPY . .
RUN CGO_ENABLED=0 go build -ldflags="-s -w" -trimpath -o /bin/frontend ./cmd/frontend

# Production stage
FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=builder /bin/frontend /bin/frontend
EXPOSE 5051
ENTRYPOINT ["/bin/frontend"]
