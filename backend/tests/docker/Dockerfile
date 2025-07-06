FROM golang:1.23-alpine

WORKDIR /app

# Copy go.mod and go.sum first for better caching
COPY go.mod go.sum ./
RUN go mod download

# Copy the entire backend directory
COPY pkg /app/pkg
COPY cmd /app/cmd
COPY internal /app/internal

# Default command (can be overridden)
CMD ["go", "run", "./cmd"] 