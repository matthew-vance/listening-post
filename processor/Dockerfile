FROM golang:1.27-alpine AS build
WORKDIR /src
COPY go.mod ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -ldflags="-s -w" -o /processor .

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /processor /processor
ENTRYPOINT ["/processor"]
