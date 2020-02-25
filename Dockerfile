# Development
FROM golang:1.13.8-alpine AS development
WORKDIR /go/src/github.com/tidepool-org/mongoproxy
RUN adduser -D tidepool && \
    chown -R tidepool /go/src/github.com/tidepool-org/mongoproxy
USER tidepool
ENV GO111MODULE=on
COPY --chown=tidepool . .
RUN go build -o mongoproxy mongoproxy.go
CMD ["./mongoproxy"]

# Production
FROM alpine:latest AS production
WORKDIR /home/tidepool
RUN apk --no-cache update && \
    apk --no-cache upgrade && \
    apk add --no-cache ca-certificates && \
    adduser -D tidepool
USER tidepool
COPY --from=development --chown=tidepool /go/src/github.com/tidepool-org/mongoproxy/mongoproxy .
CMD ["./mongoproxy"]
