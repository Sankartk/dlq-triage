ARG GO_VERSION=1.27.1


FROM node:22-alpine AS web
WORKDIR /web
COPY web/package.json web/package-lock.json ./
RUN npm ci
COPY web/ ./
RUN npm run build

FROM golang:${GO_VERSION}-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
COPY --from=web /web/dist ./web/dist
ARG VERSION=dev
RUN CGO_ENABLED=0 go build -trimpath -ldflags "-s -w -X main.version=${VERSION}" -o /out/dlq-triage ./cmd/dlq-triage

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/dlq-triage /dlq-triage
VOLUME /data
EXPOSE 8080
ENTRYPOINT ["/dlq-triage", "-config", "/etc/dlq-triage/config.json"]
