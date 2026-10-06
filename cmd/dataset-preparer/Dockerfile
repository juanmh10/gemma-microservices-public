ARG GO_BASE_IMAGE=golang@sha256:40dfc169bd5ad8a8617e49c8ead7fe16c6873e79d6937539e9c2e5947b7984ef
ARG RUNTIME_BASE_IMAGE=gcr.io/distroless/static-debian12@sha256:52dcfbabb7457ea47c82f6e13af8c8a4a1d9f7b0145142b3ecab20f2b888411d
FROM ${GO_BASE_IMAGE} AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY cmd/dataset-preparer ./cmd/dataset-preparer
COPY internal/gcs ./internal/gcs
COPY internal/preparation ./internal/preparation
RUN CGO_ENABLED=0 go build -trimpath -o /dataset-preparer ./cmd/dataset-preparer

FROM ${RUNTIME_BASE_IMAGE}
COPY --from=build /dataset-preparer /dataset-preparer
ARG GIT_SHA
LABEL org.opencontainers.image.revision=${GIT_SHA}
ENTRYPOINT ["/dataset-preparer"]
