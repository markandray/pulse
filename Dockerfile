# ---- build stage ----
FROM golang:1.27.2 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /out/pulse ./cmd/server

# ---- runtime stage ----
FROM gcr.io/distroless/static-debian12:nonroot
WORKDIR /app
COPY --from=build /out/pulse /app/pulse
COPY web /app/web
ENV WEB_DIR=/app/web
EXPOSE 8080
ENTRYPOINT ["/app/pulse"]
