FROM golang:1.21.6-alpine3.19 as build
RUN apk --update add ca-certificates wget

WORKDIR /

RUN wget -O /ocb https://github.com/open-telemetry/opentelemetry-collector/releases/download/cmd%2Fbuilder%2Fv0.92.0/ocb_0.92.0_linux_amd64 && \
    chmod +x /ocb

COPY /pgtraceconnector /pgtraceconnector
COPY builder-config.yaml /manifest.yaml
RUN CGO_ENABLED=0 /ocb --config /manifest.yaml

FROM scratch

ARG USER_UID=10001
USER ${USER_UID}

COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt

COPY --from=build /otelcol-dev/otelcol-dev-bin /otelcol-dev-bin

COPY otelcol-config.yaml /etc/otelcol-contrib/config.yaml
ENTRYPOINT ["/otelcol-dev-bin"]
CMD ["--config", "/etc/otelcol-contrib/config.yaml"]
EXPOSE 4317 4318 55678 55679