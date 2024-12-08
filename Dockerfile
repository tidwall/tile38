FROM alpine:3.20

ARG VERSION
ARG TARGETOS
ARG TARGETARCH

RUN apk add --no-cache ca-certificates

ADD packages/tile38-$VERSION-$TARGETOS-$TARGETARCH/tile38-server /usr/local/bin
ADD packages/tile38-$VERSION-$TARGETOS-$TARGETARCH/tile38-cli /usr/local/bin
ADD packages/tile38-$VERSION-$TARGETOS-$TARGETARCH/tile38-benchmark /usr/local/bin

RUN addgroup -S tile38 && \
    adduser -S -G tile38 tile38 && \
    mkdir /data && chown tile38:tile38 /data

VOLUME /data

EXPOSE 9851
CMD ["tile38-server", "-d", "/data"]
