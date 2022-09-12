FROM alpine:3.16.2
RUN apk add --no-cache ca-certificates

ADD tile38-server /usr/local/bin
ADD tile38-cli /usr/local/bin
ADD tile38-benchmark /usr/local/bin

RUN addgroup -S tile38 && \
    adduser -S -G tile38 tile38 && \
    mkdir /data && chown tile38:tile38 /data

VOLUME /data

EXPOSE 9851
CMD ["tile38-server", "-d", "/data"]
