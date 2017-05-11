FROM alpine:3.4

ENV TILE38_VERSION 1.9.0
ENV TILE38_DOWNLOAD_URL https://github.com/tidwall/tile38/releases/download/$TILE38_VERSION/tile38-$TILE38_VERSION-linux-amd64.tar.gz

RUN addgroup -S tile38 && adduser -S -G tile38 tile38

RUN apk update \
    && apk add ca-certificates \
    && update-ca-certificates \
    && apk add openssl \
    && wget -O tile38.tar.gz "$TILE38_DOWNLOAD_URL" \
    && tar -xzvf tile38.tar.gz \
    && rm -f tile38.tar.gz \
    && mv tile38-$TILE38_VERSION-linux-amd64/tile38-server /usr/local/bin \
    && mv tile38-$TILE38_VERSION-linux-amd64/tile38-cli /usr/local/bin \
    && rm -fR tile38-$TILE38_VERSION-linux-amd64

RUN mkdir /data && chown tile38:tile38 /data

VOLUME /data
WORKDIR /data

EXPOSE 9851
CMD ["tile38-server"]
