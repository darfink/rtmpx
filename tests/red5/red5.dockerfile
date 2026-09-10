# Red5 server for the rtmpx live-interop harness.
# There is no official red5/red5-server image, so we build our own from the
# pinned release tarball. Bump RED5_VERSION deliberately, not by floating.
FROM eclipse-temurin:21-jre-jammy

ARG RED5_VERSION=2.0.40
ARG RED5_TGZ=https://github.com/Red5/red5-server/releases/download/v${RED5_VERSION}/red5-server-${RED5_VERSION}.tar.gz

RUN apt-get update && apt-get install -y --no-install-recommends curl ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /opt/red5
RUN curl -fL "$RED5_TGZ" -o /tmp/red5.tar.gz \
    && tar xzf /tmp/red5.tar.gz -C /opt/red5 --strip-components=1 \
    && rm /tmp/red5.tar.gz \
    && chmod +x red5.sh

# RTMP + HTTP (debug/console). The harness only needs 1935.
EXPOSE 1935 5080

CMD ["./red5.sh"]
