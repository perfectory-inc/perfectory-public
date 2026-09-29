# tippecanoe for the lakehouse tile bake (root ADR-0112 §9).
#
# There is no official tippecanoe image or binary release, so it is built here from the tagged
# source commit, and the build refuses any other commit. The base image is pinned by digest. The
# image name and the expected version banner live in config/tile-bake-containers.contract.json.
FROM ubuntu:24.04@sha256:008173c23f95b170204355c12626cb5a965d779a7e1283b09e9cffbb1bf33ca3 AS build
ARG TIPPECANOE_COMMIT=68ab8dcc229f95b8b25877697d5e8d66783af503
RUN apt-get update \
 && apt-get install -y --no-install-recommends build-essential ca-certificates git libsqlite3-dev zlib1g-dev \
 && rm -rf /var/lib/apt/lists/*
RUN git clone --no-checkout https://github.com/felt/tippecanoe.git /src \
 && git -C /src checkout --detach "$TIPPECANOE_COMMIT" \
 && test "$(git -C /src rev-parse HEAD)" = "$TIPPECANOE_COMMIT"
RUN make -C /src -j"$(nproc)" && make -C /src install PREFIX=/opt/tippecanoe

FROM ubuntu:24.04@sha256:008173c23f95b170204355c12626cb5a965d779a7e1283b09e9cffbb1bf33ca3
RUN apt-get update \
 && apt-get install -y --no-install-recommends libsqlite3-0 zlib1g \
 && rm -rf /var/lib/apt/lists/*
COPY --from=build /opt/tippecanoe/bin/ /usr/local/bin/
USER 65534:65534
ENTRYPOINT ["tippecanoe"]
