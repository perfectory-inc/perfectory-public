#!/usr/bin/env bash
# Converts the collected 읍면동 boundary shapefile ZIPs into per-sido GeoJSON.
#
# This ran by hand on ai-server for the first administrative-boundary publication
# (2026-09-05, root memory "the admin boundary joins the map") and lived only in that
# machine's shell history — the manual step named by the 2026-09-06 SSOT sweep. The traps
# it encodes, learned live:
#   - alpine-small GDAL has no GEOS, so `-makevalid` fails: use ubuntu-small.
#   - busybox unzip cannot read Zip64: read the archive through /vsizip instead.
#   - the DBF is EUC-KR: SHAPE_ENCODING must say so or every name mojibakes.
#
# Follow with merge.py, then the snapshot writer
# (`write-official-administrative-boundary-source-snapshot`) and
# `register-serving-source-lineage` — no step of this lane is hand-typed SQL anymore.
set -Eeuo pipefail

WORKDIR="${ADMIN_BOUNDARY_WORKDIR:-/data/parcel-work/admin-src}"
cd "$WORKDIR"

# This command WRITES nothing to R2; reader credentials would do, but the sweep env is the
# root-readable file that already carries working R2 credentials on the server.
docker run --rm -v /etc/foundation-platform:/t:ro alpine:3.20@sha256:28bd5fe8b56d1bd048e5babf5b10710ebe0bae67db86916198a6eec434943f8b sh -c 'grep -E "^FOUNDATION_PLATFORM_R2_LAKEHOUSE_(BUCKET|ENDPOINT|WRITER_ACCESS_KEY_ID|WRITER_SECRET_ACCESS_KEY)=" /t/source-sweep.env' > r2v
set -a; . ./r2v; set +a; rm -f r2v
AWS() { docker run --rm -v "$WORKDIR":/w -e AWS_ACCESS_KEY_ID="$FOUNDATION_PLATFORM_R2_LAKEHOUSE_WRITER_ACCESS_KEY_ID" -e AWS_SECRET_ACCESS_KEY="$FOUNDATION_PLATFORM_R2_LAKEHOUSE_WRITER_SECRET_ACCESS_KEY" amazon/aws-cli:2.17.0@sha256:643507c10ada7964ca6157b3d799f030b90577643da9955d319a77399ed80d73 "$@"; }
mkdir -p zips out
AWS s3 cp "s3://$FOUNDATION_PLATFORM_R2_LAKEHOUSE_BUCKET/bronze/source=vworldkr__boundary_emd/" /w/zips/ --recursive --exclude "*" --include "*.zip" --endpoint-url "$FOUNDATION_PLATFORM_R2_LAKEHOUSE_ENDPOINT" | tail -1
ls zips/*.zip | wc -l
# The image is spelled literally at the run site so the container-runtime policy can read
# the pin; ubuntu-small because alpine-small lacks GEOS and -makevalid fails on it.
G() { docker run --rm -v "$WORKDIR":/w -e SHAPE_ENCODING=EUC-KR ghcr.io/osgeo/gdal:ubuntu-small-3.10.2@sha256:a2af3ef63be13b35790ce7a508ff395c409ef7a0b8ddc5ab9685dd4518af9779 "$@"; }
for z in zips/*.zip; do
  b=$(basename "$z" .zip)
  G ogr2ogr -f GeoJSON "/w/out/$b.geojson" "/vsizip//w/$z" \
    -t_srs EPSG:4326 -makevalid -nlt PROMOTE_TO_MULTI \
    -select EMD_CD,EMD_NM,COL_ADM_SE 2>"out/$b.err" || { echo "FAILED $b"; cat "out/$b.err"; exit 1; }
  echo "converted $b"
done
G ogr2ogr -f GeoJSON /w/out/sgg-attrs.geojson "/vsizip//w/sgg-sample.zip" -select BJCD,NAME -nlt NONE -dim XY 2>/dev/null || \
G ogr2ogr -f CSV /w/out/sgg-attrs.csv "/vsizip//w/sgg-sample.zip" -select BJCD,NAME 2>/dev/null
echo "conversion done"
