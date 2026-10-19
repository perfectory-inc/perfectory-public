#!/usr/bin/env bash
# Fetches the collected 읍면동 boundary shapefile ZIPs and converts the ones named into GeoJSON.
#
#   convert.sh download            every boundary ZIP under the Bronze prefix -> zips/
#   convert.sh convert <zip>...    each named zips/<zip> -> out/<zip without .zip>.geojson
#
# The scheduled release job (scripts/ops/admin-boundary-release.sh, root ADR-0236) runs both: it
# downloads, picks the newest round from the members (rounds.py), and converts that round's ZIPs
# only. This first ran by hand on ai-server (2026-09-05) and lived in that machine's shell history.
# The traps it encodes, learned live:
#   - alpine-small GDAL has no GEOS, so `-makevalid` fails: use ubuntu-small.
#   - busybox unzip cannot read Zip64: read the archive through /vsizip instead.
#   - the DBF is EUC-KR: SHAPE_ENCODING must say so or every name mojibakes.
#
# R2 is read with the read-only pair the unit's environment gives (lakehouse-reader.env and the
# lakehouse bucket settings); nothing here writes to R2.
set -Eeuo pipefail

WORKDIR="${ADMIN_BOUNDARY_WORKDIR:-/data/parcel-work/admin-src}"
cd "$WORKDIR"
mkdir -p zips out

case "${1:-}" in
  download)
    for name in FOUNDATION_PLATFORM_R2_LAKEHOUSE_BUCKET FOUNDATION_PLATFORM_R2_LAKEHOUSE_ENDPOINT \
        FOUNDATION_PLATFORM_LAKEHOUSE_READER_ACCESS_KEY_ID FOUNDATION_PLATFORM_LAKEHOUSE_READER_SECRET_ACCESS_KEY; do
      [[ -n "${!name:-}" ]] || { echo "convert.sh: ${name} is not set" >&2; exit 78; }
    done
    # --memory 512m: amazon/aws-cli 2.17.0 measured on ai-server 2026-10-18 (cgroup v2): anonymous memory peaked at 133 MiB over `s3 cp` of 1 GiB up and down (file and stdin/stdout), `s3 sync` of 5 x 200 MiB and `s3 mv`; the rest is reclaimable page cache.
    docker run --rm --memory 512m -v "$WORKDIR":/w \
      -e AWS_ACCESS_KEY_ID="$FOUNDATION_PLATFORM_LAKEHOUSE_READER_ACCESS_KEY_ID" \
      -e AWS_SECRET_ACCESS_KEY="$FOUNDATION_PLATFORM_LAKEHOUSE_READER_SECRET_ACCESS_KEY" \
      amazon/aws-cli:2.17.0@sha256:643507c10ada7964ca6157b3d799f030b90577643da9955d319a77399ed80d73 \
      s3 sync "s3://$FOUNDATION_PLATFORM_R2_LAKEHOUSE_BUCKET/bronze/source=vworldkr__boundary_emd/" /w/zips/ \
      --exclude "*" --include "*.zip" --endpoint-url "$FOUNDATION_PLATFORM_R2_LAKEHOUSE_ENDPOINT" --only-show-errors
    echo "downloaded $(ls zips/*.zip | wc -l) zips"
    ;;
  convert)
    shift
    (($# > 0)) || { echo "convert.sh convert: name the zips" >&2; exit 64; }
    for z in "$@"; do
      [[ "$z" =~ ^[A-Za-z0-9._-]+\.zip$ && -f "zips/$z" ]] || { echo "convert.sh: no zips/$z" >&2; exit 64; }
      b="${z%.zip}"
      # The image is spelled literally at the run site so the container-runtime policy can read
      # the pin; ubuntu-small because alpine-small lacks GEOS and -makevalid fails on it.
      # --memory 256m: this ogr2ogr measured on ai-server 2026-10-18 converting the whole merged national
      # 읍면동 GeoJSON (136 MB, every province at once) peaked at 21 MiB anonymous memory.
      docker run --rm --memory 256m -v "$WORKDIR":/w -e SHAPE_ENCODING=EUC-KR \
        ghcr.io/osgeo/gdal:ubuntu-small-3.10.2@sha256:a2af3ef63be13b35790ce7a508ff395c409ef7a0b8ddc5ab9685dd4518af9779 \
        ogr2ogr -f GeoJSON "/w/out/$b.geojson" "/vsizip//w/zips/$z" \
        -t_srs EPSG:4326 -makevalid -nlt PROMOTE_TO_MULTI \
        -select EMD_CD,EMD_NM,COL_ADM_SE 2>"out/$b.err" || { echo "FAILED $b"; cat "out/$b.err"; exit 1; }
      echo "converted $b"
    done
    ;;
  *)
    echo "usage: convert.sh download | convert <zip>..." >&2
    exit 64
    ;;
esac
