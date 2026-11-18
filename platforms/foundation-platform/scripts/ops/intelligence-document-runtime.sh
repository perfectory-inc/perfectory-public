# shellcheck shell=bash
# What the two intelligence document jobs share (root ADR-0477): the run directory, the intelligence
# image's own contract, the Spark steps, the media fetch and the image run. Sourced after
# admitted-writer-runtime.sh and job-journal.sh, with STATE_ROOT, journal and run_id set.

# The image the release built from platforms/intelligence-platform/compose.document-jobs.yml and
# tagged as compose names it (root ADR-0276 §13). Its contract names it too (containers.document_text).
INTELLIGENCE_DOCUMENT_IMAGE="intelligence-document-text:local"
# The pinned reference tools/technology-versions.contract.json lists (the parcel-number-change
# collector's same image); the release subtree does not carry tools/.
INTELLIGENCE_MEDIA_FETCH_IMAGE="amazon/aws-cli:2.17.0@sha256:643507c10ada7964ca6157b3d799f030b90577643da9955d319a77399ed80d73"

# work: the run's directory on the host; container_work: the same directory in the Spark container
# (compose.lakehouse.yml mounts FOUNDATION_PLATFORM_LAKEHOUSE_STATE_ROOT at /workspace/target/lakehouse).
intelligence_document_run_dir() {
  export FOUNDATION_PLATFORM_LAKEHOUSE_STATE_ROOT="${STATE_ROOT}/lakehouse"
  work="${FOUNDATION_PLATFORM_LAKEHOUSE_STATE_ROOT}/runs/$1-${run_id}"
  container_work="/workspace/target/lakehouse/runs/$1-${run_id}"
  # The job makes its own lakehouse directory: the release cannot, because systemd-tmpfiles refuses
  # to step from /data/foundation-platform (the operator's) into STATE_ROOT (ours), and that refusal
  # stopped every deploy from 2026-11-11 15:34Z (exit 73).
  install -d -m 0777 "${FOUNDATION_PLATFORM_LAKEHOUSE_STATE_ROOT}"
  # The Spark container (uid 185) writes the plan; the image (uid 10001) writes its rows.
  mkdir -p -m 0777 "${work}" "${work}/plan" "${work}/out"
  chmod 0777 "${work}" "${work}/plan" "${work}/out"
  run_log="${work}/run.log"
}

# The contract the image carries: the versions it reads and writes, its settings and caps. Planning
# from it means a run never plans for one extractor version and reads with another.
intelligence_document_extraction_contract() {
  # 64m: cat of one small file.
  docker run --rm --log-driver=none --network none --memory 64m --memory-swap=64m --entrypoint cat \
    "${INTELLIGENCE_DOCUMENT_IMAGE}" /opt/intelligence/config/document-extraction.contract.json > "${work}/extraction.json"
  chmod a+r "${work}/extraction.json"
}

# The serving runtime's gateway, from the contract the image carries (root ADR-0478 §1): base URL,
# chat path and model, on one line.
intelligence_document_model_gateway() {
  docker run --rm --log-driver=none --network none --memory 64m --memory-swap=64m --entrypoint python \
    "${INTELLIGENCE_DOCUMENT_IMAGE}" -I -c '
import json
c = json.load(open("/opt/intelligence/config/model-runtime.contract.json", encoding="utf-8"))
r = c["runtimes"][c["serving"]]
print("http://" + r["listen"], r["chat_path"], r["model"])'
}

intelligence_document_spark() {
  local command="$1"
  shift
  lakehouse_run_log "${run_log}"
  FOUNDATION_PLATFORM_LAKEHOUSE_IVY_CACHE="${RELEASE_JARS_DIR}" FOUNDATION_PLATFORM_LAKEHOUSE_IVY_MODE=ro \
  lakehouse_spark_run foundation-platform-compute \
    -e FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_URI -e FOUNDATION_PLATFORM_LAKEHOUSE_WAREHOUSE \
    -e FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_WRITER_TOKEN -e FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_PROVIDER \
    spark-small spark-submit --master 'local[2,8]' --driver-memory 1500m \
    --jars "${SPARK_RELEASE_JARS}" \
    /workspace/infra/lakehouse/spark/jobs/intelligence_documents.py "${command}" \
    --extraction "${container_work}/extraction.json" "$@" >> "${run_log}" 2>&1
}

# A value of the last summary line a Spark step printed.
intelligence_document_summary() {
  local line
  line="$(grep '^intelligence-documents-summary-json ' "${run_log}" | tail -1)"
  python3 -I -c 'import json, sys; print(json.loads(sys.argv[1].split(" ", 1)[1])[sys.argv[2]])' "${line}" "$1"
}

# The planned bytes, read only, with the lakehouse reader key, into ${work}/media/<sha256>.<format>.
# One container for the whole list; `media.tsv` is "<object key>\t<file name>" per line.
intelligence_document_fetch_media() {
  local list="$1" memory
  mkdir -p "${work}/media"
  [ -s "${list}" ] || return 0
  memory="$(python3 -I -c 'import json, sys; print(json.load(open(sys.argv[1], encoding="utf-8"))["containers"]["media_fetch"]["memory_limit"])' "${work}/extraction.json")"
  AWS_ACCESS_KEY_ID="${FOUNDATION_PLATFORM_LAKEHOUSE_READER_ACCESS_KEY_ID}" \
  AWS_SECRET_ACCESS_KEY="${FOUNDATION_PLATFORM_LAKEHOUSE_READER_SECRET_ACCESS_KEY}" \
  docker run --rm --log-driver=none --memory "${memory}" --memory-swap="${memory}" --user "$(id -u):$(id -g)" -e HOME=/tmp \
    -e AWS_ACCESS_KEY_ID -e AWS_SECRET_ACCESS_KEY -e AWS_DEFAULT_REGION=auto \
    -e ENDPOINT="${FOUNDATION_PLATFORM_R2_LAKEHOUSE_ENDPOINT}" -e BUCKET="${FOUNDATION_PLATFORM_R2_LAKEHOUSE_BUCKET}" \
    -v "${list}:/list.tsv:ro" -v "${work}/media:/w" --entrypoint /bin/sh "${INTELLIGENCE_MEDIA_FETCH_IMAGE}" -c '
      while IFS="	" read -r key name; do
        aws s3 cp --only-show-errors --endpoint-url "${ENDPOINT}" "s3://${BUCKET}/${key}" "/w/${name}"
      done < /list.tsv' >> "${run_log}" 2>&1
  chmod -R a+rX "${work}/media"
}

# The image's cap and CPUs, from the contract it carries: "<memory> <cpus>".
intelligence_document_image_limits() {
  python3 -I -c '
import json, sys
c = json.load(open(sys.argv[1], encoding="utf-8"))["containers"]["document_text"]
print(c["memory_limit"], c["cpus"])' "${work}/extraction.json"
}

# The text run: no network at all; the plan and the bytes read only, its rows into out/.
# --log-driver=none: piped stdout would also fill the json-file log on the root disk.
intelligence_document_text_run() {
  local memory cpus
  read -r memory cpus < <(intelligence_document_image_limits)
  docker run --rm --log-driver=none --name "intelligence-document-text-${run_id,,}" \
    --memory "${memory}" --memory-swap="${memory}" --cpus "${cpus}" --pids-limit 512 \
    --cap-drop ALL --security-opt no-new-privileges --network none \
    -v "${work}/plan:/plan:ro" -v "${work}/media:/media:ro" -v "${work}/out:/out" \
    "${INTELLIGENCE_DOCUMENT_IMAGE}" --index /plan/index.jsonl --media /media --out /out >> "${run_log}" 2>&1
}

# The fact run: the host network only to reach the loopback model gateway (root ADR-0478 §1).
intelligence_document_fact_run() {
  local memory cpus model_base model_path model_name
  read -r memory cpus < <(intelligence_document_image_limits)
  read -r model_base model_path model_name < <(intelligence_document_model_gateway)
  docker run --rm --log-driver=none --name "intelligence-document-fact-${run_id,,}" \
    --memory "${memory}" --memory-swap="${memory}" --cpus "${cpus}" --pids-limit 512 \
    --cap-drop ALL --security-opt no-new-privileges --network host \
    -e MODEL_RUNTIME_BASE_URL="${model_base}" -e MODEL_RUNTIME_CHAT_PATH="${model_path}" \
    -e MODEL_RUNTIME_DEFAULT_MODEL="${model_name}" \
    -v "${work}/plan:/plan:ro" -v "${work}/media:/media:ro" -v "${work}/out:/out" --entrypoint python \
    "${INTELLIGENCE_DOCUMENT_IMAGE}" -m document_text.fact_run --pages /plan --media /media --out /out >> "${run_log}" 2>&1
}
