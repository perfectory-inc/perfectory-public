#!/usr/bin/env bash
# Sourced by daily-source-sweep.sh and staging-smoke.sh: the daily sweep's VWorld lane settings
# (root ADR-0168, ADR-0172), held once so the staging smoke runs a new release's lane exactly as the
# sweep will run it after the switch (root ADR-0177).
#
#   vworld_sweep_lane_env <endpoint catalog> <plan> <inventory> <evidence> <spool dir>
#
# The plan takes only the endpoints the catalog marks source_sweep, without a summary file; the
# lane has no byte budget (ADR-0172); new files are content-addressed and spooled while hashed
# (ADR-0152, ADR-0168); RAON selection archives are left to their own lane (ADR-0170).

vworld_sweep_lane_env() {
  export FOUNDATION_PLATFORM_VWORLD_DATASET_ENDPOINT_CATALOG_PATH="$1"
  export FOUNDATION_PLATFORM_VWORLD_DATASET_DAILY_COLLECTION=source_sweep
  unset FOUNDATION_PLATFORM_VWORLD_DATASET_INVENTORY_SUMMARY_PATH FOUNDATION_PLATFORM_BRONZE_FORCE_REFETCH
  export FOUNDATION_PLATFORM_VWORLD_DATASET_COLLECTION_PLAN_PATH="$2"
  export FOUNDATION_PLATFORM_VWORLD_DATASET_FILE_INVENTORY_PATH="$3"
  export FOUNDATION_PLATFORM_VWORLD_DATASET_FILE_INGEST_EVIDENCE_PATH="$4"
  export FOUNDATION_PLATFORM_VWORLD_DATASET_FILE_LIVE_WRITE=1
  export FOUNDATION_PLATFORM_VWORLD_DATASET_FILE_CONFIRM_FULL_DOWNLOAD=1
  export FOUNDATION_PLATFORM_VWORLD_DATASET_FILE_EXCLUDE_SELECTION_ARCHIVES=1
  export FOUNDATION_PLATFORM_VWORLD_DATASET_FILE_BRONZE_KEY=content_addressed
  export FOUNDATION_PLATFORM_VWORLD_DATASET_FILE_SPOOL_DIR="$5"
}
