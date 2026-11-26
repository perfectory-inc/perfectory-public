#!/usr/bin/env bash
# One step of the rollout before the service moves (root ADR-0616 §2-4, A1b): run one one-off task and wait
# until it has stopped with every container exited 0 (the bootstrap, or the migrate task whose containers
# run in order: open, migrate, grants, finalize). Anything else fails this script, so OpenTofu fails the
# apply before the service moves; the step stays tainted and the next apply runs it again.
# terraform_data.database_bootstrap and .database_migrate in foundation-database.tf run this.
# Prints task and container names and exit codes, never a log line: a failed psql statement can quote a password.
#
# Environment (from foundation-database.tf): REGION, CLUSTER, SUBNETS (comma-separated), SECURITY_GROUP,
# TASK_DEFINITION (family:revision) and WAIT_MINUTES (the contract's database.one_off_wait_minutes).
# AWS_PROFILE is the applier's; the provider's allowed_account_ids has already refused any other account.
set -euo pipefail

task="$(aws ecs run-task --region "$REGION" --cluster "$CLUSTER" --task-definition "$TASK_DEFINITION" \
  --launch-type FARGATE --started-by perfectory-platform-rollout \
  --network-configuration "awsvpcConfiguration={subnets=[$SUBNETS],securityGroups=[$SECURITY_GROUP],assignPublicIp=ENABLED}" \
  --query 'tasks[0].taskArn' --output text)"
if [ -z "$task" ] || [ "$task" = None ]; then
  printf 'FAIL pre-deploy: %s did not start\n' "$TASK_DEFINITION" >&2
  exit 1
fi
printf 'pre-deploy: %s started (task %s)\n' "$TASK_DEFINITION" "${task##*/}"

# Poll until the task has stopped, up to the declared wait (the CLI's own waiter gives up after 10 minutes).
deadline=$((SECONDS + WAIT_MINUTES * 60))
while :; do
  status="$(aws ecs describe-tasks --region "$REGION" --cluster "$CLUSTER" --tasks "$task" \
    --query 'tasks[0].lastStatus' --output text)"
  [ "$status" = STOPPED ] && break
  if ((SECONDS >= deadline)); then
    printf 'FAIL pre-deploy: %s (task %s) is still %s after %s minutes; it keeps running, and the next apply waits for its lock\n' \
      "$TASK_DEFINITION" "${task##*/}" "$status" "$WAIT_MINUTES" >&2
    exit 1
  fi
  sleep 15
done

# Every container exited 0. A container that never started (its dependency failed) has no exit code.
failed="$(aws ecs describe-tasks --region "$REGION" --cluster "$CLUSTER" --tasks "$task" \
  --query 'tasks[0].containers[?exitCode!=`0`].[name, exitCode]' --output text)"
if [ -n "$failed" ]; then
  stop="$(aws ecs describe-tasks --region "$REGION" --cluster "$CLUSTER" --tasks "$task" \
    --query 'tasks[0].stopCode' --output text)"
  printf 'FAIL pre-deploy: %s (task %s, %s) has containers that did not exit 0:\n%s\nTheir log streams are in the platform log group.\n' \
    "$TASK_DEFINITION" "${task##*/}" "$stop" "$failed" >&2
  exit 1
fi
printf 'pre-deploy: %s exited 0 in every container (task %s)\n' "$TASK_DEFINITION" "${task##*/}"
