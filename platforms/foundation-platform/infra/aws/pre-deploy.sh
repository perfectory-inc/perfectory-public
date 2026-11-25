#!/usr/bin/env bash
# The rollout's pre-deploy step (root ADR-0616 A1b): before an apply moves the platform to a new image
# digest, the one-off tasks run in order (database bootstrap, foundation migrations, grants and finalize,
# the order docker-compose.yml runs them on the data host), and the next starts only after the one before
# exited 0. The first that does not stops this script, so OpenTofu fails the apply before the service
# moves (terraform_data.pre_deploy in foundation-database.tf runs this; the next apply runs it again).
# Prints task names and exit codes, never a log line: a failed psql statement can quote a password.
#
# Environment (from foundation-database.tf): ACCOUNT, REGION, CLUSTER, SUBNET, SECURITY_GROUP, and
# TASK_DEFINITIONS, the family:revision of each task in order. AWS_PROFILE is the applier's.
set -euo pipefail

for name in ACCOUNT REGION CLUSTER SUBNET SECURITY_GROUP TASK_DEFINITIONS; do
  if [ -z "${!name:-}" ]; then
    printf 'FAIL pre-deploy: %s is not set\n' "$name" >&2
    exit 2
  fi
done

account="$(aws sts get-caller-identity --query Account --output text)"
if [ "$account" != "$ACCOUNT" ]; then
  printf 'FAIL pre-deploy: the credentials are for account %s, not %s\n' "$account" "$ACCOUNT" >&2
  exit 2
fi

for definition in $TASK_DEFINITIONS; do
  task="$(aws ecs run-task --region "$REGION" --cluster "$CLUSTER" --task-definition "$definition" \
    --launch-type FARGATE --started-by perfectory-platform-rollout \
    --network-configuration "awsvpcConfiguration={subnets=[$SUBNET],securityGroups=[$SECURITY_GROUP],assignPublicIp=ENABLED}" \
    --query 'tasks[0].taskArn' --output text)"
  if [ -z "$task" ] || [ "$task" = None ]; then
    printf 'FAIL pre-deploy: %s did not start\n' "$definition" >&2
    exit 1
  fi
  # The waiter gives up after 10 minutes and exits non-zero, which stops the rollout too.
  aws ecs wait tasks-stopped --region "$REGION" --cluster "$CLUSTER" --tasks "$task"
  read -r code reason < <(aws ecs describe-tasks --region "$REGION" --cluster "$CLUSTER" --tasks "$task" \
    --query 'tasks[0].[containers[0].exitCode, stopCode]' --output text)
  if [ "$code" != 0 ]; then
    printf 'FAIL pre-deploy: %s exited %s (%s); task %s, its log stream in the platform log group\n' \
      "$definition" "$code" "$reason" "${task##*/}" >&2
    exit 1
  fi
  printf 'pre-deploy: %s exited 0 (task %s)\n' "$definition" "${task##*/}"
done
