#!/usr/bin/env bash
# Run one probe step as one principal of the Lakekeeper dev rig and print its result lines (probes/probe.py: JSON
# checks, never a token or a secret), then exit with the probe's own exit code.
#
#   probe.sh <principal> <step> [args...]      principal: a contract principal, or anonymous
#
# The task runs in the rig's cluster from the platform's public subnets in the probe security group; everything is
# found by the names the contract gives it. AWS_PROFILE is the operator's SSO profile (the rig's account only).
set -euo pipefail
# The AWS CLI prints log lines in UTF-8 (a probe's output may hold any character; Windows' code page cannot).
export PYTHONUTF8=1 PYTHONIOENCODING=utf-8

here="$(cd "$(dirname "$0")" && pwd -P)"
contract="$here/../../config/lakekeeper-dev.contract.json"
platform="$here/../../config/aws-platform.contract.json"
name="$(python3 -c 'import json,sys;print(json.load(open(sys.argv[1]))["name"])' "$contract")"
region="$(python3 -c 'import json,sys;print(json.load(open(sys.argv[1]))["region"])' "$platform")"
account="$(python3 -c 'import json,sys;print(json.load(open(sys.argv[1]))["account"]["id"])' "$platform")"
caller="$(aws sts get-caller-identity --query Account --output text)"
[ "$caller" = "$account" ] || { echo "not the rig's account" >&2; exit 2; }

principal="$1"
shift
command_json="$(python3 -c 'import json,sys;print(json.dumps(sys.argv[1:]))' "$@")"
vpc="$(aws ec2 describe-vpcs --region "$region" --filters Name=tag:Name,Values=perfectory-platform --query 'Vpcs[0].VpcId' --output text)"
subnets="$(aws ec2 describe-subnets --region "$region" --filters "Name=vpc-id,Values=$vpc" Name=tag:tier,Values=public \
  --query 'Subnets[].SubnetId' --output text | tr '\t' ',')"
group="$(aws ec2 describe-security-groups --region "$region" --filters "Name=vpc-id,Values=$vpc" "Name=group-name,Values=$name-probe" \
  --query 'SecurityGroups[0].GroupId' --output text)"

overrides="$(python3 -c 'import json,sys;print(json.dumps({"containerOverrides":[{"name":sys.argv[1],"command":json.loads(sys.argv[2])}]}))' \
  "$principal" "$command_json")"
task="$(aws ecs run-task --region "$region" --cluster "$name" --task-definition "$name-probe-$principal" --launch-type FARGATE \
  --started-by "$name-probe" --overrides "$overrides" \
  --network-configuration "awsvpcConfiguration={subnets=[$subnets],securityGroups=[$group],assignPublicIp=ENABLED}" \
  --query 'tasks[0].taskArn' --output text)"
id="${task##*/}"
printf '{"probe": "%s", "step": %s, "task": "%s"}\n' "$principal" "$command_json" "$id" >&2

# Wait until it stops (a step is minutes at most; the expiry step waits for a 5-minute token).
deadline=$((SECONDS + 20 * 60))
while :; do
  status="$(aws ecs describe-tasks --region "$region" --cluster "$name" --tasks "$task" --query 'tasks[0].lastStatus' --output text)"
  [ "$status" = STOPPED ] && break
  ((SECONDS < deadline)) || { echo "probe $id still running after 20 minutes" >&2; exit 3; }
  sleep 10
done
code="$(aws ecs describe-tasks --region "$region" --cluster "$name" --tasks "$task" --query 'tasks[0].containers[0].exitCode' --output text)"
reason="$(aws ecs describe-tasks --region "$region" --cluster "$name" --tasks "$task" --query 'tasks[0].stoppedReason' --output text)"
MSYS_NO_PATHCONV=1 aws logs get-log-events --region "$region" --log-group-name "/$name" --log-stream-name "probe/$principal/$id" \
  --start-from-head --query 'events[].message' --output text 2>/dev/null | tr '\t' '\n' || true
printf '{"probe": "%s", "task": "%s", "exit": "%s", "stopped": "%s"}\n' "$principal" "$id" "$code" "$reason" >&2
[ "$code" = 0 ]
