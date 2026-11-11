---
status: current
owner: gongzzang-제품
doc_type: README
last_reviewed: 2026-11-11
---

# 보안 인프라

Pulumi 인프라 스택이 읽는 트래픽·인증 엣지 생성 산출물을 둔다.

- `traffic-auth-edge-policy.generated.json`은 `docs/architecture/traffic-auth-policy-registry.v1.json`에서
  생성한, 제공자에 묶이지 않는 엣지 유입 투영이다.
- `aws-wafv2-edge-policy.generated.json`은 그 투영에서 나온 AWS WAFv2·Pulumi 규칙 목록이다.

생성 파일을 직접 수정하지 않는다. 레지스트리를 바꾼 뒤 다음 명령으로 다시 생성한다
(CI `traffic-auth-policy-drift` 가 같은 명령으로 어긋남을 검사한다).
`cargo run -p gongzzang-api --bin generate-traffic-auth-policy`.

현재 상태: `../index.ts` 가 WAFv2 규칙 목록을 읽어 WebACL 을 만든다. 범위는 Pulumi 설정 `wafScope`
(기본 `REGIONAL`, 규칙 목록의 `scope_options` 안에서만)로 정한다. 지역 범위 스택은 `wafRegionalResourceArn`
설정 또는 `GONGZZANG_WAF_REGIONAL_RESOURCE_ARN` 에 대상 ALB·API Gateway ARN 을 넣으면 WebACL 을 연결한다.
CloudFront 범위에서는 ARN 연결을 거부하며, 이 디렉터리에는 CloudFront 배포 연결이 없다.
