---
status: current
owner: gongzzang-제품
doc_type: README
last_reviewed: 2026-09-14
---

# gongzzang 플랫폼 인프라 (그린필드)

백지에서 새로 짓는 플랫폼 인프라를 Pulumi(TypeScript) 코드로 정의한 것입니다. 콘솔로 수동
생성된 **레거시 스택을 대체**하며, 레거시는 그대로 두고(WAF만 관리하는 `../infrastructure`는
건드리지 않음) 전환이 끝난 뒤에만 정리합니다.

## 비용: 런칭 전까지 0원

- **코드 작성과 `pulumi preview`는 0원**입니다. `pulumi up`을 실행하기 전에는 AWS 자원이 하나도
  생기지 않습니다.
- 실제 AWS 요금은 **런칭 때 `pulumi up`부터** 시작됩니다. 여기서 전부 준비한 뒤 한 번에 배포합니다.

## 사용 흐름

```bash
npm install
npm run typecheck        # tsc --noEmit — 코드가 컴파일되는지 검증 (0원)
pulumi preview           # 무엇이 생길지 미리보기, 실제 생성 없음 (0원)
# ... 런칭 때, AWS 자격증명과 함께:
pulumi up                # 자원 생성 (여기서부터 과금)
```

## 무엇을 만드나

| 조각 | 자원 | 비고 |
|---|---|---|
| 네트워크 | VPC(공용/사설 서브넷, NAT 1개) | 비용 아끼려 NAT 하나 |
| 컨테이너 저장소 | ECR | 우리 이미지 보관 |
| 제품 DB | RDS Postgres, 소형, Multi-AZ | 대용량은 R2/레이크하우스에 → DB는 작게 |
| 클러스터+입구 | ECS 클러스터 + 공용 ALB 1개 | 여러 서비스가 하나의 ALB(호스트/경로 분기) |
| 백엔드 | Fargate 서비스(1코어/8GB) | 레거시 **실측** 부하에 맞춘 크기 |
| 로그인(SSO) | Zitadel Fargate 서비스 | Go 단일 바이너리(JVM 아님). `auth.` 호스트 규칙으로 ALB 분기, 마스터키는 자동 생성 |

**아직 추가할 것**(같은 패턴, `index.ts`에 TODO로 명시):
웹·관리자 웹, 경로(osrm), llm 프록시, 그리고 데이터 수집·실시간 알림
(Lambda + EventBridge + SNS/SES). 앞문은 Cloudflare, 대용량은 R2 유지.

로그인은 **Zitadel**로 확정(Keycloak 대체) — Go라 가볍고, 이미 만든 identity-platform을 재사용.

## 크기는 config로 조정

기본값을 일부러 작게 잡았습니다(레거시가 실측상 거의 놀고 있었음 — DB CPU 약 2%, 백엔드 CPU
약 0.4%). 환경별로 덮어씁니다. 예:

```bash
pulumi config set env prod
pulumi config set dbInstanceClass db.m7g.large
pulumi config set backendImage <ecr-url>:<tag>
```

비밀은 코드에 없습니다: DB 마스터 비밀번호는 AWS가 관리(Secrets Manager)하고, 이미지 태그·크기는
Pulumi config에서 옵니다.

## 이 프로젝트가 아닌 것

레거시 WAF 전용 Pulumi 프로젝트(`../infrastructure`)는 별개이며 그대로 둡니다.
