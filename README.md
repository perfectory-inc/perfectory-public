---
status: current
owner: repository-maintainers
doc_type: README
last_reviewed: 2026-07-28
---

# perfectory

산업용 부동산 제품과 수평 플랫폼을 한 저장소에서 함께 관리하는 모노레포입니다.
이 파일은 전체 지도를 제공하고, 상세 내용은 소유 영역의 README와 문서 정본으로 연결합니다.

## 책임 트리

```text
perfectory/
├── products/gongzzang/                 B2C 산업용 부동산 제품
├── platforms/foundation-platform/      공공데이터·카탈로그·레이크하우스 SSOT
├── platforms/identity-platform/        직원·서비스 인증과 인가 정책
├── platforms/intelligence-platform/    LLM 정규화 제안 엔진
├── docs/                               전역 문서·ADR·기술 기준
├── scripts/                            검증·가드·자동화
└── .github/                            전역 CI/CD
```

## 시작 순서

1. [영역 규칙](./AGENTS.md)
2. [전역 문서 지도](./docs/README.md)
3. [전체 문서 자동 색인](./docs/document-catalog.md)
4. 작업 영역의 `README.md`와 `AGENTS.md`

## 영역 진입점

| 영역 | 코드 지도 | 문서 지도 |
|---|---|---|
| Gongzzang | [README](./products/gongzzang/README.md) | [docs](./products/gongzzang/docs/README.md) |
| Foundation | [README](./platforms/foundation-platform/README.md) | [docs](./platforms/foundation-platform/docs/README.md) |
| Identity | [README](./platforms/identity-platform/README.md) | [docs](./platforms/identity-platform/docs/README.md) |
| Intelligence | [README](./platforms/intelligence-platform/README.md) | [docs](./platforms/intelligence-platform/docs/README.md) |

## 공통 기준

- [기술 스택 기준표](./docs/technology-stack.md)
- [전역 ADR](./docs/adr/README.md)
- [검증 SSOT](./docs/adr/0004-verification-ssot.md)
- Rust 검증: `bash scripts/verify/cargo-verify.sh <area-dir>` (Docker 필요)

## License

이 저장소는 공개 열람 가능한 **독점 소프트웨어**이며 오픈소스가 아닙니다.
GitHub 서비스 안의 열람·fork에 필요한 권리는 GitHub 이용약관을 따르며, 그 범위를
넘는 사용·수정·배포 권리는 별도 서면 계약 없이는 부여되지 않습니다.
전체 조건은 [LICENSE](./LICENSE), 제3자 고지는
[THIRD_PARTY_NOTICES.md](./THIRD_PARTY_NOTICES.md)를 확인하세요.
