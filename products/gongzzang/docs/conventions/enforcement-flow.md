---
status: current
owner: gongzzang
doc_type: convention
last_reviewed: 2026-11-05
---

# 자동 강제 흐름 (Enforcement Flow)

> AGENTS.md §4(자동 강제 흐름)·§9(1500줄 안티패턴 경보)에서 이관 (2026-07-20, docs 대개편 Phase B2).

## 강제 단계

```
1. 에디터        rust-analyzer + Biome 확장        실시간 lint/format
2. pre-commit    lefthook                          gitleaks + Biome + rustfmt + markdownlint + panel 가드
3. pre-push      lefthook                          수 초짜리 래칫(파일 크기 포함)·문서 무결성·영역 grep만 (루트 ADR-0098, ADR-0149)
4. CI (PR·큐)    GitHub Actions                    fmt/clippy/test, Biome·타입·단위, 마이그레이션, cargo-deny, OSV, gitleaks
```

훅 정의 정본은 루트 `lefthook.yml`, 필수 CI 체크 목록 정본은 `tools/github/main-ruleset.json`이다.
Gongzzang CI는 SBOM/SLSA 첨부를 일부러 뺐다(`.github/workflows/gongzzang-ci.yml` 머리말).

컨벤션별 도구 매핑: [README.md](./README.md)의 "자동 강제 도구 매핑" 표.

## 1500줄 안티패턴 경보

1,300~1,400줄짜리 단일 SSOT 파일(legacy repo 사례 — 현 트리에는 없음) = **이름만 SSOT**. 폴더 단위 SSOT가
진짜 SSOT.

- 500줄 도달 → 분해 검토
- 1500줄 도달 → CI가 차단 (`scripts/guard/file-line-limit.sh`, 예외 목록 `tools/ci/file-line-limit.exceptions.json`)
- *처음부터* 폴더로 시작
