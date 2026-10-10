---
status: current
owner: repository-maintainers
doc_type: catalog
last_reviewed: 2026-10-10
---

# 전역 ADR

모노레포 전체에서 사용하는 단일 ADR 번호 체계입니다. 영역에만 적용되는 결정도 다음
전역 번호를 사용합니다. 각 영역의 기존 `docs/adr/` 번호 체계는 마지막 번호에서
동결하며, 영역 결정은 `GZ-ADR-NNNN`, `FP-ADR-NNNN`, `IDP-ADR-NNNN`,
`ITP-ADR-NNNN`처럼 영역 접두사를 붙여 인용합니다.

## 목록은 이 디렉터리다

ADR 목록은 이 디렉터리의 파일 목록이 정본입니다. 파일 이름이 번호 순서대로 정렬되고,
각 파일의 첫 제목(`# ADR NNNN: <제목>`)이 그 결정의 제목입니다. 이 README에 결정마다 한 줄씩
손으로 덧붙이던 목록은 두지 않습니다 — 결정을 기록하는 PR마다 같은 자리에 줄을 붙여 서로
충돌했기 때문입니다([ADR-0176](./0176-generated-docs-do-not-make-unrelated-prs-conflict.md)).

제목이 붙은 목록이 필요하면 생성기로 출력합니다. `main`의 `docs` 워크플로 실행 요약에도
같은 목록이 실립니다.

```bash
python3 scripts/catalog/render-adr-index.py           # 제목 목록 출력
python3 scripts/catalog/render-adr-index.py --check   # 이름·제목·번호 중복 검사
```

## 새 ADR

- 번호는 이 디렉터리의 가장 큰 번호 + 1, 파일 이름은 `NNNN-<kebab-case-제목>.md`입니다.
- 첫 제목은 `# ADR NNNN: <제목>`이며 번호가 파일 이름과 같아야 합니다.
- 이 README는 고치지 않습니다.
- 두 PR이 같은 번호를 잡으면 파일 이름이 달라 텍스트 충돌은 나지 않지만, `--check`가
  머지 큐에서 번호 중복으로 거부합니다. 나중 PR이 번호를 바꿉니다.
