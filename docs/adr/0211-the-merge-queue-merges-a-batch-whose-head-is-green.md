# ADR 0211: 머지 큐는 머리가 초록인 묶음을 병합한다

- Status: Accepted
- Date: 2026-10-14
- Amends: [ADR-0155](./0155-ci-and-release-builds-reuse-content-addressed-dependency-builds.md) 4항의 머지 큐 매개변수

## Context

ADR-0155는 머지 큐를 `ALLGREEN`, 동시 빌드 3, 병합 3, 대기 0분으로 두었다.

- `ALLGREEN`에서는 그룹에 든 풀 리퀘스트마다 자기 머지 그룹 빌드가 초록이어야 한다.
  큐에 3건이 있으면 main+A, main+A+B, main+A+B+C를 따로 빌드하고 셋이 모두 끝나야 병합한다.
- 같은 날 여러 워커가 풀 리퀘스트를 동시에 큐에 넣었다. 큐의 빌드 하나는 필수 워크플로 전부(Foundation 포함)를
  돌린다. 그래서 동시 빌드 3개가 표준 러너를 차지했고, 열린 풀 리퀘스트의 실행이 그 뒤에서 기다렸다.
- 빌드 중에 들어온 풀 리퀘스트는 다음 그룹이 되지만, `ALLGREEN`은 그 안의 항목마다 빌드가 초록이기를 요구한다.

## Decision

`tools/github/main-ruleset.json`의 `merge_queue`를 다음으로 바꾼다.

| 매개변수 | 전 | 후 |
|---|---|---|
| `grouping_strategy` | `ALLGREEN` | `HEADGREEN` |
| `max_entries_to_build` | 3 | 2 |
| `max_entries_to_merge` | 3 | 5 |
| `min_entries_to_merge_wait_minutes` | 0 | 5 |

나머지(`SQUASH`, `min_entries_to_merge` 1, 응답 제한 120분)는 그대로다.

1. **`HEADGREEN`: 그룹의 머리 커밋만 필수 검사를 통과하면 된다.** 머리의 트리는 main에 그룹의 모든 풀 리퀘스트를
   더한 것이다. 따라서 main의 끝이 시험받지 않은 트리가 되는 일은 없다. 대신 그룹 안 앞쪽 항목의 자기 빌드가
   실패해도, 같은 그룹의 머리가 초록이면 그 항목도 병합된다(GitHub 문서의 정의).
2. **묶음은 최대 5건, 동시 빌드는 2개.** 큐가 쓰는 러너는 동시 빌드 수가 정한다. 3에서 2로 줄여 열린 풀 리퀘스트의
   실행에 러너를 돌려준다. 묶음 크기는 빌드 수와 무관하게 5까지 키운다.
3. **대기 5분은 지금 설정에서는 효과가 없다.** GitHub의 대기 시간은 "최소 그룹 크기를 채우려고 기다리는 시간"이다.
   `min_entries_to_merge`가 1이면 첫 항목에서 이미 채워지므로 기다리지 않는다. 값은 요청대로 5로 두되, 묶음은
   대기가 아니라 빌드 중에 쌓인 항목이 다음 그룹(최대 5건)이 되는 것으로 생긴다. 일부러 기다려 묶으려면
   `min_entries_to_merge`를 2로 올려야 한다(혼자 온 한 건은 5분 뒤 최소 크기를 무시하고 병합된다). 이것은 가드의
   "최소 묶음 1" 조건과 함께 바꿀 별도 결정이다.
4. **가드는 숫자가 아니라 성질을 본다.** `scripts/guard/public-github-policy.sh`는 다음을 요구한다.
   - squash 병합
   - 그룹 전략은 `ALLGREEN` 또는 `HEADGREEN`
   - 동시 빌드 1~5, 묶음 1~10
   - 최소 묶음 1
   - 대기 0~30분
   - 응답 제한 60분 이상

   "병합 ≤ 빌드" 조건은 지운다. `HEADGREEN`에서 묶음 크기와 동시 빌드 수는 서로 다른 축이다.

## 적용

정본 파일이 바뀌어도 GitHub 쪽 규칙은 저절로 바뀌지 않는다. 병합 뒤 조직 관리자가 한 번 적용하고 다시 읽는다.

```bash
id="$(gh api repos/perfectory-inc/perfectory-public/rulesets \
  --jq '.[] | select(.name == "perfectory-public-main") | .id')"
gh api --method PUT "repos/perfectory-inc/perfectory-public/rulesets/$id" \
  --input tools/github/main-ruleset.json
bash scripts/github/configure-public-repository.sh verify
```

`verify`는 세 규칙 세트를 정본과 정규화해 비교한다. 적용하기 전에는 이 규칙 세트의 차이로 실패한다.

## Consequences

- 빌드 중에 큐에 쌓인 풀 리퀘스트 여러 건이 다음 빌드 하나로 병합된다. 큐가 동시에 쓰는 러너는 최대 2개다.
- 자기 머지 그룹 빌드가 실패한 풀 리퀘스트도 뒤따르는 머리가 초록이면 병합된다. 그 실패가 뒤 항목에서 고쳐진
  경우가 아니라면 머리도 실패하므로, main의 끝은 여전히 필수 검사를 통과한 트리다.
- main의 중간 커밋(묶음 안 앞쪽 풀 리퀘스트의 squash 커밋)은 각각 시험되지 않는다. main의 끝은 늘 시험된 트리다.
  `git bisect`가 묶음 안의 커밋에 멈추면, 그 묶음의 머리 빌드 결과로 판단한다.

## Rejected alternatives

- **`ALLGREEN` 유지, 동시 빌드만 2로**: 큐의 러너 사용은 줄지만 묶음이 생기지 않는다. 건마다 빌드가 필요하다.
- **`min_entries_to_merge` 2(대기 5분과 함께)**: 큐가 한가할 때도 묶음을 만든다. 대신 혼자 온 한 건이 5분 늦어진다.
  이번에는 요청 범위 밖이라 1로 두고, 3항에 남겼다.
