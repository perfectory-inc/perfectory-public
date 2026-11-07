# ADR 0437: 코드 판을 적지 않은 Gold 는 입력이 움직이면 전량을 키로 게시하고, 모니터는 404 도 레인이 서빙하는 것에 맞춰 본다

- Status: Accepted
- Date: 2026-11-07
- Amends: [ADR-0321](./0321-a-gold-commit-records-the-code-that-built-it.md) §4 의 "기록이 없는 머리는 지금 코드로 만든 것으로 받아들인다"
- Builds on: [ADR-0180](./0180-panel-gold-recomputes-only-the-pnus-whose-inputs-changed.md)(증분·표본 parity), [ADR-0321](./0321-a-gold-commit-records-the-code-that-built-it.md)(코드 판·Write-Audit-Publish), [ADR-0306](./0306-a-by-pnu-lane-alerts-when-what-it-serves-is-older-than-its-freshness-budget.md)(서빙 신선도), [ADR-0151](./0151-building-packs-serve-each-document-whole.md)·[ADR-0160](./0160-one-by-pnu-gateway-source-serves-both-lanes.md)(매시 서빙 모니터), [ADR-0301](./0301-deploy-stops-an-interruptible-job-rather-than-waiting-hours-and-a-long-pause-alerts.md)·[ADR-0308](./0308-the-deploy-stops-only-the-jobs-its-release-holds-and-skips-a-step-whose-inputs-did-not-change.md)(배포가 멈추는 작업)
- 관련 사건: W20(SEV1), 2026-11-07 07:17Z — 필지 by-PNU 레인이 2026-10-30T06:49Z manifest 를 192시간째 서빙(예산 48시간). building 190h, parcel-transfer-history·parcel-forest-ledger 192h. 공매 레인은 11-06 21:54Z 에 발행했는데도 모니터가 매시 실패.

## Context

실측(읽기 전용, ai-server journal·`/var/lib/foundation-gold-panel-rebuild/*/runs`·Trino `$snapshots`, 2026-11-07):

| 무엇 | 측정값 |
|---|---|
| `gold.parcel_panel` 마지막 생산자 커밋 | 2026-10-26 04:44Z(overwrite). 10-30 05:07Z 는 compaction(`replace`) 뿐 |
| 그 커밋의 코드 판(`foundation.build-source-sha256`) | **없음** — ADR-0321(10-28 병합) 이전 커밋. 계획 `code_version_recorded: false` |
| 11-03..11-06 parcel 재생성 9회 | 커밋 0. 시간 초과(160분) 4회, 배포 step 0 이 멈춤 5회 |
| 끝까지 간 회차의 단계(11-06 12:11Z) | affected 4,541 s → restrict 2,409 s → build 8 s → parity 531 s → **parity 불일치**(20,258 개 중 1 개) → 전량 폴백 → 14:50Z 시간 초과 |
| 같은 표본 | 핀이 같으면 표본 seed 가 같아(`pins_seed`) 11-05·11-06 같은 PNU 가 매번 불일치 |
| 전량 재생성 실측 | 10-19 5,555 s(커밋됨) |
| `docker … removal … already in progress` | 시간 초과·SIGTERM 뒤 `pinned-cleanup.sh` 의 정리 줄 — 원인이 아니라 결과 |
| by-PNU 굽기(11-01..11-06 매일) | 모든 패널 레인 "nothing to do: 서빙 팩이 이미 Gold 스냅숏 X 를 반영" — Gold 가 안 움직였을 뿐 |
| building 패널 | `all` 에서 parcel 다음 차례라 11-02 이후 한 번도 계획되지 않음 |
| 공매 모니터 | 표본(10-24 cut-over 의 동일성 증거) 20 개 중 4 개가 404. 공매는 공고가 끝나면 PNU 가 빠지는 레인 |

ADR-0321 §4 는 "기록이 없는 머리는 지금 코드로 만든 것으로 받아들인다"를 택했다. 배포 직후 모든 Gold 를 한 번씩 전량
계산하지 않기 위해서였고, 대가는 "그 사이 코드 변경은 표본만 잡는다"였다. 표본은 잡았다. 그러나 잡은 뒤의 길 —
증분 단계(약 2시간) 다음의 전량 폴백 — 이 작업 시간 한도를 넘어 **매 회차 같은 지점에서 죽었고**, 다음 회차는 같은
핀·같은 표본으로 같은 일을 되풀이했다(라이브락). 커밋이 없으니 판도 영원히 적히지 않았다.

공매 모니터는 표본 PNU 마다 200 과 문서를 요구한다. 공매 레인은 공고가 끝난 PNU 를 tombstone 으로 내리므로 그 PNU 의
404 는 **레인이 서빙하는 그대로의 답**인데, 모니터는 그것을 실패로 셌다.

## Decision

1. **기록 없는 코드 판은 "모름"이다.** `gold_rebuild.plan` 은 현재 Gold 가 코드 판을 적지 않았고 **입력이 이미 재생성을
   요구할 때**(이유가 하나라도 있을 때) 코드가 바뀐 경우와 똑같이 계획한다: `mode: full`, `publish: changed_keys`
   (ADR-0321 §5 의 Write-Audit-Publish — 그림자 브랜치에 전량, `row_digest` 가 다른 키만 main 에 병합·기록). 이 커밋이
   코드 판을 적으므로 다음부터는 증분이다. 입력이 안 움직인 Gold 는 그대로 둔다 — 배포 직후 전 Gold 일괄 전량을
   피한다는 ADR-0321 §4 의 이유는 그대로 지킨다.
   - 전량은 선언된 예외다(ADR-0321 §4 와 같은 이유: 코드 변경이 어느 행에 닿는지 코드에서 유도할 수 없다). 비용: Gold
     하나당 한 번, 필지 약 5,555 s(10-19 실측) + 비교·병합. 증분 2시간을 먼저 버리던 것보다 짧다.
   - 대기업 사례: Bazel 은 액션 캐시 키가 없으면 다시 만든다(없는 키를 "같다"로 보지 않는다). dbt 의 `state:modified`
     는 비교할 manifest 가 없으면 전부 modified 로 본다.
2. **서빙 모니터는 404 도 레인에 맞춰 본다.** 표본 PNU 가 404 이고 그 대상(서빙 manifest·세대·기록 상태)의 팩이 그
   PNU 를 tombstone 또는 absent 로 답하면 그 답은 충실하다(`absent_as_served`). 팩이 문서를 가진 PNU 의 404 는 여전히
   실패다. 표본 어느 PNU 에도 문서가 없으면(표본이 다 빠짐) 모니터가 아무것도 검사하지 못하므로 "새 표본을 뜨라"는
   이유로 실패한다. 판정은 `monitor::judge_serving` 하나.
3. 가드가 막는 실제 사고(루트 ADR-0001): **2026-11-03..06 parcel Gold 가 같은 parity 불일치로 9회 연속 커밋 없이 끝나
   필지·건물·종류 레인이 8일 낡은 문서를 서빙했다**(§1, `test_a_gold_built_before_the_record_is_rebuilt_whole_and_published_by_key_once_its_inputs_move`).
   **2026-11-07 공매 레인이 11-06 에 발행했는데도 끝난 공고 4건의 404 때문에 모니터가 매시 실패했다**
   (§2, `a_404_for_a_pnu_the_lane_serves_no_document_for_passes`).

## Consequences

- 다음 parcel 재생성은 증분 단계 없이 전량+키 게시로 간다. 커밋되면 by-PNU 굽기가 바뀐 키만 패치로 발행하고, 그
  뒤 building 이 같은 길로 따라온다(같은 `all` 실행 시간 안에 못 들면 다음 실행).
- 공매 모니터는 끝난 공고를 실패로 세지 않는다. 표본이 계속 줄어드는 것(새 표본을 뜨는 주체)은 cut-over 단계의 몫이고
  W18(ADR-0435)에 넘긴다.
- 이 결정 밖, 함께 측정해 넘긴 것:
  - 배포 step 0 이 `interruptible` 재생성을 멈춘다(ADR-0301). 11-06 배포 14회 중 9회가 "모든 표"를 바꾼다고 판정해
    53개 작업을 잡았다 — 바뀐 입력이 마이그레이션의 per_table 이 아닌 파일(새 원천 계약 파일 하나, Spark 작업 하나)이면
    모든 표로 본다(`release_changes.changed_tables`). 재생성 한 번이 2시간 안팎이라 낮에는 거의 끝나지 못한다. 후속 PR.
  - zoning·price·characteristics 의 488h 는 원천(연 1회 공개)이 10-17 이후 바뀌지 않아 Gold 도 안 움직인 것이다. Silver
    갱신은 11-04 부터 `flate2` 로 실패 중(W19). 레인별 원천 주기를 모르는 48시간 일률 예산의 오경보 여부는 별도 결정.
