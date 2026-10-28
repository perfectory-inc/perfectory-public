# ADR 0303: 각 by-PNU 서빙 레인은 독립 스케줄 파이프라인이다 — 무관한 레인은 서로의 bake를 막지 못한다

- Status: Accepted
- Date: 2026-10-28
- Builds on: [ADR-0193](./0193-each-data-kind-is-its-own-gold-table-and-by-pnu-lane.md)(종류=레인), [ADR-0138](./0138-scheduled-jobs-share-one-pool-sized-by-every-slot-combination.md)(풀·메모리 예산), [ADR-0274](./0274-the-by-pnu-bake-does-only-the-work-a-change-needs.md)(바뀐 만큼만 굽기), [ADR-0292](./0292-public-sale-servable-gold-carries-only-host-panel-pnus.md)(서버빌리티는 Gold 에서 해결)
- 관련 사건: 2026-10-28, 필지 by-PNU 프로필 v1→v2 전량 재굽기(느린 전국 작업)가 `by_pnu_serving_bake all` 맨 앞에서 돌자, 전혀 의존하지 않는 공매 사진(같은 잡 맨 끝)이 몇 시간 발행되지 못했다.

## Context

by-PNU 서빙 bake 는 한 잡 `by_pnu_serving_bake`(`by-pnu-serving-bake.sh all`)이 필지 → 건물 → 팩-온리 레인들을 **한 프로세스에서 순차**로 굽는다. 순차인 이유는 메모리뿐이다: 호스트 예산(ADR-0138)은 한 번에 한 레인의 export(필지 `MemoryMax=14G`)만 담는다. 이 순차 묶음의 **부작용은 무관한 레인이 서로를 막는 것**이다 — 2026-10-28 필지 프로필 v2 전량 재굽기가 맨 앞에서 느리게 돌자, 공매(맨 끝)가 돌 차례를 몇 시간 못 받았다.

ADR-0193 은 이미 "종류마다 자기 Gold·자기 레인"을 정했고, 공매는 자기 Gold 잡 `gold_public_auction_rebuild`(Silver 가 시간당 커밋되므로)까지 있다. 그런데 **bake 실행만 다시 `all` 한 배치로 묶여** 그 독립성을 잃는다.

`all` 안의 `documents_follow_lane` 게이팅은 흔한 오해와 달리 **순서 대기가 아니다**: 레인 L 의 docs 가 레인 F 를 따를 때, **F 가 이번 런에서 실패/대기했으면만** L 을 건너뛴다(`by-pnu-serving-bake.sh:111–123`). F 가 (느리더라도) 성공하면 L 은 정상적으로 그 뒤에 굽힌다. 즉 공매를 막은 것은 "따름"이 아니라 **순차 실행**이다.

대기업 데이터 플랫폼은 각 데이터 제품을 독립 파이프라인으로 둔다: 자기 트리거(자기 상류가 바뀌면 혼자 materialize), 자기 freshness SLA, 자기 관측, 그리고 **무관한 파이프라인이 서로를 막지 않음**. 실행을 자원 효율로 묶더라도 그 독립성은 자산 수준에서 유지한다.

## Decision

1. **비차단이 불변식이다.** 서로 의존하지 않는 두 by-PNU 레인은 하나가 다른 하나의 bake 를 막지 못한다. 모든 레인을 한 프로세스에 순차로 묶는 `all` 구조를 폐기한다(아래 증분으로).

2. **자기-생산 레인은 자기 bake 잡으로, 패널 bake 와 동시에 굽는다.** 자기-생산(own-producer) 종류 — 자기 Gold 생산자를 갖고(`producer` ≠ null, `follows_host_panel=false`) **서버빌리티가 Gold 단계에서 해결되는**(ADR-0292 `servable()` + `section_coverage`) 종류, 즉 공매와 앞으로 경매 — 는 `by-pnu-serving-bake.sh <lane>` 를 도는 **전용 잡**을 갖는다. 자기 Gold 자산이 커밋되면 트리거된다(`started_by: inputs`). 패널/필지 bake 와 **별도 풀 슬롯**으로 동시 실행된다. `documents_follow_lane` 의 within-run 커플링은 **"서빙 중인 host 세대 기준으로 굽는다"** 로 대체된다 — 독립 잡은 항상 *발행된* 필지 상태를 읽으므로(실패/진행 중 세대는 서빙되지 않는다) 이전 게이트보다 **더 강한 일관성**을 준다.

3. **자원 등급을 슬롯·MemoryMax 로 표현한다.** 무거운 레인(필지·건물 패널, transfer-history)은 메모리 때문에 동시 실행 불가 — 하나의 무거운 bake 단위로 순차 유지하거나 슬롯으로 상호배제한다. 가벼운 독립 레인은 1 슬롯·작은 `MemoryMax`(≈8g, 전-파일 디코드 바닥 ~7.8GB)로 동시 실행한다. ADR-0138 메모리 예산의 **최악 슬롯 조합이 62g 안에 들도록** 슬롯·MemoryMax 를 정하고, `scripts/guard/every-container-has-a-memory-cap.sh` 가 이를 검증한다. (막는 사고: 호스트 메모리 초과로 OOM/스왑 스래싱이 나 서빙·수집이 멈추는 것.)

4. **패널의 섹션인 종류(producer=null)는 패널과 함께 굽는다.** host 패널에 핀된 섹션(`follows_host_panel=true`: 5 개 필지 kind, 단가)은 패널 스냅샷에 고정되므로 패널과 같은 잡/런에서 굽는다. 이들의 독립 분리(증분 2)는 "리더 자산 트리거"로 순서를 재표현해야 하므로 범위 밖으로 둔다.

5. **각 레인은 자기 freshness SLA 를 갖는다**(위생 A/C 계열, ADR-0299 과 연결): 한 레인이 늦으면 그 레인만 경보하고, 무관한 레인의 신선도를 가리지 않는다. (증분 2.)

## Consequences

- 공매(그리고 경매) 서빙이 필지 전량 재굽기에 **영구히 안 막힌다.** 2026-10-28 사건이 구조적으로 재발 불가.
- **증분 1(이 PR)**: 자기-생산 공매 레인만 전용 bake 잡으로 분리한다. `all` 은 공매를 더 굽지 않고, 새 잡이 공매 Gold 자산에 트리거돼 동시에 굽는다. 새 잡은 1 슬롯이라, 같은 1 슬롯인 필지 bake(14g)·시간당 onbid 공매 Silver(2.5g)와 **세 잡이 3 슬롯을 함께 채울 수 있다**: 공매 bake `MemoryMax=8g` 면 최악 스파크 조합이 `14 + 8 + 2.5 = 24.5g` 가 되어 호스트 총합이 `standing 33.95g + 24.5g + reserved 5g = 63.45g` 로 62g 를 넘는다. 그래서 공매 bake 는 `MemoryMax=5g` 로 둔다(작은 레인의 보수적 상한 — gold.parcel_public_auction 은 약 4,621 행·4,677 섹션의 작은 JSON 이라 충분하고, 첫 운영 런의 실측 피크로 후속에서 조인다; 8g 은 이 종류가 닿지 않는 필지 레인의 전-파일 디코드 바닥이었다). 5g 면 최악 스파크 조합이 `14 + 5 + 2.5 = 21.5g < 폴드 8g + 필지bake 14g = 22g`(= 최대 one-shot 인 릴리스 빌더 22g 와 같음)이라 호스트 총합이 **현행과 동일한 60.95g**(62g 내, `scripts/guard/every-container-has-a-memory-cap.sh` 통과). 공매 bake 가 MemoryMax 로 죽어도 레인은 완료 시에만 원자적으로 발행하므로(실패한 bake 는 서빙 중인 세대를 그대로 둔다) 손상이 없다. 스파크 풀에 잡이 +1 되어 `test_job_specs` 의 스타베이션 상수(시간·슬롯 기준이라 메모리와 무관)와 `host-memory-budget.contract.json` 항목을 함께 갱신한다.
- **증분 2(후속)**: 무거운/가벼운 트랙 분리 + 따라가는 섹션 종류를 리더 자산 트리거로 재배선 + 레인별 freshness SLA. 스타베이션 경계 계산이 풀 멤버 수에 세제곱이라(DAG import < 5s, `job_specs.py`), 레인 수가 크게 늘면 그 알고리즘을 선형화하거나 등급별 소수 잡으로 묶는다.
- **물리적 격리(레인마다 전용 컴퓨트)는 비용**이므로 범위 밖이다. 이 결정은 그 격리로 가는 길을 막지 않는다: 각 레인이 이미 자기 잡·자산·SLA 를 가지면, 무거운 레인을 탄력 컴퓨트로 옮기는 것은 재설계 없이 가능하다.

### 기각한 대안

- **레인마다 전면 1 슬롯 잡(8+개)**: 1 슬롯 14g 짜리 3 개가 동시 실행 가능 = 42g 로 62g 예산 초과; 스타베이션 세제곱 + DAG import 예산 초과. 탄력 컴퓨트(비용) 없이는 이 호스트에서 성립 불가. 증분 2 에서 자원-등급 묶음으로 수렴한다.
- **현행 `all` 유지**: 무관한 레인이 계속 서로를 막는다(이 사건의 원인). 기각.
- **단순 재배열(공매를 `all` 맨 앞으로)**: 여전히 순차라, 공매가 돌 때 필지가 그 뒤에 막히고, 이미 돌던 필지 뒤에 공매가 들어오면 또 막힌다. 비차단 불변식을 만족하지 못한다. 기각.
- **공매 Gold 의 servable 커플링을 bake 로 옮겨 패널을 bake 시 읽기**: 불필요한 bake-시 의존을 새로 만든다. 서버빌리티는 이미 Gold 에서 `section_coverage` 로 해결된다(ADR-0292). 기각.
