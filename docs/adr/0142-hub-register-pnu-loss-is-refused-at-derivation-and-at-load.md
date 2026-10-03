# ADR 0142: 허브 대장의 PNU 손실은 조립에서 이름으로 거부하고, 적재에서 비율로 거부한다

- Status: Accepted
- Date: 2026-10-04

## Context

2026-09-27 에 `silver.building_register_titles` 가 통째로 덮어써졌다(Iceberg `overwrite`,
8,064,406 행). 그 스냅숏에서 대지구분 `0`(대지) 행의 PNU NULL 은 916,469 행이고, 그중 916,461 행이
통합 시도 12(전남광주통합특별시) 소속이다. 시도 12 의 대지 행은 **전부** PNU 를 잃었다. 직전
스냅숏의 대지 NULL 은 8 행이었다. Gold 재빌드(ADR-0139)는 이 입력으로 `gold.building_panel` 이
5,939,794 → 5,268,380 행으로 줄어드는 것을 보고 거부했다 — 그 거부가 이 결함을 드러냈다.

원인은 저장소 main 의 코드가 아니다. 증거는 이렇다.

- 스냅숏 요약에는 `foundation.ingest-write-id` 속성이 있고, 원천 스냅숏 id 가
  `building-register-title-bronze-<bronze_object_id>` 형식이다. 두 표지 모두 main 에는 없다.
  병합되지 않은 코덱스 무결성 체인지셋(로컬 커밋으로만 보존, `remote_lakehouse_job/titles.rs`)에만
  있다.
- 그 트리는 `sigungu_crosswalk.rs` 를 바꿨다. 씨앗 27쌍 전부를 "출처가 선언하지 않은 후보"로
  강등하고, `resolve_sigungu_code_via` 가 강등된 코드에 `None` 을 돌려주게 했다. 그러면 시도 12 의
  모든 시군구 코드에서 PNU 가 사라진다. 경고나 개수는 남지 않는다.
- 노트북 WSL 에서 보존된 그 실행의 핸드오프 JSONL 은 스냅숏과 행 단위로 일치한다. 행 수, NULL 965,150,
  시도 12 NULL 928,891, 시도 12 대지 NULL 916,461 이 모두 같다. 파일의 수정 시각은 Spark 앱 시작
  50초 전이다.
- 같은 트리가 2026-09-30 에 `silver.building_register_units` 도 덮어썼다. 시도 12 대지 행
  1,070,125 개가 전부 NULL 이다. 그 직전(09-26) 스냅숏의 시도 12 대지 NULL 은 0 이었다.

두 가지 구조적 구멍이 이 사고를 가능하게 했다.

1. **조립이 시도 하나를 통째로 조용히 버릴 수 있었다.** main 에서도 크로스워크에 없는 12xxx
   코드는 그대로 통과(identity)했다. 지적도에 12xxx 필지는 없으므로 결과는 고아 PNU 였다. 코덱스
   트리에서는 그 결과가 NULL 이었다. 어느 쪽도 실패로 보이지 않았다.
2. **적재 게이트는 행만 봤다.** `pnu` 는 블록(대지구분 `2`) 때문에 nullable 이다. 그래서 행 단위
   게이트는 전부 통과했다. 대지 NULL 비율이 1e-6 에서 0.116 으로 뛴 것은 아무도 재지 않았다.

## Decision

1. **통합 시도는 크로스워크가 다스린다.** `foundation_shared_kernel::pnu::SigunguCrosswalk` 는
   매핑(`current → superseded`)과 *다스리는 시도*를 함께 가진다. 다스리는 시도의 시군구 코드에
   매핑이 없으면 `UnmappedGovernedSigungu(<코드>)` 로 거부한다. 통과나 NULL 은 없다. 다스리지 않는
   시도의 코드는 그대로 조립한다. 다스리는 시도 목록은 씨앗 계약
   `sigungu-canonical-crosswalk.contract.json` 의 `sido[].current_code` 에서 읽는다. 따로 적은 목록은
   없다. 적재기 `hub_sigungu_crosswalk` 는 다음 둘도 거부한다. 하나는 다스리는 시도 밖의 매핑이고,
   다른 하나는 대상이 그 시도의 `supersedes` 밖에 있는 매핑이다.
2. **조립의 거부는 내보내기의 실패다.** 표제부·전유부·면적 계획의 `_via` 파서와 허브 공통
   내보내기(`hub_register_silver_export`)는 이 거부를 줄 번호와 함께 그대로 올린다. 그래서 Bronze
   에 새 통합 코드가 나타나면 내보내기가 그 코드를 이름으로 대며 멈춘다. 크로스워크가 Bronze 에
   있는 모든 시군구를 덮는지는 내보낼 때마다 전수로 검사된다.
3. **적재는 대지 PNU NULL 비율의 상승을 거부한다.** 허브 대장 Silver 세 표(표제부·전유부·면적)는
   계약 품질 게이트로 `ordinary_land_pnu_null_share_increase <= 0.001` 을 선언한다. 이 값의 정본은
   `lakehouse-domain` 의 `ORDINARY_LAND_PNU_NULL_SHARE_GATE` 하나다. `silver_scalar_handoff_to_lakehouse.py`
   는 쓰기 전에 두 비율을 잰다. 하나는 후보의 대지(`register_parcel_key` 11번째 글자 `0`) PNU NULL
   비율이고, 다른 하나는 대상 표의 현재 스냅숏의 같은 비율이다. 후보가 허용폭을 넘게 높으면 아무것도
   쓰지 않고 실패한다(`pnu_null_share_guard.py`). 파일 묶음 적재는 첫 묶음을 쓰기 전에 전체를 판정한다.
   `--validate-only` 에서도 같은 판정을 낸다. 표가 없으면 `no_baseline` 이라는 이름 붙은 결과를
   출력한다.
4. **허용폭 0.001 의 근거.** 09-27 상승폭은 0.116 이었고, 가장 작은 12xxx 시군구 하나는 표제부 대지
   행의 약 0.2% 다. 0.001 이면 시도 하나는 물론 그 시군구 하나만 잃어도 걸린다. 정상 재적재의
   변동은 이보다 세 자릿수 작다(대지 NULL 8 행 / 약 790만).

이 게이트가 막는 실제 사고는 2026-09-27 표제부 덮어쓰기와 2026-09-30 전유부 덮어쓰기다. 둘 다
시도 하나의 PNU 를 전부 잃은 Silver 가 정본 표에 들어갔다.

## Consequences

- 바로잡은 표제부·전유부 Silver 는 main 의 내보내기로 다시 만든다. 쓰기 방식은 운영 결정으로 남긴다.
  `append` 로 쓰면 같은 Bronze 에서 나온 두 파생이 한 스냅숏에 공존한다. 그러면 `gold.building_panel`
  의 단일 `source_snapshot_id` 검사와 `mgm_bldrgst_pk` 유일성 검사가 Gold 를 거부한다. `overwrite`
  는 새 스냅숏을 만들고, 잘못된 스냅숏은 Iceberg 이력에 남는다.
- `SigunguCrosswalk` 가 `HashMap` 을 대신하므로 `_via` 파서 셋의 시그니처가 바뀐다. 정체성
  크로스워크는 `SigunguCrosswalk::identity()` 다.
- 새 통합 시도가 생기면 씨앗 계약의 `sido` 와 `sigungu` 를 함께 늘려야 내보내기가 돈다. 시도만
  선언하고 짝을 빠뜨리면 내보내기가 빠진 코드를 대며 멈춘다.
- `silver.building_register_unit_areas` 는 아직 2026-06 Bronze 그대로라 12xxx 행이 없다. 이 결함은
  없지만 석 달 넘게 낡았다. 다시 적재하면 이 ADR 의 두 거부가 처음으로 실물에서 돈다.
