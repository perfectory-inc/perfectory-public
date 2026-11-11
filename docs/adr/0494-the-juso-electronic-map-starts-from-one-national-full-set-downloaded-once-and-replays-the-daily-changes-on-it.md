# ADR 0494: 주소정보 전자지도는 한 번 받은 전국 전체본에서 시작하고, 그 위에 매일 변동분을 다시 덮는다

- Status: Accepted
- Date: 2026-11-11
- Amends: [ADR-0475](./0475-the-juso-electronic-map-is-collected-every-day-from-the-official-daily-change-linkage-with-one-approval-key.md)
  Consequences("초기 전체본은 이 연계에 없다 … 사장 판단으로 남긴다") — 그 판단을 여기서 정한다. §1–§7 은 그대로다.
- Builds on: ADR-0329 §3(`views.changed_codes`), ADR-0318(바뀐 키만 쌓는 Silver), ADR-0152·ADR-0170(체크섬으로 이름 붙인
  Bronze 키), ADR-0016(Bronze 커밋 경계), ADR-0457(사람 없는 run), ADR-0001

## Context

ADR-0475 의 변동자료 연계는 바뀐 행만 준다. 2020 년 이후 한 번도 바뀌지 않은 건물·도로는 변동 파일에 나오지 않으므로 변동분만
쌓은 Silver 는 영원히 빈 곳이 있다. 제공처 가이드도 "초기자료 구축"은 따로 받는 전체자료로 하라고 한다.

2026-11-11 제공처 자신의 다운로드 화면 API(로그인 없이 읽히는 목록, `business.juso.go.kr/api/jst/selectBabsRtlDtaDtlList`·
`selectAttrbDBDwldList`)를 읽었다.

| 항목(rtlDtaDtlSn) | 무엇 | 제공 | 공개 |
|---|---|---|---|
| 26 도로명주소 전자지도 | 11종(건물·건물군·도로구간·실폭도로·기초구간·출입구·기초구역·시도·시군구·읍면동·리), 전국 또는 시도·시군구 | **월전체만**(APLY_DTA_SE_CD 11), 201601–202610, 202611 은 아직 없음 | N — 관할 기관장 승인 뒤 내려받기 |
| 16·17·23·24 건물 도형·건물군 동 도형·도로 도형·구역의 도형 | 변동 연계의 300001·300002·300003·300006 과 같은 이름의 상품 | 월전체 + 월변동 | N |

- 받기는 로그인·장바구니 신청·본인인증·승인이 필요하다. 작업이 대신 신청하거나 받을 길이 없다(주소 API 의 키로 열리는 길이
  아니다).
- 코디네이터가 2026-11-11 **항목 26, 202610, 전국, SHP 전체분**을 신청했다(처리 중).
- 한 달 전체본 + 그 기준일 뒤의 모든 일별 변동이면 그 뒤로는 늘 최신이다. 다음 달 전체본은 필요 없다 — 변동 연계는 ADR-0475
  로 매일 사람 없이 돈다(2026-11-10 23:30Z run 에서 네 요청 코드 모두 E1001 로 답했다: 승인된 코드다. 미승인은 E1008).

큰 곳의 방식: 변경 데이터 수집은 **스냅샷 + 그 이전 위치부터의 스트림 재생**이다. Debezium 의 초기 스냅샷(그 뒤
binlog 위치부터 스트리밍), AWS DMS 의 full load + CDC, Kafka 의 compacted topic 재구축이 모두 "전체본 한 번 + 키로 덮는 변경의
순서 재생"이고, 재생 구간이 스냅샷과 겹쳐도 같은 결과다(행 전체 이미지·키 삭제는 멱등). 우리도 전체본의 정확한 기준일을 몰라도
겹치게 재생하면 된다.

## Decision

1. **기초는 항목 26 월전체 전국 한 벌, 한 번이다.** 계약 `juso-electronic-map.contract.json` 의 `base` 가 항목(26, 전국, SHP),
   inbox(`/data/foundation-platform/juso-electronic-map-base`), 달 형식, 재생 겹침(`replay_overlap_months: 1`)을 가진다. 다른
   곳은 다시 적지 않는다. 다음 달 신청은 없다. 전체본을 다시 받는 것은 기초에 결함이 드러났을 때의 예외이고, 그때는 새 달 폴더를
   하나 더 둔다(쌓기만 한다).
2. **사람 몫은 받기까지다.** 신청·본인인증·내려받기는 사람만 할 수 있다. 받은 파일은 압축도 이름도 그대로
   `<inbox>/<YYYYMM>/` 에 둔다(에이전트가 `scp`; inbox 는 릴리스가 1777 로 만들어 서비스 그룹에 없는 운영자 계정이 쓴다, /tmp
   처럼 남의 파일은 못 지운다). 옮기는 중인 파일(`.part`·`.crdownload`·`.tmp`·`.partial`, `.` 시작)은 기다린다.
3. **싣기는 수집 작업의 run 이 한다(새 작업·새 유닛 없음). 제공처 바이트는 한 번만 둔다.** `juso_electronic_map` 의 run 이
   inbox 를 읽고, 아직 싣지 않은 zip 만 그 안 목록을 읽어 계약 `base.layers` 의 레이어마다 `layer_parts`(`.shp`·`.shx`·`.dbf`)가
   `<시도코드>/` 폴더 하나에 다 있는지 본 뒤, **zip 하나를 기초 원천 `jusogokr__electronic_map_base`(계약 `base.source`, 작업
   `juso_electronic_map_base`, 기간 YYYY-MM, 잎 `base-<YYYYMM>-<시도코드>`)의 Bronze 객체 하나로** 디스크에서 흘려(유닛 3 GB
   안) 바이트 그대로 싣는다. 키는 파일의 SHA-256 으로 이름 붙인다(ADR-0152): 같은 파일은 같은 객체. 쓰기 전에 그 키를 읽어
   같은 바이트면 쓰지 않고 장부만 맞춘다(객체 잠금 버킷은 덮어쓰기에 409 로 답한다). 종류는 **(객체, 안 파일 경로)** 로
   가리킨다: 종류마다 읽을 안 파일이 run 의 `request_params.juso_base.members` 와 상태 표지에 적히고,
   `measure-bronze-object-members`(ADR-0169 §1)가 zip 안 목록 전체를 장부 옆(`catalog.bronze_object_member`)에 쌓는다. 종류의
   Silver 는 자기 레이어의 안 파일만 범위 읽기로 연다(부분 처리). 시도 zip 은 11 레이어를 한 파일에 담아 오므로, 변동분처럼
   종류마다 복사하면(ADR-0475 §3) 같은 4.6 GB 를 열한 번 쓰게 된다 — 제공처가 묶어 준 단위가 Bronze 의 단위다(S3 Inventory·
   Iceberg 의 manifest 가 큰 파일 안 위치를 가리키는 것과 같은 방식).
4. **부분 처리.** 실은 파일은 상태 디렉터리 `collector/base/<YYYYMM>/<파일>.json` 에 크기·수정 시각·체크섬·객체 키로 적고,
   다음 run 은 그 파일을 열지 않는다. 놓인 파일이 없으면 기초는 디렉터리 목록 하나다. 키가 없어도 놓인 파일은 싣는다
   (제공처에 묻지 않으므로). inbox 의 파일은 수집기가 쓰지도 지우지도 않는다.
5. **받지 못하는 것은 run 을 실패시킨다.** zip 이 아닌 파일(로그인 화면 HTML 등), 레이어가 빠졌거나 `<시도코드>/` 폴더가 하나가
   아닌 zip, 달(YYYYMM)이 아닌 폴더, 달 폴더 안의 폴더는
   manifest `base.refused` 에 적히고 run 이 실패해 OnFailure 알림이 간다. 같은 run 에서 변동 수집은 먼저 끝나 있고, 실은 것은
   실린 채다. 고쳐 두면 다음 run 이 싣는다 — 사람이 지울 차단 파일은 없다.
6. **Silver 는 종류마다 기초 + 재생이다(다음 변경에서 만든다).** 종류의 Silver = 기초 달의 그 종류 표, 그 위에 `replay_from`
   (기초 달 `replay_overlap_months` 달 전 1일: 202610 → 2026-09-01) 이후 기준일의 모든 일별 변동을 기준일·생성일 순으로 종류의
   키(계약 `kinds.*.key`)로 덮고 `Deletion.TXT` 의 키는 지운다. 겹친 날은 같은 행이 된다(§Context). 쌓기만 한다(ADR-0318:
   바뀐 키와 삭제 표지만 새 행). 기초 행도 변동 행과 같은 `legal_code` 칸으로 `views.changed_codes` 아래 행을 다시 잇는다
   (ADR-0329 §3; 적재 때 한 번 풀고 끝내지 않는다). 레이어 대응은 계약 `base.layers`(2026-11-11 실물로 잼, 아래)이다.
   `building_group` 은 기초가 없다: 기초의 `TL_SPBD_EQB` 는 건물군 윤곽(`EQB_MAN_SN`·`SIG_CD`·`OPERT_DE`)이고 연계 300002 의
   상세주소 동(`TI_SGCO_RNADR_DONG`)이 아니다. `TL_SPBD_EQB` 는 Bronze 에 그대로 있고 `base.unmatched_layers` 가 이름을 든다.

### 실측 (2026-11-11, 202610 전국 실물, W61c)

16 개 시도 zip, 합 4,591,506,695 bytes, zip 마다 35 개(`<시도코드>/` 아래 11 레이어 × 3 + `GRS80_UTMK.prj` + 배치 PDF),
`.cpg` 없음(DBF 언어 드라이버 0, 글은 CP949). 전국 행 수는 DBF 머리의 레코드 수 합이다.

| 종류 | 레이어 | 전국 행 |
|---|---|---|
| building | TL_SPBD_BULD | 10,684,580 |
| building_entrance | TL_SPBD_ENTRC | 6,414,953 |
| road_section | TL_SPRD_MANAGE | 876,133 |
| base_interval | TL_SPRD_INTRVL | 17,039,418 |
| road_width | TL_SPRD_RW | 1,567,941 |
| basic_area | TL_KODIS_BAS | 34,519 |
| legal_sido | TL_SCCO_CTPRVN | 16 |
| legal_sigungu | TL_SCCO_SIG | 256 |
| legal_emd | TL_SCCO_EMD | 5,067 |
| legal_ri | TL_SCCO_LI | 15,161 |
| (기초 없음) | TL_SPBD_EQB 건물군 | 2,168,605 |

7. **저장 위치: R2 그대로.** 보안각서의 국외 반출 금지 조항을 알고, 사장 결정(2026-11-11)으로 R2 APAC 를 받아들인다 — Cloudflare
   API 실측으로 모든 R2 버킷이 location APAC·jurisdiction default(한국 지역 없음)이다. juso 기초·변동·Silver 모두
   `foundation-platform-lakehouse-prod` 에 둔다.

## Consequences

- 사람 할 일은 한 번이다: 승인되면 마이페이지 → 다운로드 신청내역에서 받기(본인인증). 놓기·싣기·Silver 는 사람 없이 간다.
  절차는 런북 `docs/runbooks/juso-electronic-map.md` §기초 전체본 놓기.
- 크기(실측): 4.59 GB. R2 쓰기는 한 번 4.59 GB(종류마다 복사했다면 50.5 GB), 저장은 월 4.59 × $0.015 ≈ $0.07(복사했다면
  ≈ $0.76), 올리기는 50 MB/s 에서 약 92 s(복사했다면 약 1,010 s), 해시와 안 목록 읽기는 디스크에서 zip 마다 한 번(계산).
  zip 하나가 단위라 멈춰도 그 zip 만 다음 run 이 다시 한다.
- 위험: 제공처가 일별 변동을 언제까지 들고 있는지 가이드에 없다. 202610 기초의 `replay_from`(2026-09-01) 이후 변동은 지금
  커서(2020-01-01 부터)가 따라잡는 구간 안이라 받는다.
- 가드: `juso_electronic_map_base` 의 시험이 놓인 zip 이 기초 원천에 같은 바이트로 한 번만(종류마다가 아니라) 실리고 종류마다
  안 파일이 적힘, 다음 run 은 열지 않음, zip 이 아니거나 레이어가 빠진 zip 은 아무것도 싣지 않고 실패, 옮기는 중인 파일은 기다림, 재생 시작일을 증명한다;
  `test_juso_electronic_map_command.py` 가 키 없이도 놓인 파일이면 수집기가 돈다는 것과 inbox 가 작업 상태 경로임을 증명한다.
  막는 사고: 변동분만으로 만든 Silver 가 2020 년 뒤 안 바뀐 건물·도로를 영원히 빠뜨리는 것(ADR-0475 Consequences),
  내려받기 화면이 로그인 HTML 을 준 것을 zip 으로 싣는 것, 시도 zip 16 개를 11 종류마다 복사해 4.6 GB 를 50.5 GB 로 쓰는 것
  (#1275 첫 판, 실물 배치로 발견).
