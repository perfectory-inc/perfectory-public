---
status: current
owner: foundation-platform
doc_type: reference
last_reviewed: 2026-09-12
---

# 지리 정체성 Wave 1 — 기술 설계 (ADR-0103 구현 사양)

[ADR-0103](../adr/0103-place-identity-outlives-administrative-code-changes.md)의 6기둥과
[대기업 사례 전수조사](./geography-identity-enterprise-survey.md)를 구현 가능한 사양으로 옮긴다.
방향은 ADR-0103 **Revision(2026-09-12)** 기준: 정본(canonical)은 **권위기관 현행 코드(현재 12)**,
29/46은 `superseded`, 어떤 원본 코드도 덮어쓰지 않는다(옵션 가 — 별도 안정 식별자).
저장 원칙: 아래 표들의 SSOT는 레이크하우스 Iceberg(`reference.*`, append-only)이고, PostgreSQL은
제약 검증과 스튜어드 UI 를 위한 **재구성 가능한 투영**이다. 제약 위반 = 투영 실패 = 표류 경보(기둥 ⑤).
DDL은 PostgreSQL 18 문법으로 적는다(투영 측 계약이자 컬럼·타입의 정의).

## §1 안정 ID 스킴 — 결정: **민팅 UUIDv4 + 크로스워크** (UUIDv5 기각)

| 기준 | 민팅 UUIDv4 + 크로스워크 (채택) | UUIDv5(네임스페이스+앵커 해시) (기각) |
|---|---|---|
| 앵커 의존 | 없음 — ID는 무의미·불투명 | **안정 앵커가 필요한데 그런 앵커가 없다.** PNU는 분필·합필·재코딩에 바뀌고(문제 2), 코드는 통합에 바뀐다(문제 1). `hash(pnu)` 결함을 한 층 위로 옮길 뿐 |
| 앵커 정정 시 | ID 불변, 크로스워크 행만 정정 | ID가 바뀌거나(내구성 파괴) 정의와 어긋난 채 방치(거짓말) |
| 재실행 멱등성 | 해석기 선(先)조회 후 민팅으로 확보 (§1 M2) | 결정적 재계산으로 확보 — 유일한 장점 |
| 선행 사례 | **Kimball durable "supernatural" key**(무의미·1회 부여·자연키 변경 생존), **Overture GERS**(민팅 128비트 UUID + Registry/Changelog/Bridge 로 안정성을 *운영으로* 유지, 해시 파생 아님), **Who's on First**(`wof:id` 불투명 민팅, 속성에서 파생 안 함, 재사용 금지) | OSM/Nominatim 재임포트 ID 재생성 = 우리 원래 버그의 동형(반면교사) |

UUIDv5의 멱등성 이점은 민팅을 **단일 관문**(ADR-0069 "적재 정체성은 한 곳에서 정해진다")에
두고 "resolve-먼저, 실패 시에만 mint"로 대체한다. 민팅 규칙:

- **M1** 민팅은 레지스트리 서비스(관문) 한 곳에서만. 다운스트림 잡은 절대 ID를 만들지 않는다.
- **M2** 민팅 전 반드시 `resolve()`(§3) 선조회. 기존 대상이면 기존 `stable_id` 반환(멱등).
- **M3** ID 재사용 금지·삭제 금지(WOF·ONS "코드 재사용 금지"). 폐쇄는 `status='historic'`.
- **M4** 개칭·재코딩은 새 ID를 만들지 **않는다**(ONS: 실질 경계 변경만 새 코드). 크로스워크 행 추가로 흡수.
- **M5** 분필·합필 등 실질 변경은 새 ID + 계보 엣지(§4). LINZ: 대체되는 필지는 historic, 빈틈없이 승계.
- **M6** `stable_id`는 `gen_random_uuid()`(v4). 내부 행 대리키는 PG18 `uuidv7()` 허용(인덱스 지역성; 둘 다 민팅-불투명이므로 스킴 결정과 무관).

## §2 레지스트리 + 양시간 크로스워크 (기둥 ①②⑥)

ONS **RGC/CHD 2분할**을 그대로: 기반 표는 추가전용 전체 이력(CHD), "살아있는 등록부"(RGC)는 뷰.
양시간 축 2개 — 유효시간 `valid_from/valid_to`(실세계, SQL:2011 application-time) +
시스템시간 `asserted_at/retracted_at`(기록, 정정은 retract+재삽입, UPDATE/DELETE 금지).

```sql
-- 정체성 등록부: 정체성과 생애만. 코드·이름은 전부 크로스워크의 속성(Overture Bridge·Wikidata 원칙).
CREATE TABLE geo_identity.place_registry (
    stable_id    uuid        PRIMARY KEY DEFAULT gen_random_uuid(),     -- M6
    entity_type  text        NOT NULL CHECK (entity_type IN
                             ('sido','sigungu','emd','ri','parcel','building','unit')),
    status       text        NOT NULL DEFAULT 'current'
                             CHECK (status IN ('current','historic')),  -- LINZ lifecycle
    minted_at    timestamptz NOT NULL DEFAULT now(),
    minted_by    text        NOT NULL,          -- 관문 잡 id 또는 'steward:<user>'
    retired_at   timestamptz,                   -- status='historic' 전환 시각; 행 삭제는 없음 (M3)
    CHECK ((status = 'historic') = (retired_at IS NOT NULL))
);

-- 양시간 크로스워크: stable_id <-> 외부 코드. ONS CHD + SQL:2011, PG18 WITHOUT OVERLAPS.
CREATE TABLE geo_identity.code_crosswalk (
    crosswalk_id   uuid   PRIMARY KEY DEFAULT uuidv7(),
    stable_id      uuid   NOT NULL REFERENCES geo_identity.place_registry (stable_id),
    code_system    text   NOT NULL CHECK (code_system IN
                          ('kr_legal_dong_sido',     -- 시도 2자리 (예: '12','29','46')
                           'kr_legal_dong_sigungu',  -- 시군구 5자리 (예: '12240','29140')
                           'kr_legal_dong',          -- 법정동 10자리
                           'kr_pnu')),               -- 필지 19자리
    external_code  text   NOT NULL,                  -- 원본 코드 그대로, 무손실 보존 (기둥 ⑥)
    display_name   text,                             -- 그 코드 기준 명칭 (예: '전남광주통합특별시')
    is_canonical   bool   NOT NULL,                  -- 권위기관 현행 코드인가 (Revision: 12=true, 29/46=false)
    valid_from     date   NOT NULL,                  -- 유효시간(application-time): 권위기관 생성일자
    valid_to       date,                             -- 말소일자; NULL = 현행 (반개구간 [from, to))
    valid_during   daterange GENERATED ALWAYS AS
                     (daterange(valid_from, valid_to, '[)')) STORED,
    supersedes     text[] NOT NULL DEFAULT '{}',     -- 같은 code_system의 선행 코드 (WOF 양방향 포인터)
    superseded_by  text[] NOT NULL DEFAULT '{}',     -- 후속 코드; 합병 N:1·분할 1:N을 배열로
    source         text   NOT NULL,                  -- 'mois:getStanReginCdList' | 'molit:15122617'
                                                     -- | 'derived:pnu-tail-join:2026-09' | 'steward:<user>'
    source_record_id text,                           -- 원천 레코드 식별자 (계보는 실물 명명, ADR-0086)
    asserted_at    timestamptz NOT NULL DEFAULT now(), -- 시스템시간: 우리가 이 사실을 안 시각
    retracted_at   timestamptz,                        -- 정정 시 철회 시각; 철회 후 새 행 삽입
    -- SQL:2011 application-time 유일성: 한 코드는 한 시점에 하나의 장소만 가리킨다 (PG18)
    CONSTRAINT code_points_at_one_place
        UNIQUE (code_system, external_code, valid_during WITHOUT OVERLAPS)
);

-- '살아있는 등록부'(ONS RGC 대응): 현행 코드만 보는 뷰. 서빙 표시는 is_canonical 행의 코드·명칭.
CREATE VIEW geo_identity.code_register_current AS
SELECT * FROM geo_identity.code_crosswalk
WHERE valid_to IS NULL AND retracted_at IS NULL;
```

- 유일성 제약은 `retracted_at IS NULL` 인 활성 행에만 의미가 있으므로, 투영 적재기는 철회
  행을 제외하고 적재한다(Iceberg 원본은 전 이력 보존).
- 광주 사례의 데이터 형태: `('kr_legal_dong_sigungu','29140', valid_to=2026-07-01, superseded_by={'12240'…}, is_canonical=false)` 와
  `('kr_legal_dong_sigungu','12240', valid_from=2026-07-01, supersedes={'29140'}, is_canonical=true)` 가
  **같은 `stable_id`가 아니라 각자의 장소 엔티티**에 붙고, 장소 간 승계는 §4 계보 엣지가 나른다.
  같은 필지의 옛 PNU(29140…)·새 PNU(12240…)는 `kr_pnu` 행 두 개가 **같은 `stable_id`** 를 가리킨다
  (재코딩 = 정체성 불변, M4).
- Wave 1의 `reference.sigungu_canonical_crosswalk`(계획 Task 2)는 이 일반 표에서
  `code_system='kr_legal_dong_sigungu'` 를 물질화한 투영이다. 시드 27쌍의 provenance 는
  `derived:pnu-tail-join:2026-09`, 권위 수집(Task 1) 후 `mois:*` 행으로 대체된다.

## §3 해석기 API (기둥 ③)

유일한 코드 처리 지점. Rust(`foundation-shared-kernel`)가 정의 계약이고, Spark 쪽은 동일 의미의
표 함수로 노출한다. **정확한 시그니처:**

```rust
pub enum CodeSystem { Sido, Sigungu, LegalDong, Pnu }

pub struct ResolveInput<'a> {
    pub code_system: CodeSystem,
    pub raw: &'a str,          // 원본 코드/PNU 그대로 (보존, 기둥 ⑥)
    pub as_of: NaiveDate,      // 소스 data vintage
}

pub enum MatchGrade { Certain, Probable, Possible }   // §5 EMPI 버킷

pub struct Candidate {
    pub stable_id: Uuid,
    pub canonical_code: String,   // 반드시 code_register_current의 is_canonical 행에서 복사
    pub grade: MatchGrade,
    pub evidence: String,         // 'crosswalk:mois' | 'succession-chain' | 'pnu-tail-join' | 'geometry-overlap'
}

pub enum QuarantineReason { Malformed, UnregisteredCode, AmbiguousSplit, SourceConflict, NotValidAtDate }

pub enum Resolution {
    Resolved   { stable_id: Uuid, canonical_code: String, grade: MatchGrade },
    Quarantined { reason: QuarantineReason, candidates: Vec<Candidate> },  // 후보 동봉 → §5 큐
}

pub fn resolve(xw: &dyn CrosswalkStore, input: ResolveInput<'_>) -> Resolution;
```

Spark 표 함수: `resolve_geo(code_system string, raw string, as_of date) -> struct<stable_id string, canonical_code string, grade string, quarantine_reason string>` — 같은 크로스워크 물질화 표를 읽는다.

**해석 알고리즘(결정적, 순서 고정):**

1. 형식 검증(자릿수·숫자). 실패 → `Quarantined(Malformed)`. 절대 고쳐 쓰지 않는다.
2. `(code_system, raw)` 정확 조회, `valid_during @> as_of` — 명중 → `Resolved(grade=Certain)`.
3. 코드는 있으나 `as_of` 에 유효하지 않음 → `superseded_by` 체인을 현행까지 추적.
   유일 종착 → `Resolved(Certain)` (ONS/Pelias "폐기 코드는 현행으로 리다이렉트").
   체인 분기(분할 1:N) → `Quarantined(AmbiguousSplit, candidates)`.
4. 미등록 → 파생 후보 생성(PNU 라면 시군구 크로스워크로 접두 재작성 후 **등록부에 그 PNU가
   실재하는지 확인**). 실재하는 유일 후보 → `Resolved(Probable)` + 감사 큐(§5).
   실재 확인 불가·복수 후보 → `Quarantined(UnregisteredCode | SourceConflict, candidates)`.
5. **금지 불변식(ADR-0023 "지번을 지어내지 않는다"):** 출력 `canonical_code`/PNU는 반드시
   크로스워크·등록부의 실재 행에서 복사한다. 어떤 경로도 미확인 코드를 합성해 반환하지 않는다.
   해석 불가 = `Quarantined` 이지, 그럴듯한 값이 아니다.

격리 착지 표(스튜어드 큐의 원천):

```sql
CREATE TABLE geo_identity.resolution_quarantine (
    quarantine_id   uuid PRIMARY KEY DEFAULT uuidv7(),
    source_dataset  text NOT NULL,       -- 파이프라인 그래프 노드 id (ADR-0086)
    source_record_id text NOT NULL,
    code_system     text NOT NULL,
    external_code   text NOT NULL,       -- 원본 그대로
    as_of           date NOT NULL,
    reason          text NOT NULL CHECK (reason IN
                    ('malformed','unregistered_code','ambiguous_split','source_conflict','not_valid_at_date')),
    candidates      jsonb NOT NULL DEFAULT '[]',   -- Candidate 배열 직렬화
    created_at      timestamptz NOT NULL DEFAULT now()
);
```

## §4 필지 분필·합필 계보 DAG (기둥 ①⑥, 문제 2)

모델 근거: **ISO 19152 LADM `VersionedObject`**(폐쇄 보존 + 버전 수명), **NZ LINZ**(Current→Historic,
"폐기 필지는 새 필지로 빈틈없이 전부 대체" = 계보 커버리지 100%), **Esri Parcel Fabric**(모든 법적
이벤트가 새 필지 생성 + 대체 필지는 historic, predecessor/successor 유지). 시드 원천은 추론이 아니라
받아쓰기: **MOLIT 필지고유번호변동연혁(data.go.kr 15122617)** + 우리 **토지이동이력**(1.21억 행,
ADR-0089/0090 서빙 중). 기하 겹침은 두 원천이 침묵할 때의 보완 증거일 뿐이다(Regrid 방식).

```sql
CREATE TABLE geo_identity.parcel_lineage (
    lineage_id       uuid PRIMARY KEY DEFAULT uuidv7(),
    predecessor_id   uuid NOT NULL REFERENCES geo_identity.place_registry (stable_id),
    successor_id     uuid NOT NULL REFERENCES geo_identity.place_registry (stable_id),
    event            text NOT NULL CHECK (event IN ('split','merge','recode')),
    event_date       date NOT NULL,             -- 법적 발효일 (변동연혁의 이동일자)
    source           text NOT NULL,             -- 'molit:15122617' | 'silver.land_movement_history'
                                                -- | 'derived:geometry-overlap'
    source_record_id text NOT NULL,             -- 원천 행 식별자 (지어내지 않는다, ADR-0086)
    asserted_at      timestamptz NOT NULL DEFAULT now(),
    retracted_at     timestamptz,               -- 정정은 철회+재삽입, append-only
    CONSTRAINT one_edge_per_event UNIQUE (predecessor_id, successor_id, event, event_date),
    -- 재코딩은 정체성 불변(M4): 자기 엣지로 코드 사건만 기록. 분필·합필은 반드시 새 ID.
    CONSTRAINT recode_is_identity_preserving CHECK (
        (event = 'recode') = (predecessor_id = successor_id))
);
```

**엣지 의미론:**

| event | 행 형태 | 등록부 부수효과 |
|---|---|---|
| `split` | 1 predecessor → N successors (N행) | predecessor `status='historic'`, successors 신규 민팅 (M5) |
| `merge` | N predecessors → 1 successor (N행) | predecessors 전부 historic, successor 신규 민팅 |
| `recode` | predecessor = successor (1행, 자기 엣지) | 등록부 불변; 새 코드는 §2 크로스워크 행으로 |

- **커버리지 불변식(LINZ):** `status='historic'` 인 필지는 자신을 predecessor 로 하는
  비철회 엣지가 ≥1 존재해야 한다. 위반 = 계보 구멍 = 가드 실패(§6 R4).
- 시점 조회(기둥 ⑥): `as_of` 에 유효한 크로스워크 행 + 그 시점까지의 엣지만 따라가면
  "그날의 필지 구성"이 나온다. 이력을 현행으로 덮지 않는다.

## §5 스튜어드 검수 큐 (기둥 ④) — EMPI match-grade 버킷

의료 EMPI(IHE PIX/PDQ·JeMPI)의 3버킷을 그대로: 자동 임계 위 = 자동 연결, 중간 = 사람 검토,
그 밖 = 연결 안 함. 우리 매핑과 처분:

| match_grade | 정의 | 처분 |
|---|---|---|
| `certain` | 크로스워크 정확 명중 또는 유일 승계 체인 (§3 2·3단계) | 자동 연결. 큐에 안 올림. provenance 기록 |
| `probable` | 파생 증거의 유일 후보가 등록부에 실재 (§3 4단계) | 자동 연결 **+ 비차단 감사 큐** 등재(EMPI auto-link 상단 임계). 스튜어드 기각 시 연결 철회+재격리 |
| `possible` | 복수 후보·분할 모호·소스 충돌 | **차단 격리** + 검수 큐. 사람 결정 전 적재 안 됨 |
| (없음) | 후보 0 (미등록·malformed) | 차단 격리. `match_grade IS NULL` 로 큐 등재 |

```sql
CREATE TABLE geo_identity.steward_queue (
    queue_id       uuid PRIMARY KEY DEFAULT uuidv7(),
    quarantine_id  uuid REFERENCES geo_identity.resolution_quarantine (quarantine_id), -- probable 감사건은 NULL 가능
    code_system    text NOT NULL,
    external_code  text NOT NULL,
    as_of          date NOT NULL,
    match_grade    text CHECK (match_grade IN ('certain','probable','possible')),  -- NULL = 후보 없음
    candidates     jsonb NOT NULL DEFAULT '[]',     -- 사전 채운 Candidate 배열 (원클릭 승인용)
    status         text NOT NULL DEFAULT 'open'
                   CHECK (status IN ('open','approved','rejected','superseded')),
    decided_by     text,                            -- 스튜어드 식별자
    decided_at     timestamptz,
    decision_note  text,                            -- 근거 (ADR-0103: 결정은 근거·작성자·일시 포함 데이터)
    created_at     timestamptz NOT NULL DEFAULT now(),
    CHECK ((status IN ('approved','rejected')) = (decided_by IS NOT NULL AND decided_at IS NOT NULL))
);
```

승인의 효과는 **코드 배포가 아니라 데이터 삽입**: `code_crosswalk` 에 `source='steward:<user>'`
행이 추가되고, 다음 적재부터 같은 코드가 §3 2단계에서 `Certain` 으로 자동 해석된다(ADR-0103
기둥 ④의 시험 그대로). "전남광주를 묶어 보일지" 같은 표시 그룹핑은 이 큐가 아니라 어드민
설정 데이터이며 본 설계 범위 밖.

## §6 연결성 가드 사양 (기둥 ⑤, 문제 3)

본보기: **dbt_project_evaluator** 의 `Root Models`(부모 0)·`Unused Sources`(자식 0) CI 실패 규칙,
서빙 소비자의 1급 노드화는 **dbt exposures**. 대상은 파이프라인 그래프 SSOT(ADR-0086)의 노드·엣지
— 목록을 가드에 복제하지 않고 그래프 파일 하나만 읽는다(거울 금지).

**규칙(각각 위반 노드 목록을 출력하고 전체 가드는 exit 1):**

| 규칙 | 대상 | PASS 조건 | FAIL 조건 |
|---|---|---|---|
| R1 생산자 존재 | `layer != bronze` 인 모든 노드 | 진입 엣지 ≥ 1 | 진입 엣지 0 (= Root Models: 생산자 없는 canonical/silver/gold) |
| R2 소비자 존재 | `kind != exposure` 인 모든 노드 | 진출 엣지 ≥ 1 | 진출 엣지 0 (= Unused Sources). 서빙·by-PNU 소비자는 exposure 노드로 등록해 충족 |
| R3 도달성 | 모든 gold·exposure 노드 | bronze 소스에서 도달 가능 | 도달 불가 (섬 그래프) |
| R4 계보 커버리지 | `place_registry` 의 historic 필지 | §4 predecessor 엣지 ≥ 1 | 엣지 0 (계보 구멍) |
| R5 코드 등록 | 배치의 모든 distinct 코드 | `as_of` 시점 크로스워크에서 해석 가능 | 미해석 코드 존재(격리로 갔더라도 임계 초과 시 FAIL) |

- **예외는 allowlist 파일 한 곳**: 항목마다 `node`, `rule`, `reason`, `expires`(날짜) 필수.
  만료된 예외는 그 자체로 FAIL. 침묵 예외·코드 내 하드코딩 예외 금지.
- **빈 입력은 FAIL**: 그래프 파일이 없거나 노드 0개, 또는 R5 대상 배치가 0행이면 PASS 가 아니라
  FAIL("아무 일도 안 일어남"을 성공으로 읽지 않는다).
- **가드 자기 증명(prove-the-check-can-fail):** CI 픽스처가 (a) 생산자 0 노드, (b) 소비자 0 노드,
  (c) 미등록 코드 1행을 각각 주입해 가드가 **exit 1 로 거부하는 것까지** 시험한다. 통과만 본
  가드는 신뢰하지 않는다(ADR-0001).
- 종료 규약: 위반 0 = exit 0 + 규칙별 검사 대상 수 출력(0건 검사도 노출). 위반 ≥1 = exit 1 +
  `(node, rule, detail)` 목록. 그래프 판독 불능 = exit 2 (인프라 실패를 위반과 구분).

## ADR-0103 6기둥 정렬표

| 기둥 | 본 설계 |
|---|---|
| ① 안정 정체성은 코드에서 파생 금지 | §1 민팅 UUIDv4 + M1–M6, §2 `place_registry` |
| ② 시간 사전이 정본 | §2 `code_crosswalk` + `code_register_current` (RGC/CHD 2분할) |
| ③ 관문 해석기 | §3 `resolve()` — 유일한 코드 처리 지점, PNU 조립 전 호출 |
| ④ 스튜어드 큐 | §5 EMPI 3버킷 + `steward_queue`, 결정은 데이터로 저장 |
| ⑤ 표류 감지 가드 | §6 R1–R5 + 위반 주입 자기 증명 |
| ⑥ 양시간 보존 | §2 valid/asserted 2축, §3 `as_of` 해석, §4 시점 조회, 원본 코드 무손실 |

수용 기준 자기 점검: 모든 표에 컬럼+타입 명시(§2 ×2, §3 격리, §4 계보, §5 큐) · 해석기 정확한
시그니처(§3 Rust + Spark 표 함수) · 가드 명시적 PASS/FAIL 규칙 + 종료 규약(§6). 구현(코드·마이그
레이션·잡)은 본 문서 범위 밖이며 Wave 계획이 나른다.
