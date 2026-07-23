# 캐노니컬 부동산 데이터 플랫폼 — 북극성 (North Star)

Status: reference / index. 이 문서는 결정이 아니라 **전체 지도이자 색인**이다.
개별 결정은 아래 링크된 ADR·스펙이 소유한다. 충돌 시 개별 문서가 이긴다.

Owner: foundation-platform (데이터 파운데이션) · 소비: gongzzang / dawneer

## 0. 이 문서의 목적

관련 계약과 규칙이 여러 문서에 나뉘어 있다. 이 문서는 그 전체를 하나의 플로우로
묶고, 각 조각의 정본이 어느 문서에 있는지 색인한다.

## 1. 목표 (한 줄)

정부의 흩어진 대장 수십 종을 → **같은 건물로 묶고 → 깨끗이 정규화**하여 →
하나의 신뢰할 수 있는 **건물·필지 엔티티**로 만들고 → 유저·직원에게 완전하고
정확하게 보여준다. "정확하게 연결·검증된 데이터"가 경쟁 해자(moat)다.

## 2. 전체 플로우 (★정정된 순서: 묶고 → 정규화)

```
[0] 정부 원천                                              저장
    hub.go.kr(건축물대장) · V-World(토지) · juso(건물도형 후보)
         │
         ▼
[1] 수집 ─────────────────────────────────────────────  R2(원본,불변)
    벌크/CSV/SHP 통째 수신 → 원본 보관 + 지문(SHA-256)         + DB(bronze_object)
         │
         ▼
[2] 의미 사전 (필드 → 개념) ──────────────────────────────  계약·규칙
    대장마다 칸 위치 다름(PNU가 8/10/9번...) → 개념에 매핑
         │
         ▼
[3] 깨끗한 열쇠로 먼저 묶기 (coarse link) ─────────────────  ★묶기가 먼저
    · PNU(19자리) = 전 대장 앵커, 정규화 불필요
    · 표제부 ↔ 층별개요 = 공유 PK (동 레벨)
    · 여러 필지=한 건물 = 부속지번(정부 제공)
    → 건물(동)로 그룹핑. 값 정규화 없이 가능(열쇠가 깨끗해서)
         │
         ▼
[4] 그 맥락 속에서 값 정규화 (깔때기 3단) ─────────────────  lakehouse silver
    ┌─ 1단 행 규칙(순수함수)     → 결정적 변환
    │    지1층→지하1층, 내지하2층→지하2층 (A1/A2/A5 포함)
    ├─ 2단 동 판정(증인 다수결)  → 증거 기반 모순 해소
    │    num vs 라벨 충돌 → num집합·라벨집합·표제부층수 3명 다수결
    │    (표제부 층수 증인은 [3]에서 묶었기에 확보됨) · 안 맞으면 기권
    └─ 3단 AI 제안 → 사람 승인    → 결정 규칙의 잔여 사례만
         │
         ▼
[5] 지저분한 나머지 연결 (fine link) ──────────────────────  연결 규칙
    · 전유부·전유공용면적·대지권·가격 = PNU+동+호
    · 동/호 이름 흐릿(에이동/A동)만 정규화된 이름 or Splink
    · 토지(V-World) = PNU 조인 (깨끗)
         │
         ▼
[6] 캐노니컬 엔티티 (Silver) ─────────────────────────────  lakehouse
    Parcel · Building · Floor · Unit · Usage · Structure
         │
         ▼
[7] 일관성 검사 ──────────────────────────────────────────  검증 규칙
    SQL 규칙 전수 탐지 → 증거 판정 → 확정값+신뢰도+근거
    (4단의 2단 동 판정이 일관성 규칙 BR-C01/C02의 씨앗)
         │
         ▼
[8] 서빙 / UI (Gold) ─────────────────────────────────────  공짱·Dawneer
    · 공짱: 유저에게 완전한 건물 정보(매물)
    · UI 영역 = 대장 계층 그대로: 총괄표제부/표제부/전유부(+대지권·가격)
    · Dawneer: 내부 직원 승인 콘솔([4]-3단 승인 여기)
```

## 3. ★핵심: 왜 "묶고 → 정규화" 순서인가 (MDM 표준)

이것은 **MDM 표준 2단계 = match(묶기) → merge(값 정리)** 다.

- **묶기가 먼저인 이유**: 값 모순(num=1 vs 라벨 2층)을 풀려면 "같은 건물의
  다른 증거"(표제부 층수)가 필요 → 먼저 묶어야 그 증인이 생긴다.
- **완전 선형은 아닌 이유**: 지저분한 이름 연결(전유부 동)은 정규화된 이름이
  필요 → 그 연결만 정규화 뒤.
- **그래서 정확한 구조**: 깨끗한 열쇠(PNU/PK)로 먼저 묶고 → 맥락에서 정규화 →
  지저분한 나머지 연결. (선형 아닌 의존성 그래프)
- 우리 엔티티 설계가 이미 이 순서다: 컨텍스트 팩이 건물로 먼저 묶은 뒤 정규화.

## 4. 관통 원칙 (전 단계)

1. **원본 불변** — R2 원본 안 고침. 나머지는 재생성 가능
2. **결정적 우선, AI는 잔여물만** — 규칙·다수결로 판정하고 모델은 진짜 애매한 것만
3. **못 가르면 기권** — 추측 금지, 사람이 봄
4. **추적 가능** — 모든 값이 "어느 대장 몇째 줄 → 어떤 판정 → 확정값"
5. **canonical은 사람 승인만** — AI 직접 쓰기 차단
6. **인프라 무타협 + 제품 우선** — 안 만들 건 안 만들되, 만드는 건 대기업급

## 5. 저장소 지도

| 위치 | 담는 것 | 성격 |
|---|---|---|
| **R2** | 원본 zip/CSV/SHP (건물+토지) | 영구·불변·SSOT |
| **PostgreSQL** | bronze_object 장부·승인이력·(예정)사전·판정 | 영구·장부 |
| **lakehouse** | silver 캐노니컬 엔티티(Parquet) | 영구·분석/서빙 |
| **로컬 target/** | 작업 사본·evidence | 임시·재생성 |
| GPU 서버 | Qwen 모델만 | 데이터 저장 안 함 |

## 6. 설계 참고 패턴

- 전수 검사 = 규칙: Amazon Deequ · Google TFX · LinkedIn Data Sentinel
- 값 충돌 판정 = 데이터 퓨전 VOTE(Google) · MDM survivorship(Informatica)
- 묶기→정리 = MDM 2단계 표준
- 의미 사전 = DataHub 패턴 / Palantir 온톨로지
- 지저분한 이름 연결 = Splink(영국 MoJ, 정부 데이터)
- AI = 잔여물만, 고정밀 임계값 자동조치(Amazon AutoKnow), 확률 자동수리 배제(Google)
- 현행 구현 근거: [건축물대장 일관성 규칙](./catalog/building-register-consistency-rules.v1.draft.md)과
  [층 정규화 규칙](./catalog/building-register-floor-normalization-rules.v1.md)

## 7. 관련 문서 색인

**현행 계약/규칙 (foundation-platform)**
- 정규화 소유권: `docs/adr/0027-normalization-capability-ownership.md`
- 전국 정규화 계약: `docs/catalog/national-data-normalization-contract.v1.json`
- 층 정규화 규칙과 모순 해소: `docs/catalog/building-register-floor-normalization-rules.v1.md`
- 대장 필드 매핑: `docs/catalog/building-register-field-mapping.v1.draft.md`
- 일관성 규칙 초안: `docs/catalog/building-register-consistency-rules.v1.draft.md`

**결정 (ADR, gongzzang)**
- ADR-0018 PNU-first · ADR-0048 수평 플랫폼 · ADR-0049 identity-platform · ADR-0050 Dawneer 콘솔+권한

**역사 조사 경계**
- 대기업 DQ 아키텍처, 층 정규화, 파이프라인 census와 모순해소의 dated 조사 증거는
  [루트 ADR-0007](../../../docs/adr/0007-public-code-private-operations-boundary.md)에 따라
  비공개 전환 archive에서 보존한다. 위 현행 계약과 코드가 공개 정본이다.

## 8. 열린 결정 (소유자)
- G3 정책: 다락·중층·복수층 스팬 표준 어휘
- juso.building(건물 폴리곤) 수집 여부 (UI 지도용)
- Splink 채택 시점 (전유부 동 fuzzy = Building 조립 단계)
- 관계형 vs 온톨로지 그래프 (캐노니컬 엔티티 층 — Building 조립 시점 카드)
