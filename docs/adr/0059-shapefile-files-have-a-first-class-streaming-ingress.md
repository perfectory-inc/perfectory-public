---
status: current
owner: foundation-platform
doc_type: adr
last_reviewed: 2026-08-26
---

# ADR 0059: Shapefile 파일은 API 봉투가 아닌 스트리밍 입구를 가진다

- Status: Accepted
- Date: 2026-08-26
- Amends: ADR-0042 §Decision 2의 `silver.parcel_boundaries` 원천 전제
- Related: ADR-0046, ADR-0054, FP-ADR-0022

## 배경

`silver.parcel_boundaries`의 기존 생산자는 VWorld API 응답의
`/response/result/featureCollection/features`를 읽는다. 그러나 파일 내려받기 경로의 Bronze 객체는
그 봉투가 아니라 `.shp/.shx/.dbf/.prj/.cpg`를 담은 ZIP이다. 파일을 API 응답처럼 꾸미면 실제
수집 계보가 사라지고, 기존 생산자처럼 응답 전체를 `Vec`에 모으면 약 3천만 필지에서 메모리 상한이
입력 전체 크기가 된다.

ADR-0042는 당시 필지 원천을 EPSG:4326 VWorld GeoJSON 하나로 보아 필지 표에는 재투영이 필요 없다고
기록했다. 파일 원천은 `Korea_2000_Korea_Central_Belt_2010`의 미터 좌표를 싣는다. 표 계약은 여전히
`geometry_srid = 4326`이고 같은 표의 두 수집 경로가 다른 SRID를 내보낼 수 없으므로, 파일 입구에는
검증된 재투영이 필요하다.

현행 저장소에는 산업단지 경계 한 파일을 위해 `.shp` 바이트를 직접 해석하는
`shapefile_polygon_reader.rs`가 이미 있다. 이 판독기는 원천에서 실측한 붕괴 링을 세고 버리는 특수
규칙을 소유하며 모든 polygon record를 `Vec`으로 돌려준다. DBF·CPG를 읽지 않고 ZIP dataset 계약도
표현하지 않으므로, 그 코드를 필지 3천만 행의 일반 입구로 넓히면 특수 규칙과 공통 파일 형식이 다시
한 소유자에 섞인다.

## 근본 원인과 불변식

근본 원인은 수집 경로가 다른 원천을 같은 API 봉투 해석기로 합치려 한 것이고, seek가 필요한 파일
형식과 행 규모에 맞는 공통 어댑터가 없었던 것이다. 다음 불변식을 둔다.

1. API 응답과 파일 ZIP은 서로 다른 입구이며 실제 source kind를 그대로 이름 붙인다.
2. PNU 분해·WKB·bbox·checksum·Silver transport 행은 기존 lakehouse 정규화 한 곳에서만 만든다.
3. 피처와 JSONL 행은 한 번에 하나만 살아 있고, 전체 feature/row 집합을 모으지 않는다.
4. CPG와 PRJ가 선언한 값을 읽는다. 없거나 모르면 추측하지 않고 실패한다.
5. 좌표 변환은 독립 구현인 pyproj와 실물 범위에서 대조한 오차 한계 안에 있어야 한다.
6. ZIP member는 디스크에 풀지 않는다.

## 결정

1. `foundation-shapefile` crate가 ZIP dataset의 `.shp/.shx/.dbf/.prj/.cpg` 결합과 좌표 변환을
   소유한다. `shapefile 0.9`의 iterator와 `dbase` 판독기를 재사용하고, DBF decoder에는 CPG label로
   찾은 `encoding_rs` encoding을 명시적으로 넣는다. `shapefile`은 MIT이고 0.8에서 CPG 감지와
   `encoding_rs` 지원을 추가한 뒤 0.9까지 이어졌으며, 직접 ESRI/DBF 바이너리 parser를 하나 더
   유지하는 것보다 교체 가능한 표준 경계다.
2. ZIP 압축 stream은 seek할 수 없으므로 `.shp/.shx/.dbf` 세 member만 각각 메모리 buffer로 만들고
   `shapefile::Reader::iter_shapes_and_records`로 한 행씩 읽는다. 압축 ZIP 전체, feature 전체, 출력
   JSONL 전체는 메모리에 올리지 않는다. 메모리 상한은 한 ZIP의 세 seekable member 합과 가장 큰
   feature/row 하나다.
3. `.prj`는 `proj4wkt 0.1.1`로 PROJ 정의로 바꾸고 `proj4rs 0.1.10`으로 EPSG:4326에 역투영한다.
   둘은 같은 3Liz 계열의 순수 Rust companion이고 C PROJ 설치가 필요 없다. 허용 입력은 GRS80,
   Transverse Mercator, 위도 원점 38°, false easting/northing 200000/600000, scale 1인 Korea 2000
   2010 belt이며 중앙 자오선 125/127/129/131°만 받는다. WKT parser가 읽을 수 있어도 이 signature와
   다르면 실패한다.
4. pyproj 3.7.2를 oracle로 `30563-100.zip`의 SHP bbox에서 seed 5186으로 뽑은 무작위 10,000점을
   비교했다. 최대 차이는 `9.112781640396861e-10`도, 지표면 근사거리
   `0.00010132964856545947 m`였다. 허용 한계는 `0.001 m`다. 실측 최대의 약 9.9배이면서 1 mm라
   필지 지도 위치에 의미 있는 이동을 허용하지 않는다. `verify-vworld-shapefile-projection.py`가
   1,000점 미만 실행과 이 한계 초과를 종료코드로 거부한다.
5. outbox-publisher의 `export-vworld-cadastral-shapefile-silver-handoff`가 파일 source를 plain
   GeoJSON feature 형태로 한 행씩 어댑트한다. API response envelope는 만들지 않는다. 각 유효 PNU는
   기존 `normalize_vworld_cadastral_silver_parcel_boundary_rows`와 기존 handoff builder에 한 행씩
   들어간다. 19자리 ASCII와 표준 대장구분을 통과하지 못한 PNU는 출력에서 격리하고
   `invalid_pnu_count`로 센다.
6. 출력은 새 파일만 허용하고 같은 디렉터리의 고유 partial file에 stream한 뒤 flush·sync 후 rename한다.
   실패한 partial은 drop 시 제거한다. 이 명령은 로컬 ZIP을 읽고 로컬 JSONL/summary만 쓰며 R2나
   Iceberg 표를 쓰지 않는다.
7. 산업단지 bespoke reader는 이번 변경에서 교체하지 않는다. 그 원천에서 실측한 붕괴 링 보정과
   1,343행 결과 parity를 새 crate에서 먼저 증명하지 않고 바꾸면 ADR-0047의 경계 사실을 훼손한다.
   새 crate는 ZIP/DBF/CPG/PRJ가 필요한 파일 원천의 공통 입구이고, 기존 reader의 수렴은 그 parity
   증거를 가진 별도 변경이다.

## 기각한 대안

- **파일 feature를 VWorld API 응답 봉투로 감싸기:** 수집하지 않은 API response lineage를 만든다.
- **기존 API exporter에 shapefile 분기를 계속 추가:** envelope 해석, 파일 형식, 전체 집합 dedupe가
  한 함수에 섞이고 feature 전체를 모으는 현재 메모리 구조를 보존한다.
- **산업단지용 수동 `.shp` parser를 그대로 확장:** DBF·CPG·PRJ·ZIP 계약이 없고 전체 record를
  모으며, 한 원천의 붕괴 링 규칙을 모든 shapefile의 일반 규칙으로 오인한다.
- **GDAL 또는 `proj` crate:** 성숙하지만 C PROJ와 native build/runtime data 의존성을 추가해 이
  작업의 Windows·순수 Rust 제약을 충족하지 않는다.
- **Transverse Mercator 공식을 직접 구현:** 검증된 데이터 플레인을 다시 만들고 미세한 오차가
  유효 범위 좌표로 남아 기존 품질 gate를 모두 통과할 수 있다.
- **ZIP 전체를 풀거나 feature를 정렬·dedupe하려고 모두 모으기:** 272개·약 3천만 행에서 저장공간
  또는 메모리 상한이 데이터 전체 크기가 된다.
- **CPG/PRJ 누락 시 기본값 사용:** 틀린 한글이나 좌표가 정상 JSONL로 조용히 승격된다.

## 결과

파일 수집 계보와 API 계보가 분리된 채 같은 Silver 행 정규화 SSOT로 수렴한다. 피처 수에 비례해
메모리가 늘지 않고, 새 좌표계·인코딩은 우연히 통과하지 못한다. 현재 명령은 한 로컬 ZIP path를
받지만 core reader는 `Read + Seek`를 받으므로, 65 GB 원천에서는 object별 R2 range-backed reader나
한 object만 bounded spool하는 상위 adapter를 붙일 수 있다. 전체 원천을 한 번에 로컬로 내려받는
계약은 만들지 않았다.

## 참고

- [shapefile crate reading and encoding features](https://docs.rs/shapefile/latest/shapefile/)
- [proj4rs pure Rust usage and radian contract](https://github.com/3liz/proj4rs)
- [proj4wkt WKT1/WKT2 companion](https://docs.rs/proj4wkt/)
- [PROJ Transverse Mercator accuracy](https://proj.org/en/stable/operations/projections/tmerc.html)
