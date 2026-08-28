# ADR 0064: 필지 표는 벡터화 읽기 없이 읽는다

- Status: Accepted
- Date: 2026-08-28

## Context

`silver.parcel_boundaries` 를 적재하던 중, 잡이 세 번 연속 같은 자리에서 죽었다. 종료 코드만
보면 우리 코드의 실패로 읽히지만 실물은 달랐다.

```
free(): invalid pointer
SIGSEGV — Problematic frame: C [libc.so.6+0x28898] abort+0x178
Java frames: ... org.apache.iceberg.spark.source.BaseReader.next(BaseReader.java:155)
```

`abort()` 는 glibc 가 자기 힙이 망가진 것을 감지했을 때 부른다. 즉 **네이티브 코드가 메모리를
잘못 해제**했고, 그 자리는 쓰기가 아니라 **읽기**(`BaseReader`)였다.

`--rm` 이 컨테이너를 지우면서 JVM 이 남긴 크래시 보고서까지 함께 사라지고 있었다. 볼륨을
붙여 밖으로 꺼낸 뒤에야 위 내용을 볼 수 있었다.

메모리 부족이 아니었다. 보고서에 적힌 그 순간의 상태:

```
Heap: total 14,630,912K, used 10,575,249K   (상한 24 GB — 여유 있었다)
Metaspace: used 173,648K                     (상한 없음)
memory_limit_in_bytes: unlimited             (컨테이너 한도 없음)
overcommit_memory = 0                        (커널이 느슨)
```

**서버 문제도 아니었다.** 같은 표를 무관한 다른 호스트에서 읽어도 같은 자리에서 죽었다.

```
count()                                → 3,958,994  성공
select(source_record_id).distinct()    → SIGSEGV
```

`count()` 는 매니페스트의 행 수로 답하므로 파일을 열지 않는다. 파일을 여는 순간 죽는다.

**파일 자체는 멀쩡하다.** 벡터화 읽기를 끄면 같은 파일이 그대로 읽힌다. 두 호스트에서 확인했다.

```
read.parquet.vectorization.enabled = false  → 객체 16개, 최대 도형 60,694 바이트, 정상
```

Iceberg 의 벡터화 Parquet 읽기는 힙 밖 버퍼를 쓴다. 이 표는 가변 길이 이진 열(`geometry_wkb`)을
싣고 있고, 파일 하나가 평균 29.6 MB 로 커진 것은 root ADR-0063 이후다. 그 전 판(파일 평균
0.28 MB)에서는 나타나지 않았다.

## Decision

1. `silver.parcel_boundaries` 는 표 속성 `read.parquet.vectorization.enabled = false` 를 갖는다.

2. 이 설정은 **제출 인자가 아니라 표 속성**으로 둔다. 인자로 주면 그 인자를 아는 실행만
   안전하고, 같은 표를 여는 다른 엔진은 플래그를 몰라서 죽는다. 속성이면 아무것도 모르는
   읽는 쪽도 보호된다.

3. 속성은 조건부 생성이 아니라 `ALTER TABLE ... SET TBLPROPERTIES` 로 매 실행 적용한다.
   조건부 생성은 이미 있는 표를 지나치므로, 앞선 적재가 남긴 행은 영원히 보호받지 못한다.

4. 표를 준비하는 일은 **어떤 읽기보다도 먼저** 온다. 이 잡은 이미 들어간 묶음을 건너뛸 때도
   행을 세는데 그것도 파일을 여는 일이며, 실제로 그 자리에서 세 번 죽었다.

## Consequences

벡터화 읽기는 성능 기능이고 끄면 읽기가 느려진다. 그러나 JVM 을 죽이는 읽기 경로는 느린
경로와 비교할 대상이 아니다.

**우리는 Iceberg 의 버그를 고친 것이 아니라 피한 것이다.** 왜 이 데이터에서 힙 밖 버퍼가
망가지는지는 규명하지 않았다. 재현 조건(가변 길이 이진 열 + 29.6 MB 파일 + zstd)은 위에
적어 두었으므로, 상류에 보고하거나 Iceberg 판을 올릴 때 다시 확인할 수 있다.

**같은 형태를 가진 다른 표를 아직 확인하지 않았다.**
`silver.industrial_complex_boundaries` 도 `geometry_wkb` 를 싣지만 1,343행뿐이라 파일이 작고,
지금까지 읽기에서 죽은 적이 없다. 그 표의 파일이 커지면 같은 일이 생길 수 있다.

---

*2026-08-28 추가 측정.* 위 문단이 남긴 질문을 재어 보았다. 벡터화를 켠 채로 형제 표 둘을
읽었고 둘 다 정상이었다.

```
silver.industrial_complex_boundaries   최대 geometry_wkb  88,285 바이트   정상
silver.industrial_complexes            (도형 열 없음)                     정상
silver.parcel_boundaries               최대 geometry_wkb  60,694 바이트   죽음
```

**개별 값의 크기는 원인이 아니다.** 죽지 않는 표가 더 큰 값을 싣고 있다. 남는 차이는 파일
하나에 담긴 양(29.6 MB 대 1 MB 미만)이며, 이는 위 Context 가 지목한 조건과 같은 방향이다.
결정은 바뀌지 않는다. 다른 표의 파일이 커질 때 다시 재야 할 값이 무엇인지가 좁혀졌을 뿐이다.

**종료 코드가 원인을 가린다.** `docker compose run` 이 돌려준 127 은 "명령을 못 찾음"으로
읽히지만 실제로는 JVM 이 죽은 것이었다. 적재기가 그 코드를 그대로 보고했고, 나는 처음 두 번을
일시적 결함으로 읽었다. 크래시 보고서를 컨테이너 밖으로 꺼내는 것이 판단의 시작이었다.
