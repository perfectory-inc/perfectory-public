# ADR 0607: 호스트가 지은 RAON 이미지는 입력이 바뀔 때까지 남고, RAON 한 실행은 13 파일을 받는다

- Status: Accepted
- Date: 2026-11-23
- Amends: [ADR-0198](./0198-server-scratch-journal-and-unused-images-age-out-by-themselves.md) §3(지우는 이미지의 조건),
  [ADR-0514](./0514-the-raon-lane-takes-a-capped-count-of-the-newest-files-every-day-and-a-listed-file-not-in-bronze-pages.md)(한
  실행 8 파일)
- Related: [ADR-0601](./0601-the-verification-image-is-the-one-test-image-and-reaches-the-data-host-in-the-signed-release.md)(입력의
  다이제스트로 이름 붙인 이미지), [ADR-0606](./0606-the-daily-vworld-sweep-takes-at-most-a-cap-of-new-bytes-a-run-and-the-rest-the-next-day.md)

## Context

`scripts/ops/raon-large-files.sh` 는 RAON 배치 이미지를 호스트에서 짓는다. 이미지 이름은 빌드 입력(작업자, Dockerfile, 이름 계약,
고정한 패키지)의 해시이고, 같은 이름의 이미지가 있으면 다시 짓지 않는다. 그런데 2026-11-22 의 RAON 실행(run
20261122T145314Z)은 첫 파일 전에 85 분을 이미지 짓기에 썼다. 입력은 바뀌지 않았다. 이름이 11-19 와 같은 `9ddf8e85f8125bfb` 이다.

원인은 host retention(ADR-0198 §3)이다. 11-21 05:09Z 에 그 이미지를 지웠다(저널 `remove
foundation-platform/raon-batch-9ddf8e85f8125bfb:local`). 조건 넷이 모두 맞았다.

- RAON 은 `docker run --rm` 이라 실행 사이에는 쓰는 컨테이너가 없다.
- 만든 지 7 일이 지났다.
- 릴리스 소유가 아니다.
- 이름이 실행 때 계산되므로 체크아웃의 어느 파일도 그 태그를 적지 않는다.

그래서 이 이미지는 지어질 때마다 일주일 남짓 뒤에 지워지고, 그다음 RAON 실행이 같은 것을 다시 짓는다.

잰 값(11-22): RAON 파일 하나가 3–34 분, 평균 약 15 분 걸렸다(7 파일, 0.65–1.1 GB). 85 분은 파일 다섯 개쯤이다. RAON 의 밀린
파일은 657 개이고, 한 실행 8 개(ADR-0514)로는 82 일이 걸린다. 데이터 디스크는 막지 않는다. RAON 은 파일을 하나씩 스테이징하고 R2 에
커밋하며, 다음 실행이 스테이징을 비운다.

## Decision

1. **RAON 이미지는 저장소 하나다.** `foundation-platform/raon-batch:<입력 다이제스트 16 자>` 로 태그한다. 입력이 같으면 같은
   태그를 다시 쓴다(지금처럼).
2. **host retention 은 호스트가 짓는 저장소의 가장 새 이미지를 나이와 관계없이 남긴다.** 그 저장소는
   `host_retention.py` 의 `HOST_BUILT` 에 적는다(지금은 `foundation-platform/raon-batch`). 그보다 오래된 이미지는 지난 입력의
   것이라 다른 이미지처럼 7 일 뒤 나간다. 다른 네 조건은 그대로다.
3. **RAON 한 실행은 13 파일을 받는다.** `selection_archive_max_files_per_run` 8 → 13, 예산은 그 수 × 2 GiB
   (`selection_archive_new_bytes_budget` 26 GiB, ADR-0514 의 규칙 그대로)다. 늘어난 다섯 파일이 다시 짓기에 쓰던 85 분이다.
   평균으로 약 3.3 시간이다. VWorld 레인의 50 GiB(ADR-0606, 1.3–1.9 시간)를 더해도 매일 훑기의 480 분 안이다. 밀린 657 파일은
   약 51 일에 다 받는다.

## Consequences

- 입력이 바뀐 배포 뒤 첫 RAON 실행만 이미지를 짓는다. 그 실행은 약 85 분 더 길다. 이 결정을 싣는 배포 뒤 첫 실행도 이름이 바뀌어
  한 번 짓는다.
- 가장 새 RAON 이미지 하나(약 4.6 GB)는 늘 데이터 디스크에 남는다.
- 시험
  - `test_host_retention.py`: 가장 새 RAON 이미지는 30 일이 지나도 남고, 그보다 하루 오래된 것은 나간다.
  - `test_raon_large_files_command.py`: 이미지 태그가 저장소 하나·입력 다이제스트 꼴이고, 그 저장소가 `HOST_BUILT` 에 있다.
