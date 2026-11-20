# ADR 0593: 메모리 상한이 끊은 문서는 다시 읽고 run 을 실패시키며, Tesseract 에는 크기를 묶은 그림만 준다

- Status: Accepted
- Date: 2026-11-19
- Amends: [ADR-0477](./0477-foundation-hands-intelligence-documents-and-place-ids-and-takes-back-cited-facts-in-tables-only-intelligence-writes.md)
  (고시 첨부 쪽 텍스트: "못 읽은 문서는 결과가 있으므로 다시 고르지 않는다"를 메모리 종료에 한해 바꾼다)
- Builds on: [ADR-0455](./0455-one-job-cannot-take-the-host-every-unit-declares-its-memory-and-batch-is-killed-first.md)
  §3(상한은 측정의 1.5 배), ADR-0352(할 일은 표에서 계산), ADR-0267(현재 행은 계약 하나로)

## Context

운영 커널 기록(ai-server, 읽기만, 2026-11-12..19): `ocrmypdf` 가 cgroup 메모리 상한에 215 번, 그 안의 `tesseract` 가 32 번
죽었다. 모두 `foundation-intelligence-document-text.service` 가 띄우는 `intelligence-document-text` 컨테이너(상한 2g)다.

- **원인.** 쪽 텍스트 작업은 글자가 모자란 쪽을 `ocrmypdf --force-ocr --jobs 4` 로 읽는다. OCRmyPDF 문서(성능)는 최고 메모리가
  "가장 큰 쪽의 화소당 약 15 바이트, 동시 작업마다"이고 `--jobs` 가 그것을 곱한다고 한다. 2g 는 2026-11-11 표본 97 개의 가장 큰
  도구 프로세스 777 MiB 로 정했지만, 남은 문서일수록 큰 스캔이 많다: run 의 `largest_child` 최고가 11-15·16·17 에 2036–2041 MiB,
  곧 상한이다. 운영과 같은 trixie 패키지(ocrmypdf 16.7.0, Tesseract 5.5.0, Ghostscript 10.05.1)로 노트북에서 죽은 문서를 다시
  돌리면 2g 에서 같은 문서가 같은 방식으로 죽는다(측정은 Consequences).
- **아무도 몰랐다.** 11-12 부터 문서마다 자기 작업자 프로세스에서 읽어 한 문서가 run 을 죽이지 않는다. 그래서 죽은 OCR 은 그
  문서의 `failed` 결과가 되고 run 은 `ok` 로 끝난다. 유닛이 실패하지 않으니 OnFailure 가 부르는 이도 없었다.
- **잃은 문서가 돌아오지 않는다.** plan-text 는 이 추출기 판의 결과가 *하나라도* 있는 해시를 끝났다고 본다. `failed` 도
  결과라서 메모리에 끊긴 문서는 다시 읽히지 않는다. 운영 결과 표(11-19): `failed` 251 개 중 메모리 종료가 185 개
  (`ocrmypdf exit -9` 182, `worker killed by signal 9` 3)이고 나머지는 변환 시간 초과·LibreOffice 오류 등 문서 자신의 실패다.
  `ocrmypdf exit 7` 26 개는 Tesseract 종료와 짝이 맞지만 증명할 표지가 없어 그대로 둔다. 마지막 run 은 PDF 45 개 중 18 개를 잃었다.

## Decision

1. **메모리 종료에는 표지가 붙는다.** run 은 문서를 한 번에 하나씩 읽고, 하나를 읽기 앞뒤로 컨테이너 자신의 cgroup
   `memory.events` 의 `oom_kill` 을 읽는다(ai-server: cgroup v2, 컨테이너마다 사적 cgroup 이름공간). 그 사이 늘었고 결과가
   `failed` 면 오류가 `memory cap: ` 으로 시작하고, 요약에 `memory_killed` 로 센다. 추측이 아니라 커널이 센 수다. 그래서
   ocrmypdf 아래 Tesseract 가 죽은 경우(`ocrmypdf exit 7`)도 잡는다. 그 수를 읽을 수 없으면 요약의 `memory_kill_counter` 가
   `false` 이고 run 은 실패한다(표지 없이 조용해지지 않는다). 문서를 동시에 읽게 되면 이 표지는 다시 정해야 한다.
2. **메모리 종료는 다시 읽는다, 정해진 횟수까지.** 계약 `document_text.memory_kill` 이 표지(`error_prefixes`: 새 표지와 표지 전의
   두 오류 모양)와 `attempts`(3)를 둔다. plan-text 는 해시의 *현재* 결과(계약의 `current_row_latest`, 새 run 이 이긴다)가
   메모리 종료이고 결과 행이 `attempts` 보다 적으면 다시 빚진 것으로 센다. 표지 전의 행도 한 번의 시도로 센다(옛 185 개는 두 번
   더 읽힌다). 메모리 종료는 문서가 아니라 상한에 대한 사실이라 다시 읽고, 횟수는 어떤 상한에도 안 맞는 문서가 run 마다 읽히지
   않게 한다. 횟수를 다 쓴 문서는 plan-text 요약과 작업 기록의 `memory_kill_exhausted` 로 매번 센다. 그것을 다시 읽는 길은 상한이나
   묶음을 고치며 `attempts` 를 올리는 것이다. 결과 표는 덧붙이기만 한다.
3. **사실은 가장 새 텍스트를 따른다.** plan-facts 는 해시의 현재 텍스트 결과의 run 이 현재 사실 결과의 run 보다 나중이면 다시
   빚진 것으로 센다(두 작업의 run id 는 같은 UTC 모양 `YYYYMMDDTHHMMSSZ`). `failed` 결과에는 쪽이 없다(run 이 확인한다): 다시
   읽힌 문서의 사실은 새 쪽에서만 나오고 옛 사실 행이 현재로 남지 않는다. 사람 손 없이 사실까지 다시 뽑힌다.
4. **메모리 종료나 읽지 못한 수가 있으면 유닛이 실패한다.** 이미지가 모든 행을 쓴 뒤 `exit 1`, 스크립트는 그것을 덧붙이고 사실
   작업에 바뀌었다고 알린 뒤 실패한다: OnFailure 가 주인(`X-Foundation-Owner`)을 부른다. 2 의 횟수가 있어 같은 문서는 많아야
   `attempts` 번 실패시킨다.
5. **Tesseract 에는 묶은 그림만 준다.** `ocrmypdf --tesseract-downsample-large-images --tesseract-downsample-above <px>` 로 OCR 에
   넘기는 그림만 긴 변 `ocr_downsample_above_px` 로 줄인다. 16.7.0 태그의 `builtin_plugins/tesseract_ocr.py`
   `filter_ocr_image` 가 줄인 그림은 `ocr.png` 로만 저장되고, 남는 쪽은 그와 별개로 만든다. 300 dpi A4(긴 변 3508 px)는 손대지
   않고, 300 dpi A3(4961 px)와 그보다 큰 도면 쪽은 줄어든다. 값(4000)과 컨테이너 상한(2g → 2564m)은 아래 측정으로 정했다.

## Consequences

- 운영의 메모리 종료 185 개는 이 PR 이 배포된 뒤의 첫 run 부터 다시 빚진다(`owed` 가 그만큼 는다). 사람이 할 일은 없다.
- `intelligence.document_text_outcome` 과 `intelligence.document_fact_outcome` 의 품질 관문 문장 "(sha256, extractor_version)
  unique" 는 원래 현재 행 하나를 뜻했다. 다시 읽기가 생겨 "현재 행 하나, 다시 읽으면 새 run 의 행이 덧붙는다"로 고친다. 그것을 세는
  검사는 없었다.
- 측정(노트북, 운영과 같은 패키지, `--cpus 4`, 컨테이너 anon 최고):

  | 문서(운영에서 2g 에 죽은 PDF) | 묶지 않음 (5g 상한) | `--tesseract-downsample-above 4000` (2g 상한) |
  | --- | --- | --- |
  | 31 쪽 | 2492 MiB, 247 s, 26,803 자 | 1407 MiB, 186 s, 25,871 자 |
  | 6 쪽 | 2299 MiB, 58 s (2g 에서 24 s 만에 죽음) | 1198 MiB, 45 s |
  | 6 쪽 | 2304 MiB, 53 s (2g 에서 죽음) | 1188 MiB, 67 s |
  | 나머지 7 개 (7–42 쪽) | — | 1235–1634 MiB, 52–371 s, 모두 끝까지 |

  - 묶음 없이 `--jobs 1` 은 640 MiB 였으나 2–2.5 배 느렸다. 5000 px 은 31 쪽 문서에서 1970 MiB 로 2g 에 붙었다.
  - 상한: 가장 높은 1634 MiB 에 run·작업자 프로세스 ~75 MiB 를 더한 1709 MiB 의 1.5 배, 2564m. 호스트 예산 가드는 62.00g 중
    61.96g 으로 통과한다(남는 것이 거의 없다: 다음 상한 증설은 다른 상한을 줄이며 해야 한다).
  - 글자 수는 31 쪽 문서에서 3.5 % 줄었다. 운영은 글자가 모자란 쪽만 OCR 하므로 영향은 그 쪽들뿐이다.
  - 측정은 모든 쪽을 `--force-ocr` 로 돌렸다(운영보다 많다). 운영의 첫 run 뒤 `peak_memory_mib.largest_child` 로 다시 확인한다.

- 기각: 상한만 올리기. 작업 수 4 에 큰 쪽이 겹치면 상한을 또 넘고, 늘린 만큼 같은 host 의 다른 일이 덜 받는다(ADR-0455).
  `--jobs 1`: 최고는 640 MiB 로 내려가지만 OCR 이 2–2.5 배 느려져 run 예산(55 분)에 읽는 문서가 준다. `--jobs 2` 는 재지 않았다.
  쪽마다 따로 OCR: 최고는 여전히 가장 큰 쪽 × 작업 수라 묶음이 없으면 같은 문제다.
  추출기 판 올리기: 1.6 만 개를 처음부터 다시 읽는다(부분 처리 원칙에 어긋난다).

## Revision 2026-11-20: 쪽은 300 dpi 로 먼저 그린다

배포 뒤 첫 run(20261119T224659Z, 상한 2564m·4000 px 묶음)에서 처음 30 개 중 5 개가 또 상한에 끊겼다(표지는 결정 1 대로
`memory cap: ocrmypdf exit -9`, 커널: ocrmypdf 프로세스 자신이 2.1–2.5 GB). 그 5 개를 읽기 키로 받아 재 보니 쪽이 1,134–1,200 dpi
스캔이었다(8977×12284 px, 쪽당 110–125 Mpx). OCRmyPDF 16.7.0 은 쪽을 그 쪽에서 가장 촘촘한 그림의 해상도로 상한 없이 그린다
(`pdfinfo/info.py` 의 take_max, `_pipeline.get_canvas_square_dpi`; 300 으로 묶는 것은 미리보기뿐). 결정 5 의 묶음은 그 그림이 다
그려진 뒤에야 줄인다. 첫 측정의 표본 10 개에는 그런 쪽이 없었다.

그래서 OCR 할 쪽만 Ghostscript(이미 이미지에 있다)로 `ocr_raster_dpi` 300 의 그림 PDF 로 먼저 그리고, ocrmypdf 는 그것을 읽는다.
측정(같은 패키지, 상한 2564m, 모든 쪽 OCR): 끊긴 5 개 598–709 MiB, 앞의 큰 3 개 641–759 MiB, 모두 끝까지, 글자는 앞의 읽기와
−4 %..+1.4 %. 상한은 결정 5 의 2564m 에서 (759 와 LibreOffice 777 중 큰 것 + 75) × 1.5 = 1280m 로 내린다.
`memory_kill.attempts` 를 3 에서 6 으로 올려(결정 2 가 정한 길), 이 고침 전에 횟수를 다 쓴 문서도 다시 빚진다. 추출기 판은 올리지 않는다.
