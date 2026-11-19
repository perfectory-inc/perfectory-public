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

1. **메모리 종료에는 표지가 붙는다.** run 은 문서 하나를 읽기 앞뒤로 컨테이너 자신의 cgroup `memory.events` 의 `oom_kill` 을
   읽는다. 그 사이 늘었고 결과가 `failed` 면 오류가 `memory cap: ` 으로 시작하고, 요약에 `memory_killed` 로 센다. 추측이 아니라
   커널이 센 수다.
2. **메모리 종료는 다시 읽는다, 정해진 횟수까지.** 계약 `document_text.memory_kill` 이 표지(`error_prefixes`: 새 표지와 표지 전의
   두 오류 모양)와 `attempts`(3)를 둔다. plan-text 는 해시의 *현재* 결과(계약의 `current_row_latest`, 새 run 이 이긴다)가
   메모리 종료이고 결과 행이 `attempts` 보다 적으면 다시 빚진 것으로 센다. 메모리 종료는 문서가 아니라 상한에 대한 사실이라 다시
   읽고, 횟수는 어떤 상한에도 안 맞는 문서가 run 마다 읽히지 않게 한다. 결과 표는 그대로 덧붙이기만 한다.
3. **사실은 가장 새 텍스트를 따른다.** plan-facts 는 해시의 현재 텍스트 결과의 run 이 현재 사실 결과의 run 보다 나중이면 다시
   빚진 것으로 센다. 다시 읽힌 문서는 사람 손 없이 사실까지 다시 뽑힌다.
4. **메모리 종료가 하나라도 있으면 유닛이 실패한다.** 끝낸 문서를 덧붙인 뒤 `exit 1`: OnFailure 가 주인(`X-Foundation-Owner`)을
   부른다. 2 의 횟수가 있어 같은 문서는 많아야 `attempts` 번 실패시킨다.
5. **Tesseract 에는 묶은 그림만 준다.** `ocrmypdf --tesseract-downsample-large-images --tesseract-downsample-above <px>` 로 OCR 에
   넘기는 그림만 긴 변 `ocr_downsample_above_px` 로 줄인다. 16.7.0 태그의 `builtin_plugins/tesseract_ocr.py`
   `filter_ocr_image` 가 줄인 그림은 `ocr.png` 로만 저장되고, 남는 쪽은 그와 별개로 만든다. 300 dpi A4(긴 변 3508 px)는 손대지
   않는다. 값과 상한은 아래 측정으로 정했다.

## Consequences

- 운영의 메모리 종료 185 개는 이 PR 이 배포된 뒤의 첫 run 부터 다시 빚진다(`owed` 가 그만큼 는다). 사람이 할 일은 없다.
- 결과 표 두 개의 품질 관문 문장 "(sha256, extractor_version) unique" 는 원래 현재 행 하나를 뜻했다. 다시 읽기가 생겨 문장을
  "현재 행 하나, 다시 읽으면 새 run 의 행이 덧붙는다"로 고친다. 그것을 세는 검사는 없었다.
- 측정(노트북, 운영과 같은 패키지, `--cpus 4`, 컨테이너 anon 최고):

  MEASUREMENT_TABLE

- 기각: 상한만 올리기. 작업 수 4 에 큰 쪽이 겹치면 상한을 또 넘고, 줄을 늘린 만큼 같은 host 의 다른 일이 덜 받는다(ADR-0455).
  `--jobs 1`: 최고는 640 MiB 로 내려가지만 OCR 이 2–2.5 배 느려져 run 예산(55 분)에 읽는 문서가 준다.
  추출기 판 올리기: 1.6 만 개를 처음부터 다시 읽는다(부분 처리 원칙에 어긋난다).
