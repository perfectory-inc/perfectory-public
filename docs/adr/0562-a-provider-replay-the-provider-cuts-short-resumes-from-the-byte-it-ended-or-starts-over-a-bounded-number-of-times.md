# ADR 0562: 제공자가 중간에 끊은 원천 내려받기는 끊긴 바이트부터 이어받고, 안 되면 정해진 횟수만큼 처음부터 다시 받는다

- Status: Accepted
- Date: 2026-11-12
- Builds on: [ADR-0170](./0170-large-vworld-files-come-through-the-raon-agent-on-the-data-host.md)(RAON 대용량 파일), [ADR-0514](./0514-the-raon-lane-takes-a-capped-count-of-the-newest-files-every-day-and-a-listed-file-not-in-bronze-pages.md)(RAON 레인 매일 8개), [ADR-0557](./0557-a-job-that-lands-only-bronze-is-not-held-by-a-release-that-changes-lakehouse-tables.md)(배포가 훑기를 끊지 않음)
- 관련 사건: 2026-11-12 08:08Z 첫 무인 RAON 실행(`raon-20261112T080800Z`)이 8개 중 4개만 Bronze 에 넣었다. 실패한 4개는 경기 용도지역지구 `AL_D155_41_20261109.zip`(926 MB, 6분 받다 끊김)·경북 `AL_D155_47_20261109.zip`(545 MB, 2초)·`AL_D054_00`(971 MB, 2초)·`AL_D093_00`(542 MB, 5분)이고, 넷 모두 수입기 오류 `failed to read provider acquisition replay chunk … end of file before message length reached` 였다. 같은 세션의 `AL_D125_00`(595 MB)·`AL_D160_41/44/46` 은 1.5~2.5분에 들어왔다. 끊긴 파일은 처음부터 실패로 끝났고, 다음 시도는 다음 날 훑기다.

## Context

RAON 파일은 브라우저가 VWorld 의 `fileDown` 을 눌러 얻은 요청(`raonkhandler.jsp`, 토큰 포함)을 Rust 수입기가 다시 보내
본문 전체를 받는다(`provider_acquisition_import.rs fetch_and_stage_replay_response`). 같은 요청은 이미 두 번 쓰인다(파이썬
워커의 4바이트 확인과 수입기의 전체 받기) — 다시 보낼 수 있다. 그러나 수입기는 한 번 보내 본문이 끝까지 오지 않으면
바로 실패한다. 연결이 끊기는 이유는 우리 쪽이 아니다(우리 시간 제한 없음, 같은 세션의 같은 크기 파일은 성공).

업계의 같은 문제: curl `--continue-at -`·wget `-c`·aria2·rclone 은 받던 파일의 길이를 `Range: bytes=<받은 길이>-` 로 보내
이어받고, 서버가 `206 Partial Content` 와 그 시작의 `Content-Range` 로 답할 때만 이어 붙인다. 아니면(200) 처음부터 다시
받는다. AWS SDK·gsutil 도 대용량 내려받기를 같은 방식으로 정해진 횟수까지 재시도한다.

## Decision

1. 수입기는 한 요청의 본문이 끊기면(연결 오류·선언 길이 전 종료) 같은 요청을 최대 4번까지(처음 포함) 보낸다. 간격은
   2·4·8초.
2. 앞 응답이 `Accept-Ranges: bytes` 였거나 이미 206 으로 이어 받았으면 받은 길이로 `Range` 를 보낸다. 답이 206 이고
   `Content-Range` 가 그 바이트에서 시작할 때만 이어 붙인다. 그 밖의 성공 응답은 스테이징 파일을 버리고 처음부터 쓴다 —
   스테이징된 바이트는 언제나 한 본문을 순서대로 담고, SHA-256 은 받으며 계산한다.
3. 거부(2xx 아님)·HTML 응답·ZIP 서명 아님·로컬 쓰기 실패는 다시 보내지 않는다(다시 보내도 같다).
4. 4번 모두 끊기면 실패하고 스테이징 파일을 지운다. 다음 날 훑기가 그 파일을 다시 고르고(ADR-0514: 목록에 있는데
   Bronze 에 없는 파일), 72시간이 지나면 `FoundationSourceFilesUnfetched` 가 울린다.
5. 크기 검증(`validate_staged_replay_size`)·ZIP 내용 검증은 그대로 마지막에 한 번 한다.

## Consequences

- 시험(`provider_acquisition_import/tests.rs`): 선언 길이보다 적게 보내고 끊는 시험 제공자로, Range 이어받기·Range 없는
  처음부터 다시·네 번 모두 끊김(아무것도 착지 안 함)을 확인한다.
- VWorld 가 Range 를 받는지는 아직 모른다(이 노트북에서 RAON 을 돌리지 않는다). 받지 않아도 2번의 "처음부터 다시" 가
  간헐 끊김을 넘는다. 다음 무인 실행의 로그 `provider acquisition replay body ended early; asking again` 과 그 뒤 요청의
  응답이 답한다.
- 최악의 추가 시간: 파일당 대기 14초 + 다시 받는 시간. RAON 한 실행 상한 8개·16 GiB 안이다.
