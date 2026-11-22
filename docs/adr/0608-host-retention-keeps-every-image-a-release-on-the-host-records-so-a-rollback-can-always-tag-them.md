# ADR 0608: host retention 은 호스트에 남은 릴리스가 적은 이미지를 모두 남긴다 — rollback 이 언제든 태그할 수 있게

- Status: Accepted
- Date: 2026-11-23
- Amends: [ADR-0198](./0198-server-scratch-journal-and-unused-images-age-out-by-themselves.md) §3(릴리스 소유 조건)
- Related: [ADR-0607](./0607-the-raon-image-the-host-builds-stays-until-its-inputs-change-and-a-raon-run-takes-thirteen-files.md)(같은 §3 의
  호스트가 짓는 이미지), [ADR-0276](./0276-ci-builds-and-signs-each-release-and-the-server-only-verifies-and-runs-it.md)(레지스트리 릴리스)

## Context

ADR-0198 §3 의 "릴리스 정리가 소유한 이미지"는 저장소 이름 둘(`foundation-outbox-publisher`, `foundation-tippecanoe`)이다. 서버가
릴리스를 짓던 때는 그것이 릴리스의 이미지 전부였다. 레지스트리 릴리스(ADR-0276)는 아홉 이미지를 다이제스트로 당기고 `build.json` 에
`<이름>_image` 로 적는다. runtime, postgres-recovery, Dagster, Airflow, Dawneer, 문서 추출, 검증 이미지가 그렇다.
`foundation-release.sh migrate`(rollback 도)는 그 ID 를 compose 가 부르는 `:local` 로 태그한다(`tag_compose_images`).

current 의 이미지는 `:local` 태그와 돌고 있는 컨테이너가 지킨다. previous 와 비상 릴리스의 compose 이미지는 레지스트리 다이제스트
참조만 붙들고 있다. 배포가 7 일 없으면 host retention 이 그것들을 다른 이미지처럼 지운다. 그 뒤의 rollback 은 태그할 이미지가 없어
멈춘다. 2026-11-23 에는 남은 릴리스가 적은 이미지가 모두 있었다. 배포가 매일 있었기 때문이다.

## Decision

1. **릴리스가 적은 이미지는 릴리스 정리의 것이다.** host retention(`host_retention.py`)은 `--release-root`(기본
   `/opt/foundation-platform`)의 `artifacts/<40 자 SHA>/build.json` 마다 `<이름>_image` 값을 모으고, 그 이미지는 지우지 않는다.
   current·previous·비상·가장 새 몇을 가리지 않는다. 그 릴리스들을 지울 때 이미지도 함께 지우는 것은 `foundation-release.sh
   prune` 이다(레지스트리 이미지 포함, #1632). admission 의 임시 디렉터리(`.registry-*`)는 릴리스가 아니다.
2. **모르면 지우지 않는다.** 그런 `build.json` 하나라도 읽지 못하면 그 실행은 아무것도 지우지 않고 65 로 끝난다. 릴리스 루트가 없는
   호스트는 릴리스가 없으니 지킬 이미지도 없다.

## Consequences

- previous 의 compose 이미지 몇 GB 가 7 일 넘게 남을 수 있다. 그 대신 언제든 rollback 할 수 있다.
- 릴리스의 이미지는 한 곳(prune)이 지우고, 나머지 이미지는 다른 한 곳(retention)이 지운다.
- 시험(`test_host_retention.py`)
  - 릴리스가 적은 이미지는 얼마나 오래돼도 남는다. `<이름>_image` 가 아닌 값과 임시 디렉터리는 셈하지 않는다.
  - 읽지 못하는 `build.json` 은 실행을 멈춘다.
