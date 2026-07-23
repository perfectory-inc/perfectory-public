# ADR 0002: Docs taxonomy and archive policy

> Amended by [ADR-0007](./0007-public-code-private-operations-boundary.md): historical archive,
> review, and handoff trees remain in the private transition archive and are not part of the public
> canonical code tree.

- Status: Accepted
- Date: 2026-07-20

## Context

시점 기록(plans/handoffs/specs/research/census)이 현행 문서와 같은 폴더에 섞이면
과거 절차가 현재 지침으로 오인된다. 영역마다 아카이브 선언과 위치가 달랐고, ADR
번호도 영역별 시퀀스가 병행되어 `ADR-0021`이 어느 영역 결정인지 모호했다.

## Decision

1. **문서는 두 부류뿐이다: `current`(현행) | `archived`(기록).** 제3의 상태는 없다.
   현행 문서는 지금 따라 해도 맞아야 하고, 기록은 작성 시점의 사실·의도 보존이 목적이다.

2. **dated 기록의 정의:** 파일명에 날짜가 박힌 plans / handoffs / specs / research /
   census / evidence 류는 전부 기록(archived)이다. 기록은 사후 재작성하지 않는다 —
   허용되는 수정은 깨진 상대링크 경로 교정과 날짜 명기된 개정 각주 추가뿐이다
   (링크만 고치는 것은 역사 왜곡이 아니다).

3. **공개 코드 트리에는 아카이브를 두지 않는다.** 시점 기록과 운영 증거는
   ADR-0007이 정의한 비공개 전환 아카이브 또는 운영 증거 저장소가 소유한다. 공개
   트리에는 지금 따라도 맞는 현행 계약과 절차만 남긴다.

4. **frontmatter 최소 세트:** 신설·전면 개정하는 문서는 YAML frontmatter
   `status: current|archived`를 갖는다 (+선택 `owner:`). 기존 문서에 소급 부여하지
   않고, 손대는 시점에 부여한다. ADR은 자체 `Status:` 필드가 있으므로 면제한다.

5. **ADR 번호는 루트 전역 시퀀스 하나다.** 신규 ADR은 영역 무관하게 루트 `docs/adr/`의
   다음 번호를 쓴다. 기존 영역 `docs/adr/` 시퀀스는 동결한다 (gongzzang 0050,
   foundation 0027, identity 0001, intelligence 0001에서 종료). 동결된 영역 ADR은
   영역 접두 ID로 인용한다: `GZ-ADR-NNNN`(gongzzang), `FP-ADR-NNNN`(foundation),
   `IDP-ADR-NNNN`(identity), `ITP-ADR-NNNN`(intelligence). 무접두 `ADR-NNNN`은 루트
   시퀀스를 가리킨다. Accepted-불변·supersession 규칙은 영역 ADR에도 계속 적용된다
   (대체는 루트 번호 신규 ADR로).

6. **중복 서술 금지.** 한 규칙 = 한 정본. 다른 문서는 정본 링크만 둔다. 사본이
   불가피하면 사본임을 명시한다 (gongzzang AGENTS §8 SSOT 원칙의 모노레포 확장).

## Consequences

- 공개 저장소 가드는 archive/review/handoff/agent-memory 경로의 재유입을 차단한다.
- 현재 계약 레지스트리와 규칙 문서(`*.v1.json`, `*-rules*.md` 등)는 소유 영역의
  maintained docs에 남고 CI가 코드와의 drift를 검사한다.
- 공개 문서에서 필요한 운영 증거는 값 자체를 복제하지 않고 비공개 증거 시스템의
  검증 절차와 소유권만 설명한다.
