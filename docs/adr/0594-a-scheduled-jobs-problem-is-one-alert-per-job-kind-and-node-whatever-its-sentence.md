# ADR 0594: 작업의 문제는 작업·종류·노드마다 알림 하나다 — 문장이 바뀌어도 같은 알림이다

- Status: Accepted
- Date: 2026-11-20
- Amends: [ADR-0208](./0208-the-data-flow-maps-problems-reach-slack-through-the-existing-alertmanager.md) 결정 2("새 실행이 원인을 바꾸면 레이블이 바뀌고, 그건 새 문제다")
- Related: [ADR-0591](./0591-alertmanager-sends-tickets-batched-per-rule-and-pages-as-they-come.md) (티켓 묶음)

## Context

foundation-api 는 지도의 문제를 `foundation_scheduled_job_problem{job_id,kind,node_id,summary,link} 1` 로 낸다(ADR-0208 결정 1). 알림의 정체는 레이블 집합이다. 그래서 `summary`(문제의 문장)나 `link` 가 바뀌면 새 알림이 된다. 새 알림은 메시지 한 통이고, 옛 것은 해소 한 통이다.

ADR-0208 결정 2 는 이를 일부러 택했다. 원인이 바뀌면 새 문제라는 것이다. 2026-11-18/19 서버에서 18시간 잰 값(ADR-0591 의 측정):

| 사실 | 값 |
|---|---|
| `FoundationScheduledJobProblem` 시계열 | 123 |
| 그중 같은 작업·종류에 문장만 다른 것 | 31 (11 쌍) |
| 예 | court_auction_silver `volume` 14개: "보통 104133행인데 이번엔 30434행", "보통 6255행인데 이번엔 1893행" … 실행마다 행 수가 문장에 든다 |
| 예 | data_quality `blocked` 셋: 막은 앞 작업 목록이 하나 늘거나 줄 때마다 |

같은 작업이 같은 종류의 문제로 있는 동안, 행 수나 앞 작업 목록이 바뀐 것은 새 문제가 아니다. 읽는 사람에게는 같은 일이 해소되었다가 다시 생긴 것으로 보인다.

Prometheus 3.5.0 문서(`docs/configuration/template_reference.md` @v3.5.0)에서 확인한 것:
- 경보의 annotation 템플릿은 `query` 함수로 지금의 시계열을 읽을 수 있다.
- `first` 와 `.Labels` 로 그 레이블을 꺼낸다(`template_examples.md` 의 `printf … | query` 모양).

## Decision

1. **정체는 `job_id`·`kind`·`node_id` 다.** 규칙의 식은 `max without (summary, link) (foundation_scheduled_job_problem) == 1` 이다. 문장과 링크는 알림의 레이블이 아니다.
2. **문장은 지금의 시계열에서 읽는다.** annotation `summary` 가 같은 `job_id`·`kind`·`node_id` 의 시계열을 `query` 로 읽어 그 `summary` 와 `link` 를 적는다. 메시지는 늘 지도가 지금 쓰는 문장을 보인다.
3. **내보내는 쪽(`alerts.rs`)은 그대로다.** 지도와 더니어가 같은 레이블을 읽는다. 바꾸는 것은 규칙 하나다.
4. **시험**: 두 규칙(`FoundationScheduledJobProblem`, `FoundationScheduledJobsUnreadable`)을 `foundation-api` 묶음에서 새 묶음 `foundation-data-flow` 로 옮긴다. 가드 `prometheus-rules-pass-their-tests` 는 "시험된 알림이 하나라도 있는 묶음은 모두 시험되어야 한다"고 요구하기 때문이다.
   - `foundation-data-flow.rules.test.yml` 이 셋을 본다.
     - 문장이 바뀌어도 알림이 하나이고, annotation 이 새 문장이다.
     - 같은 작업의 두 종류는 두 알림이다.
     - 읽지 못하면 30분 뒤 Unreadable 이 뜬다.
   - 옛 식(`foundation_scheduled_job_problem == 1`)으로는 첫 시험이 실패한다(15분에 알림 둘).

## Rejected

| 대안 | 판정 |
|---|---|
| 내보내는 쪽에서 `summary`·`link` 를 별도 info 시계열로 | 지도·더니어·시험까지 바뀐다. 같은 효과를 규칙 하나가 낸다 |
| `metric_relabel_configs` 로 두 레이블 지우기 | 문장을 아예 잃는다. 메시지가 원인을 말하지 않는다 |
| Alertmanager `group_by` 로만 묶기 | 묶음 안에서도 알림은 따로라 더해지고 해소되는 변화가 그대로다 |

## Consequences

- 같은 작업의 같은 종류의 문제는 문장이 바뀌어도 메시지를 새로 만들지 않는다. 바뀐 문장은 다음 묶음 메시지에 보인다. 티켓이라 많아야 한 시간 뒤이고, 그대로면 하루 재알림 때다(ADR-0591).
- 원인이 바뀐 것을 바로 알리지 않는다(ADR-0208 결정 2 와 다르다). 원인이 바뀌어도 `kind` 가 같으면 같은 실패이고, 고칠 사람은 지도를 연다. `kind` 가 바뀌면(예: `slow` 에서 `failed`) 새 알림이다.
- `keep_firing_for` 15분 동안 시계열이 없으면 annotation 의 문장이 빈다(작업·종류만 남는다). 그동안 새 메시지는 없다.
