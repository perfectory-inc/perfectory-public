# ADR 0255: Gongzzang web real-user speed is measured in Workers Analytics Engine

- Status: Accepted (owner OK 2026-10-16, within the limits in "Approval")
- Date: 2026-10-16

## Context

Measured from Korea, Gongzzang requests enter Cloudflare at LAX on the Free zone plan, and each
request on a new connection to a data edge takes about 0.8–1.4 s. #575 (preconnect) and #576 (the
buildings view fetches unit prices alongside the buildings) target that latency. Lab runs on the
read-only preview cannot show what visitors actually get: the preview relays every edge through
its own origin, and one laptop is not the population of visitors.

The web app had no real-user sink:

- `lib/panel/telemetry.ts` creates an OTEL span `panel.opened` with `panel.fetch_ms`, but no
  browser exporter exists, so the span is dropped. Its `window.dataLayer` push reaches no
  analytics tag either.
- `instrumentation.ts` starts the Node SDK on the server only.
- GZ-ADR-0008 plans Sentry for RUM, but it is not deployed and its DSN is not provisioned.

## Approval

The owner approved this ADR (2026-10-16) within four limits:

- (a) The npm package is `web-vitals` (Google), pinned in the version contract.
- (b) The beacon goes to a route on an **existing** Worker. No new Worker or service is added.
  Metrics are aggregated in Workers Analytics Engine.
- (c) Beacons are sampled. The payload carries no PNU, address or user id: only lane names and
  timings.
- (d) This revision and tests ship with the change.

## Decision

1. **Collector (web).** `web-vitals` 6.2.3 (Apache-2.0, no dependencies; pinned in
   `docs/technology-stack.md` §1.1) reports LCP, INP, CLS and TTFB. The web also reports one custom
   metric, `panel_data_ready`, per lane:
   - For each panel view (`parcel.summary`, `parcel.buildings`, `parcel.floors`,
     `parcel.listings`, `listing.summary`, `complex.summary`), it is the time from the panel's
     mount, which is the click that opened it, to the moment its fetched data has rendered.
     A cache hit is not reported: the metric measures a lane, not the cache.
   - For `parcel.unit-prices`, it is the time from the request's start, which is the click on the
     buildings view (#576), to the moment the prices have arrived.
2. **Transport.** Metrics are collected for 10% of page views, decided once per page load
   (`NEXT_PUBLIC_RUM_SAMPLE_RATE`, default `0.1`). The read-only preview, whose visitors are the
   team, sets `1`. When
   the page is hidden, the browser sends what it has collected as one
   `navigator.sendBeacon(text/plain)`. A text/plain beacon is a CORS-safelisted request, so it
   needs no preflight and its answer is never read.
3. **Sink: `POST /rum` on the existing map-edit gateway** (`map-edits.perfectory.io`,
   `foundation-map-edit-gateway`). That host is already in the web's CSP `connect-src`, and the
   map has a connection open to it from first load. The gateway checks the beacon against the
   contract block `map_edit_gateway.real_user_metrics`: metric names, the lane grammar, body,
   count and value bounds. If any entry fails, the whole beacon is dropped with 400. Otherwise the
   gateway writes one data point per metric, `(metric, lane, country, value)`, to the Analytics
   Engine dataset `gongzzang_web_rum`. Country is Cloudflare's `request.cf.country`. The read-only
   preview relays the beacon from its own origin and passes the visitor's country in
   `X-Rum-Viewer-Country`, because the relay's own country would be its colo. The account's
   Workers Paid plan includes 10 M data points a month, so at this sampling rate the cost is 0.
4. **Privacy.** The payload carries no PNU, URL, user or session id. The gateway stores no IP or
   user agent. The gateway's test drops beacons that carry a PNU field or a PNU-shaped lane. The
   web's test fails if a beacon carries any key other than `v`, `m`, `n`, `l`, `v`.
5. **Reading.** p75 per lane for Korea comes from the Analytics Engine SQL API:
   `quantileWeighted(0.75)(double1, _sample_interval)` over `blob1 = 'panel_data_ready'`, grouped
   by `blob2`, filtered on `blob3 = 'KR'`.

## Alternatives rejected

- **The by-PNU gateway (`catalog.perfectory.io`).** This was the first draft's pick because that
  host is preconnected. It was rejected because the gateway is the panels' data path, held to a
  CPU gate (root ADR-0233), and it serves values only (root ADR-0190). The map-edit gateway
  already takes POSTs and is not on the panel critical path.
- **A Next route handler that logs the metrics (pino).** Production runs on AWS ECS, which this
  work must not touch, and no log query exists to aggregate the lines.
- **Cloudflare Web Analytics.** It is free and collects Core Web Vitals, but it cannot take the
  custom `panel_data_ready` metric, and it adds a third-party script and a CSP entry.
- **Sentry RUM (GZ-ADR-0008).** It needs a DSN, a source-map pipeline and a plan decision. That is
  the right destination later, and the collector in §1 can be pointed at it then.
- **A new one-route Worker.** Excluded by the owner's limit (b).

## Consequences

- Each speed change (#575, #576 and later ones) can be judged by the Korea p75 of
  `panel_data_ready` per lane, before and after.
- One new npm dependency (`web-vitals`), and one route plus one Analytics Engine binding on an
  existing Worker. Server autodeploy ships the binding with the Worker.
- Before launch, the visitors are the team. The numbers describe the edge and the code, not
  customers, until there is traffic.
