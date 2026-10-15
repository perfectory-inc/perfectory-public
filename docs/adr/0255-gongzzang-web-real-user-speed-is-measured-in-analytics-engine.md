# ADR 0255: Gongzzang web real-user speed is measured in Workers Analytics Engine

- Status: Proposed (waiting for the owner's OK; no code until it is Accepted)
- Date: 2026-10-15

## Context

Measured from Korea, Gongzzang requests enter Cloudflare at LAX on the Free zone plan, and each
request on a new connection to a data edge takes about 0.8–1.4 s. PR #575 (preconnect) and #576
(the buildings view fetches unit prices alongside the buildings) target that latency. Lab runs on
the preview cannot show what real visitors get: the preview relays every edge through its own
origin, and one laptop is not the population of users.

The web app has no real-user sink to send to:

- `lib/panel/telemetry.ts` creates an OTEL span `panel.opened` with `panel.fetch_ms`, but no
  browser exporter exists, so the span is dropped. Its `window.dataLayer` push reaches no
  analytics tag either.
- `instrumentation.ts` starts the Node SDK on the server only.
- GZ-ADR-0008 plans Sentry for RUM, but it is not deployed and its DSN is not provisioned.
- The `web-vitals` package is not a dependency, and adding an npm package needs the owner's OK
  (gongzzang AGENTS.md §6).

## Decision (proposed)

1. **Collector.** The browser uses `web-vitals` (Google, Apache-2.0, about 2 KB gzip) to read
   LCP, INP, CLS and TTFB. It also reports one custom metric, `panel_data_ready`: the time from
   the click that opens a panel view (`PanelEntryView` mount) to the moment its data has rendered
   (the query succeeds and the view commits). The metric is reported per lane (`parcel.summary`,
   `parcel.buildings`, `unit-price`, …). It reuses the timing that `panel.fetch_ms` already
   measures.
2. **Transport.** The browser sends one `navigator.sendBeacon` per page hide (`visibilitychange`
   → hidden), batching every metric for that page view.
3. **Sink: a `/rum` route on an existing Worker that writes to Workers Analytics Engine.** The
   account is on Workers Paid, which includes 10 M data points a month. At the 10% sampling rate
   below, that allowance covers far more traffic than pre-launch, so the cost is 0. The data is
   queried with the Analytics Engine SQL API (the same token, no new store). The recommended host
   is the by-PNU gateway's parcel hostname: it is already in the CSP `connect-src` and already
   preconnected, so the beacon adds no origin. If the gateway must stay data-only (root
   ADR-0190's "data gives values"), a one-route Worker on its own hostname is the fallback. That
   is a new service and needs a separate OK.
4. **Privacy.** No PNU, no URL, no user or session id, and no IP or user agent stored. A data
   point is `(metric, lane, value_ms, rating, connection effectiveType, country)`. Country comes
   from `request.cf` and is needed to tell Korea from everywhere else. Sampling is 10% per page
   view, decided once in the browser.
5. **Guard.** A unit test fails if the beacon payload contains any key outside the allowlist in
   §4.

## Alternatives rejected

- **A Next route handler that logs the metrics (pino).** Production runs on AWS ECS, which this
  work must not touch, and no log query exists to aggregate the lines.
- **Cloudflare Web Analytics.** It is free and collects Core Web Vitals, but it cannot take the
  custom `panel_data_ready` metric, and it adds a third-party script and a CSP entry.
- **Sentry RUM (GZ-ADR-0008).** It needs a DSN, a source-map pipeline and a plan decision. That is
  the right destination later, and the collector in §1 can be pointed at it then.

## Consequences

- Each PR's effect (#575, #576 and later ones) shows up as the Korea p75 of `panel_data_ready`
  per lane, before and after.
- One new npm dependency (`web-vitals`), and one route plus one Analytics Engine binding on an
  existing Worker.
