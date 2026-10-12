import { createElement } from "react";
import { renderToStaticMarkup } from "react-dom/server";
import { describe, expect, it } from "vitest";

import type { ScheduledJobStatus } from "../api/client";
import { HealthLine } from "./FlowPage";

const failing: ScheduledJobStatus = {
  job_id: "load",
  dag_id: "synthetic_load",
  description: "a synthetic job",
  schedule: "20 7 * * *",
  enabled: true,
  disabled_reason: null,
  next_run_at: null,
  catalog_job_urn: null,
  last_run: null,
  typical_duration_seconds: null,
  typical_duration_runs: 0,
  recent_runs: [],
  started_by: "schedule",
  upstream_job_ids: [],
  pipeline_graph_edges: [],
  reads: [],
  writes: [],
};

function line(rows: Parameters<typeof HealthLine>[0]["rows"]): string {
  return renderToStaticMarkup(
    createElement(HealthLine, { rows, titleOf: (id: string) => id, now: new Date("2026-01-02T12:00:00Z"), onOpen: () => {} }),
  );
}

describe("the top line", () => {
  it("announces its problems from a live region that keeps the heading a heading", () => {
    const markup = line([
      { job: failing, problem: { kind: "failed", cause: null, cause_line: null, scheduler_run_id: null }, nodeId: null },
    ]);
    // An h2 given role="status" is no longer a heading to a screen reader.
    expect(markup).not.toMatch(/<h2[^>]*role=/);
    expect(markup).toMatch(/<div role="status"><h2 id="health-line"[^>]*>.*문제 1건<\/h2><\/div>/);
  });

  it("says all is fine from a live region", () => {
    expect(line([])).toMatch(/<p[^>]*role="status"[^>]*>.*전체 정상<\/p>/);
  });
});
