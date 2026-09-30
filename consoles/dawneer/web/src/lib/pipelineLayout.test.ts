import { describe, expect, it } from "vitest";

import { COLUMN_WIDTH, health, layout, ROW_HEIGHT, type PipelineNode } from "./pipelineLayout";

const node = (id: string, type: string, status: string, title = id): PipelineNode =>
  ({ id, type, status, title, owner: "foundation-platform", description: "", evidence_refs: [] }) as unknown as PipelineNode;

describe("pipeline layout", () => {
  it("puts each layer in its own column in the order data flows", () => {
    const positions = layout([node("g", "gold_table", "implemented"), node("s", "source_group", "implemented"), node("v", "serving_surface", "implemented")]);
    expect(positions.get("s")?.x).toBe(0);
    expect(positions.get("g")?.x).toBe(3 * COLUMN_WIDTH);
    expect(positions.get("v")?.x).toBe(5 * COLUMN_WIDTH);
  });

  it("lists working nodes above broken ones inside a column", () => {
    const positions = layout([node("broken", "silver_table", "missing", "가"), node("ok", "silver_table", "implemented", "하")]);
    expect(positions.get("ok")?.y).toBe(0);
    expect(positions.get("broken")?.y).toBe(ROW_HEIGHT);
  });

  it("reads every status as one of four states and anything unknown as broken", () => {
    expect(health("implemented")).toBe("working");
    expect(health("schema_defined")).toBe("declared");
    expect(health("waiting")).toBe("pending");
    expect(health("blocked")).toBe("broken");
    expect(health("something_new")).toBe("broken");
  });
});
