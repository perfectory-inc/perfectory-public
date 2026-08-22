// @vitest-environment node
import { describe, expect, it } from "vitest";
import koMessages from "@/lib/i18n/ko.json";
import { KINDS } from "@/lib/panel/codec";

/**
 * `PanelEntryView` looks the panel header label up as `panel.labels.<kind>.<view>`. A missing entry
 * renders the message key path to the user, which is the failure the ternary it replaced used to
 * hide behind a fallback branch. The kind/view set comes from the codec, so a kind added there
 * without a label fails here rather than in front of a user.
 */
describe("panel header labels", () => {
  const labels = koMessages.panel.labels as Record<string, Record<string, string> | undefined>;

  it("has a label for every kind and view the codec accepts", () => {
    const missing: string[] = [];
    for (const [kind, meta] of Object.entries(KINDS)) {
      for (const view of meta.views) {
        const label = labels[kind]?.[view];
        if (typeof label !== "string" || label.trim() === "") missing.push(`${kind}.${view}`);
      }
    }
    expect(missing).toEqual([]);
  });

  it("carries no label for a kind or view the codec does not accept", () => {
    const stale: string[] = [];
    for (const [kind, views] of Object.entries(labels)) {
      const meta = KINDS[kind as keyof typeof KINDS];
      if (!meta) {
        stale.push(kind);
        continue;
      }
      for (const view of Object.keys(views ?? {})) {
        if (!meta.views.has(view)) stale.push(`${kind}.${view}`);
      }
    }
    expect(stale).toEqual([]);
  });
});
