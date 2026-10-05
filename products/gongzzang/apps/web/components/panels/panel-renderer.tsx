// apps/web/components/panels/panel-renderer.tsx
"use client";

// The composition root: the panel framework plus every kind it can open. Registration lives here,
// not in `lib/panel/`, because the framework must not depend on its kinds (spec § 9 #5, enforced by
// `scripts/lefthook/panel-no-framework-import-kind.sh`). Pages mount this `PanelRenderer`, so a
// kind is registered wherever a panel can open — the `complex` kind is never left out again.
import "./complex/register";
import "./listing/register";
import "./parcel/register";
import { PanelRenderer as FrameworkPanelRenderer } from "@/lib/panel/panel-renderer";

export function PanelRenderer() {
  return <FrameworkPanelRenderer />;
}
