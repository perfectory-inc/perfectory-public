import { Suspense } from "react";
import { ComplexList } from "@/components/complexes/complex-list";
import { PanelRenderer } from "@/lib/panel/panel-renderer";

/**
 * `/complexes` — 산업단지 목록·검색.
 *
 * The map was the only way to reach a complex; this is the other way. Clicking a row pushes the
 * same `complex` panel the map boundary pushes, onto the same `?p=` stack, so nothing about the
 * panel had to be built again — `PanelRenderer` registers the `complex` kind on import.
 *
 * `Suspense` because the whole screen reads `useSearchParams` (the filters are the URL), and Next
 * requires a boundary around that in a statically rendered route.
 */
export default function ComplexesPage() {
  return (
    <main className="flex h-screen flex-col bg-[var(--color-canvas)]">
      <Suspense fallback={null}>
        <ComplexList />
      </Suspense>
      {/* Single mount, fixed overlay — same placement as the listings page. */}
      <Suspense fallback={null}>
        <PanelRenderer />
      </Suspense>
    </main>
  );
}
