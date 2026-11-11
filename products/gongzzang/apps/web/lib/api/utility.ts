import type { Energy } from "./energy";

// 인입 가능성: what public data says about whether a parcel is reached by city gas and a road
// (platforms/foundation-platform/docs/catalog/parcel-utility-connectivity-sources.md). Every signal
// is read from a document the panel already has; none is a separate fetch. A signal is evidence, not
// a ruling: no public source says a line can be connected, so a parcel without one shows nothing,
// never "없음".

/**
 * 도시가스: the newest month the 지번 has a 가스 사용량 row in its energy document (root ADR-0398) —
 * a 수용가 metered there, so a gas line reaches the parcel. Null when no month of the window has a
 * row: the provider leaves out houses and 산업용 meters, so "no row" says nothing about the pipe.
 */
export function gasConnection(energy: Energy | null | undefined): { latestUseYm: string } | null {
  const months = (energy?.months ?? []).filter((month) => month.gas_rows > 0);
  if (months.length === 0) return null;
  const latest = months.reduce((a, b) => (b.use_ym > a.use_ym ? b : a));
  return { latestUseYm: latest.use_ym };
}
