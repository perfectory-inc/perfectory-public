// The console's menus and where each address goes (root ADR-0114, ADR-0188).

/** The tabs under the 데이터 흐름 graph: everything the old 운영 현황 and 데이터 카탈로그 pages showed. */
export const FLOW_TABS = [
  { id: "timeline", label: "실행 타임라인" },
  { id: "jobs", label: "작업 목록" },
  { id: "quality", label: "품질 검사" },
  { id: "freshness", label: "수집·적재 기록" },
  { id: "catalog", label: "데이터 찾기" },
] as const;
export type FlowTab = (typeof FLOW_TABS)[number]["id"];

export type Route =
  | { page: "review-list" }
  | { page: "review-item"; itemId: string }
  | { page: "flow"; tab: FlowTab }
  | { page: "tiles" }
  | { page: "complexes" };

/** Addresses that moved, and where they live now; an old link must still land somewhere. */
const MOVED: Record<string, string> = {
  "/operations": "/flow",
  "/catalog": "/flow/catalog",
};

/** Where `path` (the hash without `#`) now lives, or null when it has not moved. */
export function movedTo(path: string): string | null {
  return MOVED[path] ?? null;
}

export function resolveRoute(path: string): Route {
  const target = movedTo(path) ?? path;
  const itemId = target.match(/^\/items\/([0-9a-f-]{36})$/)?.[1];
  if (itemId) return { page: "review-item", itemId };
  const flow = target.match(/^\/flow(?:\/([a-z]+))?$/);
  if (flow) {
    const tab = FLOW_TABS.find((candidate) => candidate.id === flow[1])?.id ?? "timeline";
    return { page: "flow", tab };
  }
  if (target === "/tiles") return { page: "tiles" };
  if (target === "/complexes") return { page: "complexes" };
  return { page: "review-list" };
}

/** The console's menus; each is a platform's own screen composed here (root ADR-0114). */
export const MENU = [
  { href: "#/", label: "필지 계보 검토", page: ["review-list", "review-item"] },
  { href: "#/flow", label: "데이터 흐름", page: ["flow"] },
  { href: "#/tiles", label: "지도 타일 발행", page: ["tiles"] },
  { href: "#/complexes", label: "산업단지", page: ["complexes"] },
] as const;
