// 자원 한도 (root ADR-0574): every host unit's and container's declared memory and time limit against
// what its runs measured, as the host's half-hourly measurement wrote them. Read-only: a limit
// changes in code — a pull request through the host budget contract and its guard.
import type { ContainerLimit, UnitLimit } from "../api/client";

/** Where a limit lives: the host budget's unit entries are generated from the declared definitions
 * (scripts/catalog/declared_definitions.py), so a change is a pull request there. */
export const DEFINITIONS_SEARCH = "https://github.com/perfectory-inc/perfectory-public/search";

/** The state's order on the screen: what failed first, then what is near. */
const ORDER: Record<string, number> = { failing: 0, near: 1, fits: 2, unmeasured: 3 };

export const STATE_LABEL: Record<string, string> = {
  failing: "한도에서 죽음",
  near: "한도 가까움",
  fits: "넉넉함",
  unmeasured: "run 없음",
};

/** Failing first, then near, each by the share of its limit it reached, highest first. */
export function byUrgency<T extends { state: string; memory_share?: number | null; time_share?: number | null }>(
  rows: readonly T[],
): T[] {
  const reach = (row: T) => Math.max(row.memory_share ?? 0, row.time_share ?? 0);
  return [...rows].sort((a, b) => (ORDER[a.state] ?? 9) - (ORDER[b.state] ?? 9) || reach(b) - reach(a));
}

/** Bytes as MiB or GiB, the way the units declare them. */
export function size(bytes: number | null | undefined): string {
  if (bytes == null) return "-";
  const mib = bytes / 2 ** 20;
  return mib >= 1024 ? `${(mib / 1024).toFixed(1)}G` : `${Math.round(mib)}M`;
}

/** Seconds as the units declare them. */
export function duration(seconds: number | null | undefined): string {
  if (seconds == null) return "-";
  if (seconds < 120) return `${Math.round(seconds)}초`;
  if (seconds < 7200) return `${Math.round(seconds / 60)}분`;
  return `${(seconds / 3600).toFixed(1)}시간`;
}

export function percent(share: number | null | undefined): string {
  return share == null ? "-" : `${Math.round(share * 100)}%`;
}

/** The unit's last failure at a limit, or nothing. */
export function lastFailure(unit: UnitLimit): string {
  const kills = [
    unit.last_oom_at ? { at: unit.last_oom_at, what: "OOM" } : null,
    unit.last_timeout_at ? { at: unit.last_timeout_at, what: "timeout" } : null,
  ].filter((kill): kill is { at: string; what: string } => kill !== null);
  if (kills.length === 0) return "-";
  const newest = kills.reduce((a, b) => (a.at > b.at ? a : b));
  return `${newest.what} ${new Date(newest.at).toLocaleString("ko-KR")}`;
}

/** One unit as one sentence: what to do, by ADR-0574 §4 — grow the job's streaming, not the cap. */
export function unitSentence(unit: UnitLimit): string {
  if (unit.state === "failing") {
    const kills = [unit.oom ? `OOM ${unit.oom}번` : "", unit.timeout ? `timeout ${unit.timeout}번` : ""].filter(Boolean).join(", ");
    return `${unit.runs}번 중 ${kills} — 입력이 자랐으면 흘려 처리로 고치고, 아니면 잰 최고점 × 1.5 로 한도를 고치는 PR`;
  }
  if (unit.state === "near") {
    const streak = Math.max(unit.near_memory_runs, unit.near_time_runs);
    return streak >= 3 ? `최근 ${streak}번 연달아 80% 넘음 — 알림이 갔어요` : `한 번 이상 80% 넘음 — 연달아 3번이면 알림`;
  }
  if (unit.state === "unmeasured") return "기간 안에 끝난 run 이 없어요";
  return "잰 값이 한도 안이에요";
}

export function containerSentence(container: ContainerLimit): string {
  if (container.state === "failing") return `커널이 상한에서 ${container.oom}번 죽였어요`;
  if (container.state === "near") return "상한의 80% 넘게 썼어요";
  return container.state === "unmeasured" ? "잰 값 없음" : "상한 안이에요";
}

/** Where a change to this unit's limit starts: the pull request that edits its declared entry. */
export function proposalLink(unit: string): string {
  const query = `"${unit}" path:platforms/foundation-platform/definitions`;
  return `${DEFINITIONS_SEARCH}?${new URLSearchParams({ q: query, type: "code" }).toString()}`;
}
