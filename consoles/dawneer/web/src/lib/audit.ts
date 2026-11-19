// The audit chain screen's pure parts (root ADR-0569 §7): the filters as a query, the words shown for
// the chain's own words, and what changed between before and after.

import type { AuditEvent } from "../api/client";

/** What the screen filters by. `actor` is a staff account id, or `me` for the signed-in account. */
export interface AuditFilter {
  actor?: string;
  actorExternal?: string;
  objectType?: string;
  objectId?: string;
  source?: string;
  action?: string;
  result?: string;
  changeId?: string;
  /** `YYYY-MM-DD`, KST: from the start of that day. */
  from?: string;
  /** `YYYY-MM-DD`, KST: up to the end of that day. */
  to?: string;
}

/** A KST calendar day's start as an RFC 3339 instant. */
function kstDayStart(day: string, plusDays = 0): string | null {
  const match = day.match(/^(\d{4})-(\d{2})-(\d{2})$/);
  if (!match) return null;
  const utc = Date.UTC(Number(match[1]), Number(match[2]) - 1, Number(match[3]) + plusDays) - 9 * 3600 * 1000;
  return new Date(utc).toISOString();
}

/** The API query of a filter; empty fields are left out. */
export function auditQuery(filter: AuditFilter, extra: Record<string, string> = {}): URLSearchParams {
  const query = new URLSearchParams();
  const set = (name: string, value: string | undefined) => {
    const trimmed = value?.trim();
    if (trimmed) query.set(name, trimmed);
  };
  set("actor", filter.actor);
  set("actor_external", filter.actorExternal);
  set("object_type", filter.objectType);
  set("object_id", filter.objectId);
  set("source", filter.source);
  set("action", filter.action);
  set("result", filter.result);
  set("change_id", filter.changeId);
  set("from", filter.from ? (kstDayStart(filter.from) ?? undefined) : undefined);
  set("to", filter.to ? (kstDayStart(filter.to, 1) ?? undefined) : undefined);
  for (const [name, value] of Object.entries(extra)) query.set(name, value);
  return query;
}

/** The chain's sources, as the screen names them. */
export const SOURCE_LABELS: Record<string, string> = {
  dawneer: "더니어",
  audit: "감사 기록",
  github: "GitHub PR",
  merge_queue: "merge queue",
  deploy: "자동 배포",
  host_command: "호스트 명령",
  repo_settings: "저장소 설정",
  identity: "신원",
};

export const RESULT_LABELS: Record<string, string> = {
  requested: "요청",
  succeeded: "성공",
  failed: "실패",
  refused: "거부",
  expired: "만료",
};

export const ACTOR_KIND_LABELS: Record<string, string> = {
  staff: "직원",
  service: "서비스",
  system: "시스템",
  unlinked: "연결 안 된 계정",
};

/** Who acted, in a few characters: a staff account's short id, else the source's own id. */
export function actorText(event: Pick<AuditEvent, "actor_kind" | "actor_staff_id" | "actor_external">): string {
  if (event.actor_staff_id) return `직원 ${event.actor_staff_id.slice(0, 8)}`;
  if (event.actor_external) return event.actor_external;
  return ACTOR_KIND_LABELS[event.actor_kind] ?? event.actor_kind;
}

/** One key that differs between before and after. */
export interface ChangedKey {
  key: string;
  before: string | null;
  after: string | null;
}

function shown(value: unknown): string | null {
  if (value === undefined || value === null) return null;
  return typeof value === "string" ? value : JSON.stringify(value);
}

/** Every key whose value differs between `before` and `after`, in key order. */
export function changedKeys(
  before: Record<string, unknown> | null | undefined,
  after: Record<string, unknown> | null | undefined,
): ChangedKey[] {
  const keys = new Set([...Object.keys(before ?? {}), ...Object.keys(after ?? {})]);
  return [...keys]
    .sort()
    .map((key) => ({ key, before: shown(before?.[key]), after: shown(after?.[key]) }))
    .filter((change) => change.before !== change.after);
}

/** Where an applied-via reference opens, when the screen can link it. */
export function appliedViaHref(kind: string | null | undefined, ref: string | null | undefined): string | null {
  if (!kind || !ref) return null;
  if (kind === "pull_request" && /^\d+$/.test(ref)) {
    return `https://github.com/perfectory-inc/perfectory-public/pull/${ref}`;
  }
  if (kind === "deploy" && /^[0-9a-f]{7,40}$/.test(ref)) {
    return `https://github.com/perfectory-inc/perfectory-public/commit/${ref}`;
  }
  return null;
}
