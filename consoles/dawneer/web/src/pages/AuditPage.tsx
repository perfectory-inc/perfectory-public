// 감사 기록 (root ADR-0569 §7): every change to the platform, one hash-chained row each, by account, by
// object and by time; a row opens to what changed, why, how it was applied and the rest of its change.
// The chain is read only here: it is written by the actions it records, and an export is one of them.
import { useInfiniteQuery, useQuery } from "@tanstack/react-query";
import { useState } from "react";

import type { AuditEvent, DawneerClient } from "../api/client";
import {
  RESULT_LABELS,
  SOURCE_LABELS,
  actorText,
  appliedViaHref,
  auditQuery,
  changedKeys,
  type AuditFilter,
} from "../lib/audit";

const PAGE_SIZE = 100;
const TH = "px-3 py-2 text-left text-xs font-semibold text-slate-600";
const TD = "px-3 py-2 align-top text-sm";

const KST = new Intl.DateTimeFormat("ko-KR", {
  timeZone: "Asia/Seoul",
  year: "numeric",
  month: "2-digit",
  day: "2-digit",
  hour: "2-digit",
  minute: "2-digit",
  second: "2-digit",
  hour12: false,
});

function when(instant: string): string {
  return KST.format(new Date(instant));
}

function ResultBadge({ result }: { result: string }) {
  const tone =
    result === "succeeded"
      ? "bg-emerald-50 text-emerald-800"
      : result === "failed" || result === "refused"
        ? "bg-red-50 text-red-800"
        : "bg-slate-100 text-slate-700";
  return <span className={`rounded px-1.5 py-0.5 text-xs ${tone}`}>{RESULT_LABELS[result] ?? result}</span>;
}

/** The other events of the same change, oldest first: the request, its switch, its deploy, ... */
function ChangeChain({ client, changeId, seq }: { client: DawneerClient; changeId: string; seq: number }) {
  const chain = useQuery({
    queryKey: ["audit-change", changeId],
    queryFn: () => client.auditEvents(auditQuery({ changeId }, { limit: "50" })),
  });
  if (chain.isPending) return <p className="text-xs text-slate-500">같은 변경을 찾는 중…</p>;
  if (chain.isError) return <p className="text-xs text-red-700">같은 변경을 읽지 못했어요: {chain.error.message}</p>;
  const events = [...chain.data.events].reverse();
  return (
    <ol className="mt-1 space-y-0.5 text-xs">
      {events.map((event) => (
        <li key={event.seq} className={event.seq === seq ? "font-semibold" : ""}>
          #{event.seq} {when(event.occurred_at)} · {SOURCE_LABELS[event.source] ?? event.source} · {event.action} ·{" "}
          {RESULT_LABELS[event.result] ?? event.result}
        </li>
      ))}
    </ol>
  );
}

function EventDetail({ client, event }: { client: DawneerClient; event: AuditEvent }) {
  const changes = changedKeys(
    event.before as Record<string, unknown> | null | undefined,
    event.after as Record<string, unknown> | null | undefined,
  );
  const via = appliedViaHref(event.applied_via_kind, event.applied_via_ref);
  return (
    <div className="grid gap-3 bg-slate-50 px-4 py-3 text-sm md:grid-cols-2">
      <div>
        <h3 className="text-xs font-semibold text-slate-600">바뀐 값</h3>
        {changes.length === 0 ? (
          <p className="text-xs text-slate-500">전·후 값이 없는 사건입니다.</p>
        ) : (
          <table className="mt-1 text-xs">
            <thead>
              <tr>
                <th className="pr-3 text-left font-medium text-slate-500">키</th>
                <th className="pr-3 text-left font-medium text-slate-500">전</th>
                <th className="text-left font-medium text-slate-500">후</th>
              </tr>
            </thead>
            <tbody>
              {changes.map((change) => (
                <tr key={change.key}>
                  <td className="pr-3 font-mono">{change.key}</td>
                  <td className="pr-3 font-mono text-red-700">{change.before ?? "—"}</td>
                  <td className="font-mono text-emerald-800">{change.after ?? "—"}</td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
        {event.error ? <p className="mt-2 text-xs text-red-700">오류: {event.error}</p> : null}
      </div>
      <div className="space-y-1 text-xs">
        <p>
          적용:{" "}
          {event.applied_via_kind ? (
            via ? (
              <a className="text-sky-700 underline" href={via} target="_blank" rel="noreferrer">
                {event.applied_via_kind} {event.applied_via_ref}
              </a>
            ) : (
              `${event.applied_via_kind} ${event.applied_via_ref ?? ""}`
            )
          ) : (
            "—"
          )}
        </p>
        <p>
          주체: {actorText(event)} {event.actor_verified ? "(원천이 확인)" : "(확인되지 않음)"}
          {event.actor_external && event.actor_staff_id ? ` · ${event.actor_external}` : ""}
          {event.on_behalf_of ? ` · 대리 ${event.on_behalf_of}` : ""}
        </p>
        <p>
          원천 id: <span className="font-mono">{event.source_event_id}</span> · 기록 {when(event.recorded_at)}
        </p>
        <p className="break-all font-mono text-slate-500">row_hash {event.row_hash}</p>
        {event.change_id ? (
          <div>
            <h3 className="font-semibold text-slate-600">같은 변경</h3>
            <ChangeChain client={client} changeId={event.change_id} seq={event.seq} />
          </div>
        ) : null}
      </div>
    </div>
  );
}

function ImportStatus({ client }: { client: DawneerClient }) {
  const status = useQuery({ queryKey: ["audit-import-status"], queryFn: () => client.auditImportStatus() });
  if (!status.data) return null;
  const digest = status.data.latest_digest;
  return (
    <p className="text-xs text-slate-500">
      가져오기:{" "}
      {status.data.sources.length === 0
        ? "아직 없음"
        : status.data.sources
            .map((source) => `${SOURCE_LABELS[source.source] ?? source.source} ${source.state} (${when(source.advanced_at)})`)
            .join(" · ")}
      {" · "}
      증인:{" "}
      {digest
        ? `#${digest.from_seq}–#${digest.to_seq} ${digest.rekor_log_index === null || digest.rekor_log_index === undefined ? "Rekor 대기" : `Rekor ${digest.rekor_log_index}`}`
        : "아직 없음"}
    </p>
  );
}

const FIELD = "rounded border border-slate-300 px-2 py-1 text-sm";

export function AuditPage({
  client,
  actor,
  objectType,
  objectId,
}: {
  client: DawneerClient;
  actor: string | null;
  objectType: string | null;
  objectId: string | null;
}) {
  const [draft, setDraft] = useState<AuditFilter>({
    ...(actor ? { actor } : {}),
    ...(objectType ? { objectType } : {}),
    ...(objectId ? { objectId } : {}),
  });
  const [filter, setFilter] = useState<AuditFilter>(draft);
  const [open, setOpen] = useState<number | null>(null);
  const [notice, setNotice] = useState<string | null>(null);

  const events = useInfiniteQuery({
    queryKey: ["audit-events", filter],
    initialPageParam: null as number | null,
    queryFn: ({ pageParam }) =>
      client.auditEvents(
        auditQuery(filter, {
          limit: String(PAGE_SIZE),
          ...(pageParam === null ? {} : { before_seq: String(pageParam) }),
        }),
      ),
    getNextPageParam: (last) => last.next_before_seq ?? null,
  });

  const download = async (format: "jsonl" | "csv") => {
    try {
      const blob = await client.exportAudit(auditQuery(filter, { format }));
      const url = URL.createObjectURL(blob);
      const link = document.createElement("a");
      link.href = url;
      link.download = `audit-${new Date().toISOString().slice(0, 10)}.${format}`;
      link.click();
      URL.revokeObjectURL(url);
      setNotice("내보냈어요. 내보낸 것도 감사 기록에 남습니다.");
    } catch (error) {
      setNotice(`내보내지 못했어요: ${error instanceof Error ? error.message : String(error)}`);
    }
  };

  const set = (key: keyof AuditFilter) => (value: string) => setDraft((current) => ({ ...current, [key]: value }));
  const rows = events.data?.pages.flatMap((page) => page.events) ?? [];

  return (
    <section className="space-y-4">
      <div>
        <h1 className="text-xl font-semibold">감사 기록</h1>
        <p className="text-sm text-slate-500">
          플랫폼의 모든 변경을 직원 계정별로 한 줄씩 남깁니다. 줄은 고치거나 지울 수 없고, 앞 줄의 해시로 이어집니다.
        </p>
        <ImportStatus client={client} />
      </div>
      <form
        className="flex flex-wrap items-end gap-2"
        onSubmit={(event) => {
          event.preventDefault();
          setOpen(null);
          setFilter(draft);
        }}
      >
        <label className="flex items-center gap-1 text-sm">
          <input
            type="checkbox"
            checked={draft.actor === "me"}
            onChange={(event) => set("actor")(event.target.checked ? "me" : "")}
          />
          내 기록만
        </label>
        <input
          aria-label="직원 계정 id"
          className={FIELD}
          placeholder="직원 계정 id"
          value={draft.actor === "me" ? "" : (draft.actor ?? "")}
          disabled={draft.actor === "me"}
          onChange={(event) => set("actor")(event.target.value)}
        />
        <input
          aria-label="외부 계정"
          className={FIELD}
          placeholder="외부 계정(github:…, unix:…)"
          value={draft.actorExternal ?? ""}
          onChange={(event) => set("actorExternal")(event.target.value)}
        />
        <input
          aria-label="대상 종류"
          className={FIELD}
          placeholder="대상 종류(job, lane, …)"
          value={draft.objectType ?? ""}
          onChange={(event) => set("objectType")(event.target.value)}
        />
        <input
          aria-label="대상 id 앞부분"
          className={FIELD}
          placeholder="대상 id 앞부분"
          value={draft.objectId ?? ""}
          onChange={(event) => set("objectId")(event.target.value)}
        />
        <select aria-label="원천" className={FIELD} value={draft.source ?? ""} onChange={(event) => set("source")(event.target.value)}>
          <option value="">모든 원천</option>
          {Object.entries(SOURCE_LABELS).map(([value, label]) => (
            <option key={value} value={value}>
              {label}
            </option>
          ))}
        </select>
        <input
          aria-label="동작"
          className={FIELD}
          placeholder="동작(operator_action.run_now)"
          value={draft.action ?? ""}
          onChange={(event) => set("action")(event.target.value)}
        />
        <select aria-label="결과" className={FIELD} value={draft.result ?? ""} onChange={(event) => set("result")(event.target.value)}>
          <option value="">모든 결과</option>
          {Object.entries(RESULT_LABELS).map(([value, label]) => (
            <option key={value} value={value}>
              {label}
            </option>
          ))}
        </select>
        <input aria-label="시작일" type="date" className={FIELD} value={draft.from ?? ""} onChange={(event) => set("from")(event.target.value)} />
        <input aria-label="종료일" type="date" className={FIELD} value={draft.to ?? ""} onChange={(event) => set("to")(event.target.value)} />
        <button type="submit" className="rounded bg-slate-900 px-3 py-1 text-sm text-white">
          보기
        </button>
        <button type="button" onClick={() => void download("jsonl")} className="rounded border border-slate-300 px-3 py-1 text-sm hover:bg-slate-100">
          JSONL 내보내기
        </button>
        <button type="button" onClick={() => void download("csv")} className="rounded border border-slate-300 px-3 py-1 text-sm hover:bg-slate-100">
          CSV 내보내기
        </button>
      </form>
      {notice ? <p className="text-sm text-slate-600">{notice}</p> : null}
      {events.isPending ? (
        <p className="text-slate-500">불러오는 중…</p>
      ) : events.isError ? (
        <p className="text-red-700">감사 기록을 읽지 못했어요: {events.error.message}</p>
      ) : rows.length === 0 ? (
        <p className="text-slate-500">이 조건의 기록이 없어요.</p>
      ) : (
        <div className="overflow-x-auto rounded border border-slate-200 bg-white">
          <table className="min-w-full">
            <thead className="bg-slate-50">
              <tr>
                <th className={TH}>#</th>
                <th className={TH}>시각(KST)</th>
                <th className={TH}>원천</th>
                <th className={TH}>동작</th>
                <th className={TH}>주체</th>
                <th className={TH}>대상</th>
                <th className={TH}>결과</th>
                <th className={TH}>이유</th>
              </tr>
            </thead>
            <tbody>
              {rows.map((event) => (
                <AuditRow
                  key={event.seq}
                  client={client}
                  event={event}
                  open={open === event.seq}
                  onToggle={() => setOpen(open === event.seq ? null : event.seq)}
                />
              ))}
            </tbody>
          </table>
        </div>
      )}
      {events.hasNextPage ? (
        <button
          type="button"
          disabled={events.isFetchingNextPage}
          onClick={() => void events.fetchNextPage()}
          className="rounded border border-slate-300 px-3 py-1 text-sm hover:bg-slate-100 disabled:opacity-50"
        >
          더 보기
        </button>
      ) : null}
    </section>
  );
}

function AuditRow({
  client,
  event,
  open,
  onToggle,
}: {
  client: DawneerClient;
  event: AuditEvent;
  open: boolean;
  onToggle: () => void;
}) {
  return (
    <>
      <tr className="cursor-pointer border-t border-slate-100 hover:bg-slate-50" onClick={onToggle}>
        <td className={`${TD} font-mono text-xs text-slate-500`}>{event.seq}</td>
        <td className={`${TD} whitespace-nowrap`}>{when(event.occurred_at)}</td>
        <td className={TD}>{SOURCE_LABELS[event.source] ?? event.source}</td>
        <td className={`${TD} font-mono text-xs`}>{event.action}</td>
        <td className={TD}>
          {event.actor_staff_id ? (
            <a
              className="text-sky-700 hover:underline"
              href={`#/audit/actor/${event.actor_staff_id}`}
              onClick={(click) => click.stopPropagation()}
            >
              {actorText(event)}
            </a>
          ) : (
            actorText(event)
          )}
        </td>
        <td className={TD}>
          <a
            className="text-sky-700 hover:underline"
            href={`#/audit/object/${event.object_type}/${encodeURIComponent(event.object_id)}`}
            onClick={(click) => click.stopPropagation()}
          >
            {event.object_type} {event.object_id}
          </a>
        </td>
        <td className={TD}>
          <ResultBadge result={event.result} />
        </td>
        <td className={`${TD} max-w-xs truncate`}>{event.reason ?? "—"}</td>
      </tr>
      {open ? (
        <tr>
          <td colSpan={8}>
            <EventDetail client={client} event={event} />
          </td>
        </tr>
      ) : null}
    </>
  );
}
