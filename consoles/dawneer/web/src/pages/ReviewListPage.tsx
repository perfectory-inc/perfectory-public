import { useQuery } from "@tanstack/react-query";
import { useState } from "react";

import type { DawneerClient, ReviewItem, ReviewKind, ReviewStatus } from "../api/client";
import { DATA_DECIDED_FILTER_LABEL, formatCode, KIND_LABEL, STATUS_LABEL } from "../lib/labels";

/** What waits for a person — parcel lineage and 법정동 pairs — in subject-code order, page by page. */
export function ReviewListPage({ client }: { client: DawneerClient }) {
  const [kind, setKind] = useState<ReviewKind | "">("");
  const [status, setStatus] = useState<ReviewStatus | "">("");
  const [prefix, setPrefix] = useState("");
  const [undecidedOnly, setUndecidedOnly] = useState(true);
  // Splits and changes awaiting data need no person (ADR-0144 §4): hidden unless asked for.
  const [showDataDecided, setShowDataDecided] = useState(false);
  const [cursors, setCursors] = useState<string[]>([]);
  const after = cursors.at(-1);

  const filter = {
    ...(kind ? { kind } : {}),
    ...(status ? { status } : {}),
    ...(/^\d{1,19}$/.test(prefix) ? { prefix } : {}),
    undecidedOnly,
    // A status picked by name is shown whatever it is.
    actionableOnly: !status && !showDataDecided,
    ...(after ? { after } : {}),
  };
  const page = useQuery({
    queryKey: ["items", filter],
    queryFn: () => client.listItems(filter),
  });
  const reset = () => setCursors([]);

  return (
    <section>
      <div className="flex flex-wrap items-end gap-4">
        <h1 className="mr-auto text-xl font-semibold">검토</h1>
        <label className="text-sm">
          <span className="block text-slate-500">종류</span>
          <select
            className="rounded border border-slate-300 bg-white px-2 py-1"
            value={kind}
            onChange={(e) => {
              setKind(e.target.value as ReviewKind | "");
              setStatus("");
              reset();
            }}
          >
            <option value="">전체</option>
            {Object.entries(KIND_LABEL).map(([code, label]) => (
              <option key={code} value={code}>
                {label}
              </option>
            ))}
          </select>
        </label>
        <label className="text-sm">
          <span className="block text-slate-500">상태</span>
          <select
            className="rounded border border-slate-300 bg-white px-2 py-1"
            value={status}
            onChange={(e) => {
              setStatus(e.target.value as ReviewStatus | "");
              reset();
            }}
          >
            <option value="">전체</option>
            {Object.entries(STATUS_LABEL).map(([code, label]) => (
              <option key={code} value={code}>
                {label}
              </option>
            ))}
          </select>
        </label>
        <label className="text-sm">
          <span className="block text-slate-500">지역 코드 앞자리</span>
          <input
            className="w-32 rounded border border-slate-300 px-2 py-1"
            inputMode="numeric"
            placeholder="예: 28"
            value={prefix}
            onChange={(e) => {
              setPrefix(e.target.value.replace(/\D/g, "").slice(0, 19));
              reset();
            }}
          />
        </label>
        <label className="flex items-center gap-2 text-sm">
          <input
            type="checkbox"
            checked={undecidedOnly}
            onChange={(e) => {
              setUndecidedOnly(e.target.checked);
              reset();
            }}
          />
          결정 안 된 것만
        </label>
        <label className="flex items-center gap-2 text-sm">
          <input
            type="checkbox"
            checked={showDataDecided}
            onChange={(e) => {
              setShowDataDecided(e.target.checked);
              reset();
            }}
          />
          {DATA_DECIDED_FILTER_LABEL}
        </label>
      </div>

      {page.isPending && <p className="mt-6 text-slate-500">불러오는 중…</p>}
      {page.isError && <p className="mt-6 text-red-700">{page.error.message}</p>}
      {page.data && (
        <>
          <ReviewTable items={page.data.items} />
          <div className="mt-4 flex gap-2">
            <button
              type="button"
              disabled={cursors.length === 0}
              onClick={() => setCursors(cursors.slice(0, -1))}
              className="rounded border border-slate-300 px-3 py-1 text-sm disabled:opacity-40"
            >
              이전
            </button>
            <button
              type="button"
              disabled={!page.data.next_after}
              onClick={() => page.data.next_after && setCursors([...cursors, page.data.next_after])}
              className="rounded border border-slate-300 px-3 py-1 text-sm disabled:opacity-40"
            >
              다음
            </button>
          </div>
        </>
      )}
    </section>
  );
}

/** One row per item, whatever its kind; the subject reads as a PNU or as a 법정동 code and name. */
export function ReviewTable({ items }: { items: ReviewItem[] }) {
  return (
    <table className="mt-6 w-full border-collapse overflow-hidden rounded-lg bg-white text-sm shadow-sm">
      <thead className="bg-slate-100 text-left text-slate-600">
        <tr>
          <th className="px-4 py-2">종류</th>
          <th className="px-4 py-2">대상</th>
          <th className="px-4 py-2">상태</th>
          <th className="px-4 py-2">후보</th>
          <th className="px-4 py-2">맡은 사람</th>
          <th className="px-4 py-2">결정</th>
        </tr>
      </thead>
      <tbody>
        {items.length === 0 && (
          <tr>
            <td colSpan={6} className="px-4 py-8 text-center text-slate-500">
              검토할 항목이 없습니다.
            </td>
          </tr>
        )}
        {items.map((item) => (
          <tr key={item.item_id} className="border-t border-slate-100 hover:bg-slate-50">
            <td className="px-4 py-2 text-slate-600">{KIND_LABEL[item.kind]}</td>
            <td className="px-4 py-2">
              <a className="font-mono text-blue-700 hover:underline" href={`#/items/${item.item_id}`}>
                {formatCode(item.kind, item.subject_code)}
              </a>
              {item.legal_dong && <span className="ml-2 text-slate-700">{item.legal_dong.name}</span>}
            </td>
            <td className="px-4 py-2">{STATUS_LABEL[item.status]}</td>
            <td className="px-4 py-2">{item.legal_dong ? item.legal_dong.candidates.length : item.candidates.length}</td>
            <td className="px-4 py-2 text-slate-500">{item.claim ? "맡음" : "—"}</td>
            <td className="px-4 py-2 text-slate-500">{item.decisions.length ? `${item.decisions.length}건` : "—"}</td>
          </tr>
        ))}
      </tbody>
    </table>
  );
}
