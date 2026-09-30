import { useQuery } from "@tanstack/react-query";
import { useState } from "react";

import type { DawneerClient, ReviewStatus } from "../api/client";
import { formatPnu, STATUS_LABEL } from "../lib/labels";

/** The parcels waiting for a person, in parcel order, page by page. */
export function ReviewListPage({ client }: { client: DawneerClient }) {
  const [status, setStatus] = useState<ReviewStatus | "">("");
  const [prefix, setPrefix] = useState("");
  const [undecidedOnly, setUndecidedOnly] = useState(true);
  const [cursors, setCursors] = useState<string[]>([]);
  const after = cursors.at(-1);

  const filter = {
    ...(status ? { status } : {}),
    ...(/^\d{1,19}$/.test(prefix) ? { prefix } : {}),
    undecidedOnly,
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
        <h1 className="mr-auto text-xl font-semibold">필지 계보 검토</h1>
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
      </div>

      {page.isPending && <p className="mt-6 text-slate-500">불러오는 중…</p>}
      {page.isError && <p className="mt-6 text-red-700">{page.error.message}</p>}
      {page.data && (
        <>
          <table className="mt-6 w-full border-collapse overflow-hidden rounded-lg bg-white text-sm shadow-sm">
            <thead className="bg-slate-100 text-left text-slate-600">
              <tr>
                <th className="px-4 py-2">필지(PNU)</th>
                <th className="px-4 py-2">상태</th>
                <th className="px-4 py-2">후보</th>
                <th className="px-4 py-2">맡은 사람</th>
                <th className="px-4 py-2">결정</th>
              </tr>
            </thead>
            <tbody>
              {page.data.items.length === 0 && (
                <tr>
                  <td colSpan={5} className="px-4 py-8 text-center text-slate-500">
                    검토할 필지가 없습니다.
                  </td>
                </tr>
              )}
              {page.data.items.map((item) => (
                <tr key={item.item_id} className="border-t border-slate-100 hover:bg-slate-50">
                  <td className="px-4 py-2 font-mono">
                    <a className="text-blue-700 hover:underline" href={`#/items/${item.item_id}`}>
                      {formatPnu(item.subject_pnu)}
                    </a>
                  </td>
                  <td className="px-4 py-2">{STATUS_LABEL[item.status]}</td>
                  <td className="px-4 py-2">{item.candidates.length}</td>
                  <td className="px-4 py-2 text-slate-500">{item.claim ? "맡음" : "—"}</td>
                  <td className="px-4 py-2 text-slate-500">{item.decisions.length ? `${item.decisions.length}건` : "—"}</td>
                </tr>
              ))}
            </tbody>
          </table>
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
