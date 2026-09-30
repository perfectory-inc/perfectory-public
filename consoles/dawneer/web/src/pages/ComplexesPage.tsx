import { useQuery } from "@tanstack/react-query";
import { useState, type ReactNode } from "react";

import type { Complex, ComplexFilter, DawneerClient } from "../api/client";
import {
  COMPLEX_KIND_LABEL,
  COMPLEX_STATUS_LABEL,
  formatArea,
  label,
  LOT_SALES_LABEL,
  SIDO_LABEL,
} from "../lib/catalogLabels";

const PAGE_SIZE = 50;

function Row({ name, children }: { name: string; children: ReactNode }) {
  return (
    <div className="grid grid-cols-[8rem_1fr] gap-3 border-b border-slate-100 py-2 text-sm">
      <dt className="text-slate-500">{name}</dt>
      <dd className="min-w-0 whitespace-pre-line break-words text-slate-800">{children ?? "—"}</dd>
    </div>
  );
}

function ComplexDetail({ client, complexId, onClose }: { client: DawneerClient; complexId: string; onClose: () => void }) {
  const complex = useQuery({ queryKey: ["complex", complexId], queryFn: () => client.getComplex(complexId) });
  if (complex.isPending) return <p className="p-5 text-slate-500">불러오는 중…</p>;
  if (complex.isError) return <p className="p-5 text-red-700">단지를 읽지 못했습니다: {complex.error.message}</p>;
  const c: Complex = complex.data;
  return (
    <div className="p-5">
      <div className="flex items-start justify-between gap-3">
        <div>
          <h2 className="text-lg font-semibold">{c.name}</h2>
          <p className="text-sm text-slate-500">
            유형 {label(COMPLEX_KIND_LABEL, c.kind)} · {c.official_complex_code}
          </p>
        </div>
        <button type="button" onClick={onClose} className="rounded border border-slate-300 px-2 py-0.5 text-sm hover:bg-slate-100">
          닫기
        </button>
      </div>
      <dl className="mt-4">
        <Row name="조성 단계">{label(COMPLEX_STATUS_LABEL, c.status)}</Row>
        <Row name="분양">{label(LOT_SALES_LABEL, c.lot_sales_status)}</Row>
        <Row name="면적">{formatArea(c.area_m2)}</Row>
        <Row name="진척률">{c.development_progress_percent ? `${c.development_progress_percent}%` : null}</Row>
        <Row name="주소">{c.address_text}</Row>
        <Row name="관리기관">{c.management_agency_name}</Row>
        <Row name="시행자">{c.developer_name}</Row>
        <Row name="지정일">{c.designated_date}</Row>
        <Row name="착공일">{c.construction_start_date}</Row>
        <Row name="준공일">{c.completion_date}</Row>
        <Row name="사업기간">{c.business_period_raw}</Row>
        <Row name="근거법">{c.designation_basis_law_raw}</Row>
        <Row name="개발방식">{c.development_method_raw}</Row>
        <Row name="유치업종">{c.invited_industries_raw}</Row>
        <Row name="개발목적">{c.development_purpose_raw}</Row>
        <Row name="Foundation id">
          <span className="font-mono text-xs">{c.id}</span>
        </Row>
        <Row name="레이크하우스 id">
          {c.lakehouse_complex_id ? <span className="font-mono text-xs">{c.lakehouse_complex_id}</span> : null}
        </Row>
        <Row name="마지막 변경">
          {new Date(c.updated_at).toLocaleString("ko-KR")} · 판 {c.version}
        </Row>
      </dl>
    </div>
  );
}

/** The canonical industrial complexes, searchable, with one complex's full record beside the list. */
export function ComplexesPage({ client }: { client: DawneerClient }) {
  const [q, setQ] = useState("");
  const [sidoCode, setSidoCode] = useState("");
  const [status, setStatus] = useState("");
  const [sort, setSort] = useState<NonNullable<ComplexFilter["sort"]>>("name_asc");
  const [page, setPage] = useState(0);
  const [selected, setSelected] = useState<string | null>(null);

  const filter: ComplexFilter = { q: q.trim(), sidoCode, status, sort, page, size: PAGE_SIZE };
  const list = useQuery({ queryKey: ["complexes", filter], queryFn: () => client.listComplexes(filter) });
  const reset = () => setPage(0);
  const select = "rounded border border-slate-300 bg-white px-2 py-1";

  return (
    <section>
      <div className="flex flex-wrap items-end gap-4">
        <h1 className="mr-auto text-xl font-semibold">산업단지</h1>
        <label className="text-sm">
          <span className="block text-slate-500">이름 또는 단지코드</span>
          <input
            className="w-56 rounded border border-slate-300 px-2 py-1"
            value={q}
            onChange={(e) => {
              setQ(e.target.value);
              reset();
            }}
          />
        </label>
        <label className="text-sm">
          <span className="block text-slate-500">시도</span>
          <select
            className={select}
            value={sidoCode}
            onChange={(e) => {
              setSidoCode(e.target.value);
              reset();
            }}
          >
            <option value="">전체</option>
            {Object.entries(SIDO_LABEL).map(([code, name]) => (
              <option key={code} value={code}>
                {name}
              </option>
            ))}
          </select>
        </label>
        <label className="text-sm">
          <span className="block text-slate-500">조성 단계</span>
          <select
            className={select}
            value={status}
            onChange={(e) => {
              setStatus(e.target.value);
              reset();
            }}
          >
            <option value="">전체</option>
            {Object.entries(COMPLEX_STATUS_LABEL).map(([code, name]) => (
              <option key={code} value={code}>
                {name}
              </option>
            ))}
          </select>
        </label>
        <label className="text-sm">
          <span className="block text-slate-500">정렬</span>
          <select
            className={select}
            value={sort}
            onChange={(e) => {
              setSort(e.target.value as NonNullable<ComplexFilter["sort"]>);
              reset();
            }}
          >
            <option value="name_asc">이름순</option>
            <option value="area_desc">면적 큰 순</option>
            <option value="official_complex_code_asc">단지코드순</option>
          </select>
        </label>
      </div>

      <div className="mt-4 grid gap-4 xl:grid-cols-[1fr_28rem]">
        <div className="overflow-x-auto rounded-lg border border-slate-200 bg-white">
          {list.isPending ? (
            <p className="p-5 text-slate-500">불러오는 중…</p>
          ) : list.isError ? (
            <p className="p-5 text-red-700">목록을 읽지 못했습니다: {list.error.message}</p>
          ) : list.data.complexes.length === 0 ? (
            <p className="p-5 text-slate-500">조건에 맞는 단지가 없습니다.</p>
          ) : (
            <table className="w-full text-sm">
              <thead className="bg-slate-50 text-left text-slate-500">
                <tr>
                  <th className="px-3 py-2 font-normal">단지</th>
                  <th className="px-3 py-2 font-normal">유형</th>
                  <th className="px-3 py-2 font-normal">시도</th>
                  <th className="px-3 py-2 font-normal">조성 단계</th>
                  <th className="px-3 py-2 text-right font-normal">면적(㎡)</th>
                </tr>
              </thead>
              <tbody>
                {list.data.complexes.map((c) => (
                  <tr
                    key={c.id}
                    onClick={() => setSelected(c.id)}
                    className={`cursor-pointer border-t border-slate-100 hover:bg-slate-50 ${selected === c.id ? "bg-sky-50" : ""}`}
                  >
                    <td className="px-3 py-2">
                      <button type="button" className="text-left font-medium hover:underline" onClick={() => setSelected(c.id)}>
                        {c.name}
                      </button>
                      <span className="ml-2 font-mono text-xs text-slate-400">{c.official_complex_code}</span>
                    </td>
                    <td className="px-3 py-2">{label(COMPLEX_KIND_LABEL, c.kind)}</td>
                    <td className="px-3 py-2">{label(SIDO_LABEL, c.sido_code)}</td>
                    <td className="px-3 py-2">{label(COMPLEX_STATUS_LABEL, c.status)}</td>
                    <td className="px-3 py-2 text-right tabular-nums">{c.area_m2.toLocaleString("ko-KR")}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          )}
          {list.data ? (
            <div className="flex items-center justify-between border-t border-slate-100 px-3 py-2 text-sm text-slate-600">
              <span>
                전체 {list.data.total.toLocaleString("ko-KR")}개 중 {page * PAGE_SIZE + 1}–
                {page * PAGE_SIZE + list.data.complexes.length}
              </span>
              <span className="flex gap-2">
                <button
                  type="button"
                  disabled={page === 0}
                  onClick={() => setPage(page - 1)}
                  className="rounded border border-slate-300 px-2 py-0.5 disabled:opacity-40"
                >
                  이전
                </button>
                <button
                  type="button"
                  disabled={!list.data.has_next}
                  onClick={() => setPage(page + 1)}
                  className="rounded border border-slate-300 px-2 py-0.5 disabled:opacity-40"
                >
                  다음
                </button>
              </span>
            </div>
          ) : null}
        </div>
        <aside className="self-start rounded-lg border border-slate-200 bg-white xl:sticky xl:top-4">
          {selected ? (
            <ComplexDetail client={client} complexId={selected} onClose={() => setSelected(null)} />
          ) : (
            <p className="p-5 text-sm text-slate-500">단지를 누르면 전체 기록이 여기 나옵니다.</p>
          )}
        </aside>
      </div>
    </section>
  );
}
