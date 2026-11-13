import type { z } from "zod";
import { env } from "@/lib/env";
import { PnuSchema } from "./building-edge";
import { ParcelRealTransactionByPnuSchema as Generated } from "./real-transaction.generated";

// A parcel's 국토교통부 실거래 are their own by-PNU document on the parcel edge (`parcel-real-transaction`
// lane, root ADR-0350, ADR-0371): the parcel's own deals and, for a 공장용지·창고용지 parcel, the newest
// 공장창고 deals of its 법정동. Amounts are the provider's 만원; a deal linked by inference has an
// `inferred_*` grade and the panel says 추정 (root ADR-0351). The document's shape is the schema
// registry's (root ADR-0336), narrowed to what the panel reads; this file only extends it.
export const RealTransactionDocumentSchema = Generated.extend({ pnu: PnuSchema });

export type RealTransaction = z.infer<typeof RealTransactionDocumentSchema>["real_transaction"] & {
  transactionTotal: number;
  /** The other current parcels of this parcel's 본번 group, when a deal reached it by that group. */
  mainLotGroupOthers: string[];
};
export type Deal = RealTransaction["transactions"][number];
export type Comparable = RealTransaction["comparables"][number];

/** A parcel the lane has no document for (not published yet, or no deal): null, not a failure. */
export async function fetchRealTransaction(
  pnu: string,
  signal?: AbortSignal,
): Promise<RealTransaction | null> {
  PnuSchema.parse(pnu);
  const base = env.NEXT_PUBLIC_PARCEL_EDGE_BASE_URL.replace(/\/$/, "");
  const response = await fetch(`${base}/parcels/real-transaction/by-pnu/${pnu}`, {
    signal,
    headers: { accept: "application/json" },
  });
  if (response.status === 404) return null;
  if (!response.ok) throw new Error(`real transaction edge fetch failed: ${response.status}`);
  const document = RealTransactionDocumentSchema.parse(await response.json());
  if (document.pnu !== pnu)
    throw new Error("real transaction edge response PNU disagrees with request");
  return {
    ...document.real_transaction,
    transactionTotal: document.transaction_total ?? document.real_transaction.transactions.length,
    mainLotGroupOthers: document.real_transaction.main_lot_group_pnus.filter((p) => p !== pnu),
  };
}

/** The grade of a deal linked to every parcel of its 본번 group, not to one parcel (root ADR-0405). */
export const MAIN_LOT_GROUP_GRADE = "inferred_same_main_lot_group";

/**
 * The same 본번 group link for a dataset measured under the 99% bar, shown only while the owner's one
 * switch shows it (the 실거래 contract's `place_inference.below_bar`, root ADR-0483 §4).
 */
export const MAIN_LOT_GROUP_BELOW_BAR_GRADE = "inferred_same_main_lot_group_below_bar";

/** Whether the deal reached this parcel as one of its 본번 group: the screen says 추정 · 같은 본번. */
export function isMainLotGroup(grade: string | null | undefined): boolean {
  return grade === MAIN_LOT_GROUP_GRADE || grade === MAIN_LOT_GROUP_BELOW_BAR_GRADE;
}

/** Whether the link is under the bar: the screen says its measured precision (the contract's below_bar.label_ko). */
export function isBelowBar(grade: string | null | undefined): boolean {
  return grade === MAIN_LOT_GROUP_BELOW_BAR_GRADE;
}

/** A PNU's lot as a 지번: 산 flag, 본번 and 부번 (9999900000203000001 → "산 300-1"). */
export function lotOfPnu(pnu: string): string {
  const main = Number(pnu.slice(11, 15));
  const sub = Number(pnu.slice(15, 19));
  return `${pnu[10] === "2" ? "산 " : ""}${main}${sub > 0 ? `-${sub}` : ""}`;
}

/** Whether the deal reached its parcel by inference, not by the lot the provider wrote (root ADR-0351). */
export function isInferred(grade: string | null | undefined): boolean {
  return grade?.startsWith("inferred_") ?? false;
}

/** A deal of a share of the land (the provider's 지분구분 `지분`): its area is the share's (root ADR-0351 §1). */
export function isShareDeal(deal: Deal): boolean {
  return deal.share_dealing_type === "지분";
}

/** 만원 as the provider writes it, in 억·만원: 52300 → "5억 2,300만원", 8000 → "8,000만원". */
export function formatManwon(value: number): string {
  const eok = Math.floor(value / 10_000);
  const man = value % 10_000;
  const parts = [
    eok > 0 ? `${eok.toLocaleString("ko-KR")}억` : "",
    man > 0 ? `${man.toLocaleString("ko-KR")}만원` : "",
  ];
  const text = parts.filter(Boolean).join(" ");
  return text === "" ? "0원" : man > 0 ? text : `${text}원`;
}

/**
 * The datasets whose history the lane does not hold whole yet, grouped by the month each is held
 * from, oldest month first; `null` for those it holds no month of yet (root ADR-0479). A dataset the
 * document leaves out is whole, and the screen says nothing about it.
 */
export function coverageGroups(
  transaction: RealTransaction,
): { fromMonth: string | null; datasets: string[] }[] {
  const groups = new Map<string | null, string[]>();
  for (const { dataset_slug, from_month } of transaction.coverage ?? []) {
    groups.set(from_month, [...(groups.get(from_month) ?? []), dataset_slug]);
  }
  return [...groups.entries()]
    .sort(([a], [b]) => (a === null ? 1 : b === null ? -1 : a.localeCompare(b)))
    .map(([fromMonth, datasets]) => ({ fromMonth, datasets }));
}

/** The 공장창고 row of the 법정동's 12 months, if the document carries one. */
export function industrialRegion(transaction: RealTransaction) {
  return transaction.region.datasets.find(
    (d) => d.dataset_slug === "real_transaction_industrial_trade",
  );
}
