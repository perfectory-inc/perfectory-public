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
  };
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

/** The 공장창고 row of the 법정동's 12 months, if the document carries one. */
export function industrialRegion(transaction: RealTransaction) {
  return transaction.region.datasets.find(
    (d) => d.dataset_slug === "real_transaction_industrial_trade",
  );
}
