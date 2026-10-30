import type { z } from "zod";
import { env } from "@/lib/env";
import { PnuSchema } from "./building-edge";
import { ParcelOwnershipByPnuSchema as Generated } from "./ownership.generated";

// A parcel's owners are their own by-PNU document on the parcel edge (`parcel-ownership` lane,
// root ADR-0361), built from the 토지소유 file (VWorld AL_D161): the parcel's own co-owners and, for a
// collective building on it, only how many unit owners of each 소유구분. The file names no person;
// the panel shows the provider's words as they are (root ADR-0070/0078). The document's shape is the
// schema registry's (root ADR-0336), narrowed to what the panel reads; this file only extends it.
export const OwnershipDocumentSchema = Generated.extend({ pnu: PnuSchema });

export type Ownership = z.infer<typeof OwnershipDocumentSchema>["ownership"] & {
  ownerTotal: number;
};

/** A parcel the lane has no document for (not published yet, or no owner row): null, not a failure. */
export async function fetchOwnership(pnu: string, signal?: AbortSignal): Promise<Ownership | null> {
  PnuSchema.parse(pnu);
  const base = env.NEXT_PUBLIC_PARCEL_EDGE_BASE_URL.replace(/\/$/, "");
  const response = await fetch(`${base}/parcels/ownership/by-pnu/${pnu}`, {
    signal,
    headers: { accept: "application/json" },
  });
  if (response.status === 404) return null;
  if (!response.ok) throw new Error(`ownership edge fetch failed: ${response.status}`);
  const document = OwnershipDocumentSchema.parse(await response.json());
  if (document.pnu !== pnu) throw new Error("ownership edge response PNU disagrees with request");
  return {
    ...document.ownership,
    ownerTotal: document.owner_total ?? document.ownership.owners.length,
  };
}

/** Owners counted by 소유구분, most first, as `[name, count]` (a null kind is the source's silence). */
export function ownerKinds(ownership: Ownership): [string | null, number][] {
  const counts = new Map<string | null, number>();
  for (const owner of ownership.owners) {
    const kind = owner.ownership_kind_name ?? null;
    counts.set(kind, (counts.get(kind) ?? 0) + 1);
  }
  return [...counts].sort((a, b) => b[1] - a[1]);
}

/** The listed owner whose ownership changed last (by 소유권변동일자), or undefined. */
export function latestChange(ownership: Ownership): Ownership["owners"][number] | undefined {
  return ownership.owners
    .filter((owner) => owner.ownership_changed_on)
    .sort((a, b) => (b.ownership_changed_on ?? "").localeCompare(a.ownership_changed_on ?? ""))[0];
}
