// apps/web/lib/api/complexes.ts
import { z } from "zod";
import { env } from "@/lib/env";
import { IndustrialComplexIndexSchema } from "./complex-index.generated";
import { IndustrialComplexGoldProfileSchema } from "./complex-profile.generated";

/**
 * The industrial-complex index on the parcel edge (root ADR-0456): every complex's list fields and
 * the key of its current Gold profile. `industrial_complex_documents.index_object` in the foundation
 * connection contract; `edge-contract.test.ts` holds this to it. The edge answers `no-cache` with an
 * ETag, so the browser revalidates it (304 while unchanged) rather than downloading it again.
 */
export const COMPLEX_INDEX_PATH = "gold/industrial-complex/index.json";

export type ComplexIndex = z.infer<typeof IndustrialComplexIndexSchema>;

function edgeUrl(path: string): string {
  return `${env.NEXT_PUBLIC_PARCEL_EDGE_BASE_URL.replace(/\/$/, "")}/${path}`;
}

async function edgeJson(path: string, signal?: AbortSignal): Promise<unknown> {
  const response = await fetch(edgeUrl(path), { signal, headers: { accept: "application/json" } });
  if (!response.ok) throw new Error(`complex edge fetch failed: ${response.status}`);
  return response.json();
}

export async function fetchComplexIndex(signal?: AbortSignal): Promise<ComplexIndex> {
  return IndustrialComplexIndexSchema.parse(await edgeJson(COMPLEX_INDEX_PATH, signal));
}

/** A decimal text area as whole square metres, or null when the source stated none. */
export function wholeSquareMetres(value: string | null | undefined): number | null {
  if (value == null || value.trim() === "") return null;
  const area = Number(value);
  return Number.isFinite(area) ? Math.round(area) : null;
}

/**
 * The industrial-complex summary the panel is allowed to draw.
 *
 * Zod's default object mode strips keys the schema does not name, so a column the profile grows
 * tomorrow reaches no card until it is declared here. That is deliberate and is the reason the
 * schema is not `.passthrough()`: a card can only render what this file admits, and `ComplexInfo`
 * is derived from the schema rather than written by hand, so a card reaching for an undeclared
 * column fails to type-check.
 *
 * `.nullish()` rather than `.optional()` throughout: the profile carries an absent column as `null`
 * and an older one may omit it, so one branch in the card handles both spellings of "the source did
 * not state this".
 */
export const ComplexInfoSchema = z.object({
  lakehouse_complex_id: z.string(),
  official_complex_code: z.string(),
  name: z.string(),
  kind: z.string(),
  /**
   * `"unknown"` and absent are different answers. `unknown` means the source stated a lifecycle
   * this contract does not recognize; absent means it stated none.
   */
  status: z.string().nullish(),
  address_text: z.string().nullish(),
  area_m2: z.number().int().nullish(),
  management_agency_name: z.string().nullish(),
  developer_name: z.string().nullish(),
  designated_date: z.string().nullish(),
  construction_start_date: z.string().nullish(),
  completion_date: z.string().nullish(),
  /**
   * Exact decimal text, not a number: the canonical column is `numeric(5,2)` and a JSON number is a
   * double for most clients. `"0.00"` is a real answer — site works that have not started — and is
   * not the same as absent.
   */
  development_progress_percent: z.string().nullish(),
  lot_sales_status: z.string().nullish(),
  business_period_raw: z.string().nullish(),
  designation_basis_law_raw: z.string().nullish(),
  development_method_raw: z.string().nullish(),
  development_purpose_raw: z.string().nullish(),
  invited_industries_raw: z.string().nullish(),
});

export type ComplexInfo = z.infer<typeof ComplexInfoSchema>;

/**
 * One complex's summary: its index entry names its current profile, which carries the Gold row
 * (root ADR-0456). A complex the index does not list is an error, as an unknown id was before.
 */
export async function fetchComplex(
  lakehouseComplexId: string,
  signal?: AbortSignal,
): Promise<ComplexInfo> {
  const index = await fetchComplexIndex(signal);
  const entry = index.complexes.find((complex) => complex.complex_id === lakehouseComplexId);
  if (entry === undefined) throw new Error(`complex ${lakehouseComplexId} is not in the index`);
  const profile = IndustrialComplexGoldProfileSchema.parse(
    await edgeJson(entry.profile_object_key, signal),
  );
  if (profile.complex_id !== lakehouseComplexId)
    throw new Error("complex profile disagrees with the requested complex");
  const { attributes: a } = profile;
  return ComplexInfoSchema.parse({
    ...a,
    lakehouse_complex_id: a.complex_id,
    area_m2: wholeSquareMetres(a.official_area_sqm),
  });
}
