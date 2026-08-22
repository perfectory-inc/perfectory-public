// apps/web/lib/api/complexes.ts
import { z } from "zod";
import { apiProxyClient } from "@/lib/api/api-proxy-client.generated";

/**
 * The industrial-complex summary the panel is allowed to draw.
 *
 * Zod's default object mode strips keys the schema does not name, so a column the API grows
 * tomorrow reaches no card until it is declared here. That is deliberate and is the reason the
 * schema is not `.passthrough()`: a card can only render what this file admits, and `ComplexInfo`
 * is derived from the schema rather than written by hand, so a card reaching for an undeclared
 * column fails to type-check.
 *
 * `.nullish()` rather than `.optional()` throughout: the API omits an absent column entirely
 * (`skip_serializing_if = "Option::is_none"`), and accepting `null` too means one branch in the
 * card handles both spellings of "the source did not state this".
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
  area_m2: z.number().int(),
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

export async function fetchComplex(
  lakehouseComplexId: string,
  signal?: AbortSignal,
): Promise<ComplexInfo> {
  const json = await apiProxyClient.complexRead.getJson<unknown>(
    { lakehouse_complex_id: lakehouseComplexId },
    { signal },
  );
  return ComplexInfoSchema.parse(json);
}
