// apps/web/components/panels/parcel/unit-prices.ts
import { queryOptions } from "@tanstack/react-query";
import { fetchUnitPrices } from "@/lib/api/unit-price-edge";

/** The key of the units no building holds (root ADR-0204): the nil UUID. */
export const NO_BUILDING = "00000000-0000-0000-0000-000000000000";

/**
 * One building's unit prices (root ADR-0190, ADR-0204): one request per building with units, all
 * started together once the buildings arrive, since their ids name the documents.
 */
export function unitPricesQuery(pnu: string, buildingId: string) {
  return queryOptions({
    queryKey: ["unit-prices", pnu, buildingId],
    queryFn: ({ signal }) => fetchUnitPrices(pnu, buildingId, signal),
    staleTime: 60 * 60_000,
  });
}
