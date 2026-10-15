// apps/web/components/panels/parcel/unit-prices.ts
import { queryOptions } from "@tanstack/react-query";
import { fetchUnitPrices } from "@/lib/api/unit-price-edge";

/**
 * The units' prices are the parcel's own document (root ADR-0190), one request per parcel. The
 * buildings view starts it alongside its own fetch and the card reads it, so both name one query.
 */
export function unitPricesQuery(pnu: string) {
  return queryOptions({
    queryKey: ["unit-prices", pnu],
    queryFn: ({ signal }) => fetchUnitPrices(pnu, signal),
    staleTime: 60 * 60_000,
  });
}
