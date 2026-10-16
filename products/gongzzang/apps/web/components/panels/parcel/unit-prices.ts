// apps/web/components/panels/parcel/unit-prices.ts
import { queryOptions } from "@tanstack/react-query";
import { fetchUnitPrices } from "@/lib/api/unit-price-edge";
import { recordPanelDataReady } from "@/lib/rum";

/**
 * The units' prices are the parcel's own document (root ADR-0190), one request per parcel. The
 * buildings view starts it alongside its own fetch and the card reads it, so both name one query.
 */
export function unitPricesQuery(pnu: string) {
  return queryOptions({
    queryKey: ["unit-prices", pnu],
    queryFn: async ({ signal }) => {
      // Started with the buildings view's own fetch (register.ts), so this runs from the click.
      const started = performance.now();
      const prices = await fetchUnitPrices(pnu, signal);
      recordPanelDataReady("parcel.unit-prices", performance.now() - started);
      return prices;
    },
    staleTime: 60 * 60_000,
  });
}
