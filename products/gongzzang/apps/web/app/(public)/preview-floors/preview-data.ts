import { z } from "zod";

const PreviewParcelsSchema = z.array(
  z
    .object({
      pnu: z.string().regex(/^\d{19}$/),
      address: z.string().trim().min(1),
    })
    .strict(),
);

export type PreviewParcel = z.infer<typeof PreviewParcelsSchema>[number];

export function parsePreviewParcels(raw: string | undefined): ReadonlyArray<PreviewParcel> {
  if (raw == null || raw.trim() === "") return [];
  return PreviewParcelsSchema.parse(JSON.parse(raw));
}

// Concrete parcel selections are local operational inputs, not public source fixtures.
export const PREVIEW_PARCELS = parsePreviewParcels(
  process.env.GONGZZANG_PREVIEW_FLOORS_PARCELS_JSON,
);
