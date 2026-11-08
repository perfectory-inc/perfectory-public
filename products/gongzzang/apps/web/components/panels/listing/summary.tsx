"use client";
import { Badge } from "@gongzzang/ui";
import { MEDIA_QUERIES_MAX } from "@gongzzang/ui/tokens.js";
import { useTranslations } from "next-intl";
import { useState } from "react";
import { PhotoTile } from "@/components/media/photo-grid";
import type { ListingDetail } from "@/lib/listings/api";
import { formatAreaPyeong, formatPriceKrw } from "@/lib/listings/format";
import { listingPhotoImageSrc, listingPhotoStore } from "@/lib/listings/photos";
import type { PanelStackEntry } from "@/lib/panel/types";

/** The cover fills the panel: the whole width of a phone, 600px on a desktop panel. */
const COVER_SIZES = `${MEDIA_QUERIES_MAX.xl} 100vw, 600px`;

export function ListingSummaryCard({
  entry,
  data,
}: {
  entry: Extract<PanelStackEntry, { kind: "listing" }>;
  data: ListingDetail;
}) {
  const t = useTranslations("panels.listing.summary");
  const [imageBroken, setImageBroken] = useState(false);
  const cover = data.photos?.[0];

  return (
    <div className="flex flex-col gap-4 p-6">
      {cover && !imageBroken && (
        <div className="relative aspect-[4/3] w-full overflow-hidden rounded-md bg-[var(--color-surface-cream-strong)]">
          {cover.content_sha256 && cover.media_format ? (
            // The display ladder made once from the stored original (root ADR-0427).
            <PhotoTile
              store={listingPhotoStore(entry.id, cover)}
              photo={{ sha256: cover.content_sha256, media_format: cover.media_format }}
              alt={data.title}
              viewOriginal={t("viewOriginal")}
              sizes={COVER_SIZES}
            />
          ) : (
            // An id-key photo the backfill has not mapped yet: its file, through the same route.
            // biome-ignore lint/performance/noImgElement: a short-lived signed redirect; the Next optimizer would cache a private photo server-side.
            <img
              src={listingPhotoImageSrc(entry.id, cover)}
              alt={data.title}
              loading="lazy"
              decoding="async"
              className="h-full w-full object-cover"
              onError={() => setImageBroken(true)}
            />
          )}
        </div>
      )}
      {cover && imageBroken && (
        <div className="aspect-[4/3] w-full overflow-hidden rounded-md bg-[var(--color-surface-cream-strong)] p-4 text-[length:var(--text-caption)] text-[var(--color-muted)]">
          {cover.caption ?? t("photoUnavailable")}
        </div>
      )}
      <header className="flex items-center gap-2">
        <Badge>{t(`type.${data.listing_type}` as never)}</Badge>
        <Badge variant="outline">{t(`transaction.${data.transaction_type}` as never)}</Badge>
      </header>
      <h2 className="text-[length:var(--text-title-lg)] font-semibold text-[var(--color-ink)]">
        {data.title}
      </h2>
      <dl className="grid grid-cols-2 gap-y-2 text-[length:var(--text-body-sm)]">
        <dt className="text-[var(--color-muted)]">{t("area")}</dt>
        <dd>{formatAreaPyeong(data.area_m2)}</dd>
        <dt className="text-[var(--color-muted)]">{t("price")}</dt>
        <dd>{formatPriceKrw(data.price_krw)}</dd>
        <dt className="text-[var(--color-muted)]">PNU</dt>
        <dd className="font-mono">{data.parcel_pnu}</dd>
      </dl>
      <p className="whitespace-pre-wrap text-[length:var(--text-body-sm)] text-[var(--color-muted)]">
        {data.description}
      </p>
    </div>
  );
}
