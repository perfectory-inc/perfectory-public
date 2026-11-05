// apps/web/components/panels/parcel/photo-grid.tsx
"use client";
import { onbidMediaSrc } from "@/lib/api/public-auction";

type Photo = { sha256: string; media_format: string; title?: string | null };

/** Content-addressed photos from the shared media store on the parcel edge (root ADR-0296 §5),
 * as a thumbnail grid in the source's own order (root ADR-0070). 공매 and 경매 both show theirs here. */
export function PhotoGrid({ photos, altFallback }: { photos: readonly Photo[]; altFallback: string }) {
  if (photos.length === 0) return null;
  return (
    <ul className="mt-2 grid grid-cols-3 gap-2">
      {photos.map((photo) => (
        <li
          key={photo.sha256}
          className="relative aspect-[4/3] w-full overflow-hidden rounded-md bg-[var(--color-surface-cream-strong)]"
        >
          {/* biome-ignore lint/performance/noImgElement: the media is immutable,
              content-addressed CDN output (sha256); the Next image optimizer would add a
              server hop with no gain, so a plain img is correct here. */}
          <img
            src={onbidMediaSrc(photo)}
            alt={photo.title ?? altFallback}
            loading="lazy"
            decoding="async"
            className="h-full w-full object-cover"
          />
        </li>
      ))}
    </ul>
  );
}
