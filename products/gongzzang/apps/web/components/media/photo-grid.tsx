// apps/web/components/media/photo-grid.tsx
"use client";
import { useState } from "react";
import {
  type MediaStore,
  mediaFileSrc,
  renditionSrc,
  renditionSrcSet,
  type StoredFile,
} from "@/lib/media/photo-ladder";

/** One photo of a grid: the stored file its ladder is made from, and its caption if any. */
export type GridPhoto = StoredFile & { readonly title?: string | null };

/** A tile is a third of the panel: 160px wide on a desktop panel, a third of a phone. */
const TILE_SIZES = "(max-width: 640px) 33vw, 160px";

/**
 * Stored photos as a three-column grid in the given order (the source's own, root ADR-0070), each
 * from its rendition ladder (root ADR-0427). Every screen that shows photos — 공매, 경매, 매물 — shows
 * them here; they differ only in the store they read.
 */
export function PhotoGrid({
  store,
  photos,
  altFallback,
  viewOriginal,
}: {
  store: MediaStore;
  photos: readonly GridPhoto[];
  altFallback: string;
  viewOriginal: string;
}) {
  if (photos.length === 0) return null;
  return (
    <ul className="mt-2 grid grid-cols-3 gap-2">
      {photos.map((photo, index) => (
        <li
          // biome-ignore lint/suspicious/noArrayIndexKey: the same bytes may be shown twice; order is the identity.
          key={`${photo.sha256}-${index}`}
          className="relative aspect-[4/3] w-full overflow-hidden rounded-md bg-[var(--color-surface-cream-strong)]"
        >
          <PhotoTile
            store={store}
            photo={photo}
            alt={photo.title ?? altFallback}
            viewOriginal={viewOriginal}
          />
        </li>
      ))}
    </ul>
  );
}

/**
 * One photo from its rendition ladder: AVIF, else WebP, the browser taking the smallest width that
 * fills the tile at its pixel density, loaded lazily. The tile links to the stored file itself, so
 * the original is fetched only when asked for. Until a ladder exists (a photo newer than its
 * collector run's ladder budget) the stored file itself is shown.
 */
export function PhotoTile({
  store,
  photo,
  alt,
  viewOriginal,
}: {
  store: MediaStore;
  photo: StoredFile;
  alt: string;
  viewOriginal: string;
}) {
  const [laddered, setLaddered] = useState(true);
  const file = mediaFileSrc(store, photo);
  return (
    <a
      href={file}
      target="_blank"
      rel="noopener noreferrer"
      title={viewOriginal}
      className="block h-full w-full"
    >
      {laddered ? (
        <picture>
          <source
            type="image/avif"
            srcSet={renditionSrcSet(store, photo, "avif")}
            sizes={TILE_SIZES}
          />
          <img
            src={renditionSrc(store, photo)}
            srcSet={renditionSrcSet(store, photo, "webp")}
            sizes={TILE_SIZES}
            alt={alt}
            loading="lazy"
            decoding="async"
            onError={() => setLaddered(false)}
            className="h-full w-full object-cover"
          />
        </picture>
      ) : (
        // biome-ignore lint/performance/noImgElement: immutable content-addressed CDN media (sha256); the Next optimizer adds a hop for no gain.
        <img
          src={file}
          alt={alt}
          loading="lazy"
          decoding="async"
          className="h-full w-full object-cover"
        />
      )}
    </a>
  );
}
