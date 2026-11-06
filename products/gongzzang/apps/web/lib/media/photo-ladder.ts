// The one way the web names a stored photo and its display ladder (root ADR-0427): every photo the
// screens show — 공매, 경매 and 매물 — is stored once under its content hash in a media store
// (`<prefix>/sha256/<hex>.<ext>`, root ADR-0298) with its ladder beside it
// (`<prefix>/renditions/<version>/<hex>/<width>.<format>`). A store is where it is served from and its
// key prefix; the screens differ only in which store they read.

/** Where a media store is served from: the public base URL and the key prefix inside it. */
export type MediaStore = { readonly base: string; readonly prefix: string };

/** A stored file: its content hash and the extension it was stored under. */
export type StoredFile = { readonly sha256: string; readonly media_format: string };

/**
 * The display ladder the foundation makes once from each stored photo (root ADR-0427): every width
 * (longest side, never larger than the source) in every format, under the source's own hash. A copy
 * of `config/r2-connections.contract.json` `parcel_media.renditions`, held to it by
 * edge-contract.test.ts.
 */
export const PHOTO_RENDITIONS = {
  version: "r1",
  widths: [400, 960, 1920],
  formats: ["avif", "webp"],
} as const;

export type RenditionFormat = (typeof PHOTO_RENDITIONS.formats)[number];

function root(store: MediaStore): string {
  return `${store.base.replace(/\/$/, "")}/${store.prefix.replace(/^\/|\/$/g, "")}`;
}

/** The public URL of a stored file itself (a photo's original, a map, a PDF). */
export function mediaFileSrc(store: MediaStore, file: StoredFile): string {
  return `${root(store)}/sha256/${file.sha256}.${file.media_format}`;
}

function ladderDir(store: MediaStore, source: { sha256: string }): string {
  return `${root(store)}/renditions/${PHOTO_RENDITIONS.version}/${source.sha256}`;
}

/** A photo source's ladder in one format as a `srcset` (`<url> <width>w, …`), smallest first. */
export function renditionSrcSet(
  store: MediaStore,
  source: { sha256: string },
  format: RenditionFormat,
): string {
  const dir = ladderDir(store, source);
  return PHOTO_RENDITIONS.widths.map((width) => `${dir}/${width}.${format} ${width}w`).join(", ");
}

/** The smallest WebP of a photo source's ladder: the `src` (WebP is read by 97% of browsers, root
 * ADR-0427 §3; a browser that reads neither falls back to the stored file on error). */
export function renditionSrc(store: MediaStore, source: { sha256: string }): string {
  const [smallest] = PHOTO_RENDITIONS.widths;
  return `${ladderDir(store, source)}/${smallest}.webp`;
}
