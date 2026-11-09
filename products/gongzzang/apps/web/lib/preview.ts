import { ROUTES } from "@/lib/routes";

/**
 * The temporary read-only preview of the map and panels (root ADR-0249) is built with
 * `NEXT_PUBLIC_GONGZZANG_READ_ONLY_PREVIEW=1`. Next inlines `NEXT_PUBLIC_*` at build time, so a
 * build without it compiles this to `false` and carries no way around the login.
 */
export function isReadOnlyPreview(): boolean {
  return process.env.NEXT_PUBLIC_GONGZZANG_READ_ONLY_PREVIEW === "1";
}

/**
 * The preview has no identity, no session store and no Rust API behind it, so it serves the pages
 * whose data comes straight from the public edges — the map, and the industrial-complex list, read
 * from the parcel edge's index (root ADR-0456) — and answers 404 for everything else.
 */
export function isReadOnlyPreviewPage(pathname: string): boolean {
  return pathname === ROUTES.listings.index || pathname === ROUTES.complexes.index;
}
