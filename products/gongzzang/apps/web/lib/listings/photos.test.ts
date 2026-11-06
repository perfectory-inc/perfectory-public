import { describe, expect, it } from "vitest";
import { mediaFileSrc, renditionSrc, renditionSrcSet } from "@/lib/media/photo-ladder";
import { listingPhotoImageSrc, listingPhotoStore } from "./photos";

describe("listing photo urls", () => {
  it("uses the authenticated proxy path with photo_id instead of r2_key", () => {
    const src = listingPhotoImageSrc("lst_123", {
      photo_id: "lph_456",
      r2_key: "listings/lst_123/lph_456.jpg",
    });

    expect(src).toBe("/api/proxy/listings/lst_123/photos/lph_456");
    expect(src).not.toContain("listings/lst_123/lph_456.jpg");
  });

  // Root ADR-0427: the listing photo's ladder goes through the same authorized route.
  it("names the stored file and each rung of its ladder through the photo's route", () => {
    const store = listingPhotoStore("lst_123", { photo_id: "lph_456", r2_key: "ignored" });
    const photo = { sha256: "a".repeat(64), media_format: "jpg" };
    const route = "/api/proxy/listings/lst_123/photos/lph_456";

    expect(mediaFileSrc(store, photo)).toBe(route);
    expect(renditionSrc(store, photo)).toBe(`${route}?rendition=400.webp`);
    expect(renditionSrcSet(store, photo, "avif")).toBe(
      `${route}?rendition=400.avif 400w, ${route}?rendition=960.avif 960w, ${route}?rendition=1920.avif 1920w`,
    );
  });
});
