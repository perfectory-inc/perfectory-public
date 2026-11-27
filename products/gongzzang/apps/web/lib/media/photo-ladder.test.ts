import { describe, expect, it } from "vitest";
import { mediaFileSrc, renditionSrc, renditionSrcSet } from "./photo-ladder";

const sha = "a".repeat(64);

describe("a media store names a file and its ladder one way", () => {
  it("joins the base and the prefix whatever their slashes", () => {
    for (const store of [
      { base: "https://edge.example", prefix: "media" },
      { base: "https://edge.example/", prefix: "/media/" },
    ]) {
      expect(mediaFileSrc(store, { sha256: sha, media_format: "jpg" })).toBe(
        `https://edge.example/media/display/sha256/${sha}.jpg`,
      );
      expect(renditionSrc(store, { sha256: sha })).toBe(
        `https://edge.example/media/rendition/ladder=1/${sha}/400.webp`,
      );
    }
  });

  it("lists every rung of one format, smallest first", () => {
    const store = { base: "https://media.example", prefix: "listing/media" };
    expect(renditionSrcSet(store, { sha256: sha }, "avif")).toBe(
      [400, 960, 1920]
        .map((w) => `https://media.example/listing/media/rendition/ladder=1/${sha}/${w}.avif ${w}w`)
        .join(", "),
    );
  });
});
