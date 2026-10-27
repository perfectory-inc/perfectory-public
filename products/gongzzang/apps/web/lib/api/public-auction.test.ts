import { afterEach, describe, expect, it, vi } from "vitest";
import { fetchPublicAuction, onbidMediaSrc, PublicAuctionDocumentSchema } from "./public-auction";

vi.mock("@/lib/env", () => ({
  env: { NEXT_PUBLIC_PARCEL_EDGE_BASE_URL: "https://catalog.example.test" },
}));

// Synthetic only (public-repo fixture-safety): a canonical-but-unused PNU, a made-up item, no real
// onbid numbers or place names.
const pnu = "9999900000100000000";
const document = () => ({
  schema_version: "foundation-platform.parcel_public_auction_by_pnu.v1",
  pnu,
  source: { table: "gold.parcel_public_auction", iceberg_snapshot_id: "999990000000000001" },
  public_auction: {
    rounds: [
      {
        item_name: "테스트 물건 1",
        appraisal_amount_krw: 100000000,
        lowest_bid_price_krw: 70000000,
        bid_begins_at: "2026-01-02T01:00:00.000Z",
        bid_ends_at: "2026-01-05T08:00:00.000Z",
        pbct_nsq: "001",
        land_area_m2: 330,
        building_area_m2: 0,
        share_item_yn: "N",
        usg_scls_ctgr_name: null,
        usg_mcls_ctgr_name: null,
        usg_lcls_ctgr_name: null,
        photos: [],
      },
    ],
  },
});

// Two synthetic 64-hex content addresses (not real onbid media).
const photoSha = "a".repeat(64);
const appraisalSha = "b".repeat(64);

afterEach(() => vi.unstubAllGlobals());

describe("public auction edge", () => {
  it("requests the lane directly and returns its rounds", async () => {
    const fetcher = vi.fn().mockResolvedValue(Response.json(document()));
    vi.stubGlobal("fetch", fetcher);
    const signal = new AbortController().signal;
    const rounds = await fetchPublicAuction(pnu, signal);
    expect(rounds).toEqual(document().public_auction.rounds);
    expect(fetcher).toHaveBeenCalledWith(
      `https://catalog.example.test/parcels/public-auction/by-pnu/${pnu}`,
      { signal, headers: { accept: "application/json" } },
    );
  });

  it("reads 404 as no auctions and propagates outages", async () => {
    const fetcher = vi
      .fn()
      .mockResolvedValueOnce(new Response(null, { status: 404 }))
      .mockResolvedValueOnce(new Response(null, { status: 503 }));
    vi.stubGlobal("fetch", fetcher);
    expect(await fetchPublicAuction(pnu)).toEqual([]);
    await expect(fetchPublicAuction(pnu)).rejects.toThrow("503");
  });

  it("refuses a noncanonical PNU before fetching and a response for another parcel", async () => {
    const fetcher = vi
      .fn()
      .mockResolvedValue(Response.json({ ...document(), pnu: "9999900000200000000" }));
    vi.stubGlobal("fetch", fetcher);
    await expect(fetchPublicAuction("../manifest.json")).rejects.toThrow();
    expect(fetcher).not.toHaveBeenCalled();
    await expect(fetchPublicAuction(pnu)).rejects.toThrow("PNU");
  });
});

describe("public auction media", () => {
  const photo = {
    sha256: photoSha,
    media_kind: "photo",
    sequence: 1,
    title: "정면",
    media_format: "jpg",
    size_bytes: 1234,
  };
  const appraisal = {
    sha256: appraisalSha,
    media_kind: "appraisal",
    sequence: null,
    title: null,
    media_format: "pdf",
    size_bytes: 9999,
  };
  // The schema takes raw JSON: build rounds and let it validate and apply the photos default.
  const parse = (rounds: unknown[]) =>
    PublicAuctionDocumentSchema.parse({ ...document(), public_auction: { rounds } });

  it("parses a round's photos in source order and defaults a round with no photos field to []", () => {
    const parsed = parse([
      { item_name: "테스트 물건 1", photos: [photo, appraisal] },
      { item_name: "테스트 물건 2" }, // omits `photos`: the schema must fill in an empty array
    ]);
    expect(parsed.public_auction.rounds.map((r) => r.photos.map((p) => p.media_kind))).toEqual([
      ["photo", "appraisal"],
      [],
    ]);
  });

  it("accepts the live v1 and the photo-bearing v2, and rejects any other version", () => {
    for (const schema_version of [
      "foundation-platform.parcel_public_auction_by_pnu.v1",
      "foundation-platform.parcel_public_auction_by_pnu.v2",
    ]) {
      expect(PublicAuctionDocumentSchema.parse({ ...document(), schema_version })).toMatchObject({
        schema_version,
      });
    }
    expect(() =>
      PublicAuctionDocumentSchema.parse({
        ...document(),
        schema_version: "foundation-platform.parcel_public_auction_by_pnu.v3",
      }),
    ).toThrow();
  });

  it("rejects a malformed content address so it can never reach an img/href", () => {
    expect(() =>
      parse([{ item_name: "테스트 물건", photos: [{ ...photo, sha256: "../../etc/passwd" }] }]),
    ).toThrow();
  });

  it("builds the media URL off the configured catalog base, content-addressed by sha256", () => {
    expect(onbidMediaSrc({ sha256: photoSha, media_format: "jpg" })).toBe(
      `https://catalog.example.test/onbid/media/sha256/${photoSha}.jpg`,
    );
    expect(onbidMediaSrc({ sha256: appraisalSha, media_format: "pdf" })).toBe(
      `https://catalog.example.test/onbid/media/sha256/${appraisalSha}.pdf`,
    );
  });
});
