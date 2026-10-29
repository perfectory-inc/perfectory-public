import { afterEach, describe, expect, it, vi } from "vitest";
import {
  fetchPublicAuction,
  joinBlocks,
  onbidMediaSrc,
  PublicAuctionDocumentSchema,
} from "./public-auction";

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
        cltr_mng_no: "X-0001",
        onbid_pbanc_no: 1,
        pbct_no: 11,
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
    const bid_ends_at = "2026-01-05T08:00:00.000Z";
    const parsed = parse([
      {
        cltr_mng_no: "X-0001",
        item_name: "테스트 물건 1",
        bid_ends_at,
        photos: [photo, appraisal],
      },
      // A revision-1 round: no `photos`, read as [].
      { cltr_mng_no: "X-0002", item_name: "테스트 물건 2", bid_ends_at },
    ]);
    expect(parsed.public_auction.rounds.map((r) => r.photos.map((p) => p.media_kind))).toEqual([
      ["photo", "appraisal"],
      [],
    ]);
  });

  // Root ADR-0336: major 1's strings, `.v1` (which every later revision of the major carries) and
  // the legacy `.v2`; no `.v3` is ever written for a revision.
  it("accepts every string of major 1, and rejects any other version", () => {
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
      parse([
        {
          cltr_mng_no: "X-0001",
          item_name: "테스트 물건",
          bid_ends_at: "2026-01-05T08:00:00.000Z",
          photos: [{ ...photo, sha256: "../../etc/passwd" }],
        },
      ]),
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

// Root ADR-0340 §7: a revision-3 document carries each item and announcement round once, beside the
// rounds; the web joins them back onto every round that names them.
describe("public auction blocks", () => {
  it("joins each round to its item and announcement round, and leaves an older round alone", async () => {
    const base = document();
    const revision3 = {
      ...base,
      schema_revision: 3,
      public_auction: {
        rounds: [
          base.public_auction.rounds[0],
          { ...base.public_auction.rounds[0], pbct_no: 12, pbct_nsq: "002" },
          { ...base.public_auction.rounds[0], cltr_mng_no: "X-0002", onbid_pbanc_no: null },
        ],
        items: [
          {
            cltr_mng_no: "X-0001",
            detail: { icdl_cdtn_cont: "현 상태대로", leas_inf_list: [{ irst_div_nm: "주거" }] },
            history: [{ pbct_stat_nm: "유찰", valid_bidder_count: 0 }],
            media: [],
          },
        ],
        announcements: [{ onbid_pbanc_no: 1, pbct_no: 11, org_nm: "합성 기관" }],
      },
    };
    vi.stubGlobal("fetch", vi.fn().mockResolvedValue(Response.json(revision3)));
    const rounds = await fetchPublicAuction(pnu);
    expect(rounds.map((r) => r.item?.detail?.icdl_cdtn_cont ?? null)).toEqual([
      "현 상태대로",
      "현 상태대로",
      null,
    ]);
    expect(rounds.map((r) => r.announcement?.org_nm ?? null)).toEqual(["합성 기관", null, null]);
    expect(
      joinBlocks(PublicAuctionDocumentSchema.parse(base).public_auction)[0]?.item,
    ).toBeUndefined();
  });
});
