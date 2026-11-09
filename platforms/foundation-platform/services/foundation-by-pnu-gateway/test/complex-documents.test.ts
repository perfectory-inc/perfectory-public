import type { Miniflare } from "miniflare";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import connectionContract from "../../../config/r2-connections.contract.json";
import { startGateway, TEST_ALLOWED_ORIGIN } from "./miniflare";

// Root ADR-0456: the parcel edge serves the industrial-complex index and profiles from the lakehouse
// bucket its lane binds. The ids are synthetic; the bytes are stand-ins the read path never inspects.
const DOCS = connectionContract.industrial_complex_documents;
const PROFILES = connectionContract.profile_gateway.object_key;
const PROFILE = `${PROFILES.root}/0196e7e0-3c20-7000-8000-100000000002${PROFILES.suffix}`;

describe("the parcel edge serves the industrial-complex documents", () => {
	let runtime: Miniflare | undefined;

	beforeAll(async () => {
		runtime = await startGateway("parcel");
		const bucket = await runtime.getR2Bucket(DOCS.r2_binding);
		await bucket.put(DOCS.index_object, '{"generation":1}\n');
		await bucket.put(PROFILE, '{"complex_id":"x"}\n');
	});

	afterAll(async () => {
		await runtime?.dispose();
	});

	function get(path: string, init?: Parameters<Miniflare["dispatchFetch"]>[1]) {
		if (runtime === undefined) throw new Error("Miniflare did not start");
		return runtime.dispatchFetch(`https://catalog.example.test/${path}`, init);
	}

	it("serves the index revalidated by ETag, and 304s an unchanged one", async () => {
		const response = await get(DOCS.index_object, {
			headers: { Origin: TEST_ALLOWED_ORIGIN },
		});
		expect(response.status).toBe(200);
		expect(response.headers.get("cache-control")).toBe(
			DOCS.index_cache_control,
		);
		expect(response.headers.get("access-control-allow-origin")).toBe(
			TEST_ALLOWED_ORIGIN,
		);
		expect(await response.text()).toBe('{"generation":1}\n');
		const etag = response.headers.get("etag") ?? "";
		expect(
			(await get(DOCS.index_object, { headers: { "If-None-Match": etag } }))
				.status,
		).toBe(304);
	});

	it("serves a profile immutable, twice (the second from the edge cache)", async () => {
		for (const pass of ["miss", "hit"]) {
			const response = await get(PROFILE);
			expect(response.status, pass).toBe(200);
			expect(response.headers.get("cache-control"), pass).toBe(
				connectionContract.profile_gateway.cache_control,
			);
			expect(await response.text(), pass).toBe('{"complex_id":"x"}\n');
		}
	});

	it("refuses an origin outside the allowlist, a write, a query and an unknown key", async () => {
		expect(
			(
				await get(DOCS.index_object, {
					headers: { Origin: "https://other.example.test" },
				})
			).status,
		).toBe(403);
		expect((await get(DOCS.index_object, { method: "POST" })).status).toBe(405);
		expect((await get(`${DOCS.index_object}?x=1`)).status).toBe(404);
		expect(
			(
				await get(
					`${PROFILES.root}/0196e7e0-3c20-7000-8000-100000000009${PROFILES.suffix}`,
				)
			).status,
		).toBe(404);
		// The history is kept, not served: only the index and the profiles are paths.
		expect((await get(`${DOCS.index_history_dir}/1.json`)).status).toBe(404);
	});
});

it("the profile path rule is the profile gateway's key layout", () => {
	// The Worker reads by the documents block's rule; the profile key's owner is profile_gateway.object_key.
	const rule = new RegExp(`^(?:${DOCS.profile_path_pattern})$`);
	expect(rule.test(PROFILE)).toBe(true);
	expect(DOCS.profile_path_pattern.startsWith(`${PROFILES.root}/`)).toBe(true);
	expect(DOCS.profile_path_pattern).toContain(PROFILES.artifact_id_pattern);
	expect(rule.test(PROFILE.replace(PROFILES.suffix, ".jsonx"))).toBe(false);
});

it("the building lane does not serve the complex documents", async () => {
	const runtime = await startGateway("building");
	try {
		const bucket = await runtime.getR2Bucket(DOCS.r2_binding);
		await bucket.put(DOCS.index_object, "{}\n");
		expect(
			(
				await runtime.dispatchFetch(
					`https://buildings.example.test/${DOCS.index_object}`,
				)
			).status,
		).toBe(404);
	} finally {
		await runtime.dispose();
	}
});
