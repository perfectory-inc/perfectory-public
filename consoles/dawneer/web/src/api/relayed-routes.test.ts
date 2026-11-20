import { describe, expect, it } from "vitest";

import { DawneerClient, type Fetch, type SessionInfo } from "./client";
import relayed from "./relayed-routes.json";

const SESSION: SessionInfo = {
	sub: "1",
	name: "Synthetic",
	email: "s@example.test",
	csrf: "csrf-value",
};
const ID = "00000000-0000-5000-8000-000000000001";
const JOB = "shape_ledger_check";

type Route = { method: string; api: string; path: string };
const key = (r: Route) => `${r.method} ${r.api} ${r.path}`;

/** One call of every client method, with synthetic arguments. */
const CALLS: Record<string, (client: DawneerClient) => Promise<unknown>> = {
	listItems: (c) => c.listItems({}),
	getItem: (c) => c.getItem(ID),
	claim: (c) => c.claim(ID),
	release: (c) => c.release(ID),
	dryRun: (c) => c.dryRun(ID, {} as never),
	decide: (c) => c.decide(ID, {} as never, "key"),
	tileManifest: (c) => c.tileManifest(),
	listComplexes: (c) => c.listComplexes({}),
	searchCatalog: (c) => c.searchCatalog("x"),
	catalogEntity: (c) => c.catalogEntity("urn:li:dataset:synthetic"),
	scheduledJobs: (c) => c.scheduledJobs(),
	recordOperatorAction: (c) => c.recordOperatorAction(JOB, {} as never),
	datasetSnapshots: (c) => c.datasetSnapshots("synthetic"),
	serviceChecks: (c) => c.serviceChecks(),
	parcelEditions: (c) => c.parcelEditions(),
	byPnuCutovers: (c) => c.byPnuCutovers(),
	qualityChecks: (c) => c.qualityChecks(),
	pipelineGraph: (c) => c.pipelineGraph(),
	getComplex: (c) => c.getComplex(ID),
	rule: (c) => c.rule(ID, {} as never),
	auditEvents: (c) => c.auditEvents(new URLSearchParams()),
	auditImportStatus: (c) => c.auditImportStatus(),
	exportAudit: (c) => c.exportAudit(new URLSearchParams()),
};

describe("relayed routes", () => {
	it("lists exactly the routes the client calls, so the relay test covers each", async () => {
		const asked: Route[] = [];
		const fetchImpl: Fetch = async (url, init) => {
			const match = /^\/api\/([^/]+)\/([^?]*)/.exec(String(url));
			asked.push({
				method: init?.method ?? "GET",
				api: match?.[1] ?? "",
				path: match?.[2] ?? "",
			});
			return new Response("{}", {
				status: 200,
				headers: { "content-type": "application/json" },
			});
		};
		const client = new DawneerClient(SESSION, fetchImpl);

		const methods = Object.getOwnPropertyNames(DawneerClient.prototype).filter(
			(name) => name !== "constructor" && name !== "call",
		);
		expect(
			Object.keys(CALLS).sort(),
			"a new client method needs a line in CALLS",
		).toEqual(methods.sort());
		for (const call of Object.values(CALLS)) await call(client);

		expect([...new Set(asked.map(key))].sort()).toEqual(
			relayed.routes.map(key).sort(),
		);
	});
});
