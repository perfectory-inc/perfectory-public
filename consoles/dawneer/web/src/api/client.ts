// The screens' only way to the platforms: the Dawneer server's own routes (root ADR-0116).
// The browser holds no token. Reads go to `/api/foundation/...`; every write carries the session's
// CSRF value, and a decision carries an idempotency key so a retry cannot decide twice.
import type { components } from "./foundation";
import type { components as CatalogComponents } from "./catalog";
import type { components as DataCatalogComponents } from "./data-catalog";

type Schemas = components["schemas"];
export type ReviewItem = Schemas["LineageReviewItem"];
export type ReviewItemPage = Schemas["LineageReviewItemPage"];
export type ReviewStatus = Schemas["LineageReviewStatus"];
export type DecisionRequest = Schemas["LineageDecisionRequest"];
export type DecisionResponse = Schemas["LineageDecisionResponse"];
export type StewardDecision = Schemas["LineageStewardDecision"];
export type ReviewClaim = Schemas["LineageReviewClaim"];
export type ApprovalRequest = Schemas["LineageApprovalRequest"];
type CatalogSchemas = CatalogComponents["schemas"];
export type TileManifest = CatalogSchemas["VectorTileRuntimeManifestResponse"];
export type TileUnit = CatalogSchemas["VectorTilePublicationUnitResponse"];
export type Complex = CatalogSchemas["IndustrialComplexResponse"];
export type ComplexPage = CatalogSchemas["IndustrialComplexListResponse"];

type DataCatalogSchemas = DataCatalogComponents["schemas"];
export type CatalogEntity = DataCatalogSchemas["DataCatalogEntity"];
export type CatalogSearchPage = DataCatalogSchemas["DataCatalogSearchPage"];
export type CatalogNeighbourhood = DataCatalogSchemas["DataCatalogNeighbourhood"];

/** The complex list's filters, as Foundation names them. */
export interface ComplexFilter {
  q?: string;
  sidoCode?: string;
  status?: string;
  page?: number;
  size?: number;
  sort?: "name_asc" | "area_desc" | "official_complex_code_asc";
}

export interface SessionInfo {
  sub: string;
  name: string;
  email: string;
  csrf: string;
}

/** A response the platform refused, with its message for the screen. */
export class ApiError extends Error {
  constructor(
    readonly status: number,
    message: string,
  ) {
    super(message);
  }
}

/** Thrown when there is no session; the screen sends the browser to sign in. */
export class NotSignedIn extends Error {}

export type Fetch = typeof fetch;

// The browser's fetch must be called as a plain function (or on window). Kept as a default
// argument and called as `this.fetchImpl(...)`, it runs with the client as `this` and throws
// "Illegal invocation" — so the default is a wrapper that calls it unbound, at call time.
const browserFetch: Fetch = (input, init) => fetch(input, init);

async function readError(response: Response): Promise<string> {
  const text = await response.text();
  try {
    const body = JSON.parse(text) as { error?: string };
    return body.error ?? text;
  } catch {
    return text || response.statusText;
  }
}

export class DawneerClient {
  constructor(
    private readonly session: SessionInfo,
    private readonly fetchImpl: Fetch = browserFetch,
  ) {}

  private async call<T>(
    path: string,
    init: RequestInit & { idempotencyKey?: string } = {},
    api: "foundation" | "data-catalog" = "foundation",
  ): Promise<T> {
    const headers = new Headers(init.headers);
    const method = init.method ?? "GET";
    if (method !== "GET") {
      headers.set("x-dawneer-csrf", this.session.csrf);
      headers.set("content-type", "application/json");
    }
    if (init.idempotencyKey) headers.set("idempotency-key", init.idempotencyKey);
    const response = await this.fetchImpl(`/api/${api}/${path}`, {
      ...init,
      method,
      headers,
      credentials: "same-origin",
    });
    if (response.status === 401) throw new NotSignedIn();
    if (!response.ok) throw new ApiError(response.status, await readError(response));
    if (response.status === 204) return undefined as T;
    return (await response.json()) as T;
  }

  listItems(filter: {
    status?: ReviewStatus;
    prefix?: string;
    undecidedOnly?: boolean;
    after?: string;
    limit?: number;
  }): Promise<ReviewItemPage> {
    const query = new URLSearchParams();
    if (filter.status) query.set("status", filter.status);
    if (filter.prefix) query.set("prefix", filter.prefix);
    if (filter.undecidedOnly) query.set("undecided_only", "true");
    if (filter.after) query.set("after", filter.after);
    query.set("limit", String(filter.limit ?? 50));
    return this.call(`lineage-review/items?${query.toString()}`);
  }

  getItem(itemId: string): Promise<ReviewItem> {
    return this.call(`lineage-review/items/${encodeURIComponent(itemId)}`);
  }

  claim(itemId: string): Promise<ReviewClaim> {
    return this.call(`lineage-review/items/${encodeURIComponent(itemId)}/claim`, { method: "POST" });
  }

  release(itemId: string): Promise<void> {
    return this.call(`lineage-review/items/${encodeURIComponent(itemId)}/release`, { method: "POST" });
  }

  /** Checks a decision with the platform's own rules without storing it. */
  dryRun(itemId: string, decision: DecisionRequest): Promise<DecisionResponse> {
    return this.call(`lineage-review/items/${encodeURIComponent(itemId)}/decisions?dry_run=true`, {
      method: "POST",
      body: JSON.stringify(decision),
    });
  }

  decide(itemId: string, decision: DecisionRequest, idempotencyKey: string): Promise<DecisionResponse> {
    return this.call(`lineage-review/items/${encodeURIComponent(itemId)}/decisions`, {
      method: "POST",
      body: JSON.stringify(decision),
      idempotencyKey,
    });
  }


  /** What the map serves right now: one entry per publication unit (read-only). */
  tileManifest(): Promise<TileManifest> {
    return this.call("vector-tiles/runtime-manifest");
  }

  listComplexes(filter: ComplexFilter): Promise<ComplexPage> {
    const query = new URLSearchParams();
    if (filter.q) query.set("q", filter.q);
    if (filter.sidoCode) query.set("sido_code", filter.sidoCode);
    if (filter.status) query.set("status", filter.status);
    if (filter.sort) query.set("sort", filter.sort);
    query.set("page", String(filter.page ?? 0));
    query.set("size", String(filter.size ?? 50));
    return this.call(`complexes?${query.toString()}`);
  }

  /** Datasets whose name or description matches, best first (read-only, root ADR-0119). */
  searchCatalog(text: string, start = 0, count = 20): Promise<CatalogSearchPage> {
    const query = new URLSearchParams({ q: text, start: String(start), count: String(count) });
    return this.call(`search?${query.toString()}`, {}, "data-catalog");
  }

  /** One dataset or job with its columns and one step of lineage either way. */
  catalogEntity(urn: string): Promise<CatalogNeighbourhood> {
    return this.call(`entity?${new URLSearchParams({ urn }).toString()}`, {}, "data-catalog");
  }

  getComplex(complexId: string): Promise<Complex> {
    return this.call(`complexes/${encodeURIComponent(complexId)}`);
  }

  rule(decisionId: string, request: ApprovalRequest): Promise<StewardDecision> {
    return this.call(`lineage-review/decisions/${encodeURIComponent(decisionId)}/approval`, {
      method: "POST",
      body: JSON.stringify(request),
    });
  }
}

/** Who is signed in, or null. */
export async function loadSession(fetchImpl: Fetch = browserFetch): Promise<SessionInfo | null> {
  const response = await fetchImpl("/api/session", { credentials: "same-origin" });
  if (response.status === 401) return null;
  if (!response.ok) throw new ApiError(response.status, await readError(response));
  return (await response.json()) as SessionInfo;
}

/** Ends the session and returns where to send the browser. */
export async function signOut(session: SessionInfo, fetchImpl: Fetch = browserFetch): Promise<string> {
  const response = await fetchImpl("/auth/logout", {
    method: "POST",
    headers: { "x-dawneer-csrf": session.csrf },
    credentials: "same-origin",
  });
  const body = (await response.json().catch(() => ({}))) as { end_session_url?: string | null };
  return body.end_session_url ?? "/";
}
