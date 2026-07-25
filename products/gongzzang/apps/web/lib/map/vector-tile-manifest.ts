import { z } from "zod";

export const CORE_VECTOR_TILE_LAYER = "parcels" as const;
export const OPTIONAL_VECTOR_TILE_LAYERS = ["admin", "complex"] as const;
export const PARCEL_ANCHOR_AGGREGATE_VECTOR_TILE_LAYER = "parcel_anchor_aggregate" as const;
export const PARCEL_ANCHOR_VECTOR_TILE_LAYER = "parcel_anchor" as const;
export type VectorTileLayerId =
  | typeof CORE_VECTOR_TILE_LAYER
  | (typeof OPTIONAL_VECTOR_TILE_LAYERS)[number]
  | typeof PARCEL_ANCHOR_AGGREGATE_VECTOR_TILE_LAYER
  | typeof PARCEL_ANCHOR_VECTOR_TILE_LAYER;

type EnvLike = Record<string, string | undefined>;
const manifestUrlSymbol: unique symbol = Symbol("gongzzang.vectorTileManifestUrl");

const uuidSchema = z.string().uuid();

const safeGenerationSchema = z.number().int().min(1).max(Number.MAX_SAFE_INTEGER);

const runtimeTilesUrlTemplateSchema = z
  .string()
  .min(1)
  .superRefine((value, ctx) => {
    for (const placeholder of ["{z}", "{x}", "{y}"]) {
      if (value.split(placeholder).length - 1 !== 1) {
        ctx.addIssue({
          code: z.ZodIssueCode.custom,
          message: `${placeholder} must occur exactly once`,
        });
      }
    }
    if (/[{}]/.test(value.replaceAll("{z}", "").replaceAll("{x}", "").replaceAll("{y}", ""))) {
      ctx.addIssue({ code: z.ZodIssueCode.custom, message: "unknown URL placeholder" });
    }
    let parsed: URL;
    try {
      parsed = new URL(value);
    } catch {
      ctx.addIssue({ code: z.ZodIssueCode.custom, message: "tile URL must be absolute" });
      return;
    }
    if (parsed.protocol !== "https:" && parsed.protocol !== "http:") {
      ctx.addIssue({ code: z.ZodIssueCode.custom, message: "tile URL must use http or https" });
    }
    if (
      parsed.protocol === "http:" &&
      !["localhost", "127.0.0.1", "[::1]"].includes(parsed.hostname)
    ) {
      ctx.addIssue({ code: z.ZodIssueCode.custom, message: "http tile URLs are loopback-only" });
    }
  });

const VectorTileLineageSchema = z.object({
  source_record_id: uuidSchema,
  manifest_file_asset_id: uuidSchema,
  tilejson_file_asset_id: uuidSchema,
  source_file_asset_ids: z.array(uuidSchema),
});

const zoomSchema = z.number().int().min(0).max(24);
const martinIdentifierSchema = z
  .string()
  .min(1)
  .max(128)
  .regex(/^[A-Za-z][A-Za-z0-9_-]*$/, "Martin identifiers must be stable safe names");

const VectorTileArtifactSchema = z
  .object({
    source_layer: z.string().min(1),
    tile_min_zoom: zoomSchema,
    tile_max_zoom: zoomSchema,
    render_min_zoom: zoomSchema,
    render_max_zoom: zoomSchema,
    tilejson_object_key: z.string().min(1),
    object_key_prefix: z.string().min(1),
    flat_tile_count: z.number().int().min(1),
    flat_tile_total_bytes: z.number().int().min(1),
    lineage: VectorTileLineageSchema,
  })
  .superRefine((artifact, ctx) => {
    if (artifact.tile_min_zoom > artifact.tile_max_zoom) {
      ctx.addIssue({
        code: z.ZodIssueCode.custom,
        message: "tile_min_zoom must be <= tile_max_zoom",
        path: ["tile_min_zoom"],
      });
    }
    if (artifact.render_min_zoom > artifact.render_max_zoom) {
      ctx.addIssue({
        code: z.ZodIssueCode.custom,
        message: "render_min_zoom must be <= render_max_zoom",
        path: ["render_min_zoom"],
      });
    }
  });

const tilesTemplateSchema = z
  .string()
  .min(1)
  .refine((value) => ["{object_key_prefix}", "{z}", "{x}", "{y}"].every((p) => value.includes(p)), {
    message: "tiles_url_template must contain {object_key_prefix}, {z}, {x}, and {y}",
  });

const VectorTileManifestSchema = z.object({
  schema_version: z.number().int().min(1),
  current_version: uuidSchema,
  previous_version: uuidSchema,
  tiles_url_template: tilesTemplateSchema,
  published_at: z.string().datetime({ offset: true }),
  artifacts: z
    .record(z.string(), VectorTileArtifactSchema)
    .refine((artifacts) => Object.keys(artifacts).length > 0, {
      message: "foundation-platform vector tile manifest must include at least one artifact",
    }),
});

export type VectorTileManifest = z.infer<typeof VectorTileManifestSchema> & {
  readonly [manifestUrlSymbol]?: string;
};
export type VectorTileArtifact = z.infer<typeof VectorTileArtifactSchema>;

export type VectorTileSource = {
  type: "vector";
  tiles: [string];
  minzoom: number;
  maxzoom: number;
  promoteId?: string;
};

const RuntimeTileLayerSchema = z
  .object({
    source_layer: z
      .string()
      .min(1)
      .regex(/^[A-Za-z][A-Za-z0-9_-]{0,127}$/, "source_layer must be a Martin identifier"),
    feature_id_property: z
      .string()
      .min(1)
      .regex(/^[a-z][a-z0-9_]*$/, "feature_id_property must be lower-case"),
    tile_min_zoom: zoomSchema,
    tile_max_zoom: zoomSchema,
    render_min_zoom: zoomSchema,
    render_max_zoom: zoomSchema,
    feature_filter_properties: z.record(z.string(), z.string()),
  })
  .strict()
  .superRefine((layer, ctx) => {
    if (layer.tile_min_zoom > layer.tile_max_zoom) {
      ctx.addIssue({ code: z.ZodIssueCode.custom, message: "tile zoom range is inverted" });
    }
    if (layer.render_min_zoom > layer.render_max_zoom) {
      ctx.addIssue({ code: z.ZodIssueCode.custom, message: "render zoom range is inverted" });
    }
  });

const DynamicPostgisSourceSchema = z
  .object({
    kind: z.literal("dynamic_postgis"),
    martin_source_id: martinIdentifierSchema,
    tiles_url_template: runtimeTilesUrlTemplateSchema,
    postgis_projection_revision: uuidSchema,
    cache_policy: z.literal("no_store"),
  })
  .strict()
  .superRefine((source, ctx) => {
    if (/[?#]/.test(source.tiles_url_template)) {
      ctx.addIssue({
        code: z.ZodIssueCode.custom,
        message: "dynamic PostGIS tile URLs must not use query/hash cache selectors",
        path: ["tiles_url_template"],
      });
    }
  });

const StaticPmtilesSourceSchema = z
  .object({
    kind: z.literal("static_pmtiles"),
    martin_source_id: martinIdentifierSchema,
    tiles_url_template: runtimeTilesUrlTemplateSchema,
    pmtiles_object_key: z.string().min(1),
    pmtiles_file_asset_id: uuidSchema,
    pmtiles_sha256: z.string().regex(/^[0-9a-f]{64}$/),
    pmtiles_bytes: z.number().int().positive(),
  })
  .strict();

const RuntimeLineageSchema = z
  .object({
    source_record_id: uuidSchema,
    source_file_asset_ids: z.array(uuidSchema).min(1),
  })
  .strict();

const RuntimePublicationUnitSchema = z
  .object({
    data_revision: uuidSchema,
    serving_generation: safeGenerationSchema,
    active_release_id: uuidSchema,
    canonical_iceberg_snapshot_id: z.string().regex(/^[1-9][0-9]*$/),
    source: z.discriminatedUnion("kind", [DynamicPostgisSourceSchema, StaticPmtilesSourceSchema]),
    layers: z
      .record(z.string().min(1), RuntimeTileLayerSchema)
      .refine((value) => Object.keys(value).length > 0),
    lineage: RuntimeLineageSchema,
  })
  .strict();

const VectorTileRuntimeManifestSchema = z
  .object({
    schema_version: z.literal(2),
    current_version: uuidSchema,
    manifest_generation: safeGenerationSchema,
    refresh_after_seconds: z.literal(4),
    published_at: z.string().datetime({ offset: true }),
    publication_units: z
      .record(z.string().min(1), RuntimePublicationUnitSchema)
      .refine((units) => Object.keys(units).length > 0),
  })
  .strict()
  .superRefine((manifest, ctx) => {
    const sourceIds = new Set<string>();
    for (const [unitName, unit] of Object.entries(manifest.publication_units)) {
      if (!martinIdentifierSchema.safeParse(unitName).success) {
        ctx.addIssue({
          code: z.ZodIssueCode.custom,
          message: "publication unit names must be safe Martin identifiers",
          path: ["publication_units", unitName],
        });
      }
      validateRuntimePublicationUnit(unitName, unit, ctx);
      if (sourceIds.has(unit.source.martin_source_id)) {
        ctx.addIssue({
          code: z.ZodIssueCode.custom,
          message: "Martin source ids must be globally unique",
          path: ["publication_units", unitName, "source", "martin_source_id"],
        });
      } else {
        sourceIds.add(unit.source.martin_source_id);
      }
    }
  });

function validateRuntimePublicationUnit(
  unitName: string,
  unit: z.infer<typeof RuntimePublicationUnitSchema>,
  ctx: z.RefinementCtx,
): void {
  const routeSuffix = `/${unit.source.martin_source_id}/{z}/{x}/{y}`;
  if (!unit.source.tiles_url_template.endsWith(routeSuffix)) {
    ctx.addIssue({
      code: z.ZodIssueCode.custom,
      message: "Martin tile URL must end with its declared source id and tile placeholders",
      path: ["publication_units", unitName, "source", "tiles_url_template"],
    });
  }
  const sourceLayers = new Set<string>();
  for (const [layerName, layer] of Object.entries(unit.layers)) {
    if (!martinIdentifierSchema.safeParse(layerName).success) {
      ctx.addIssue({
        code: z.ZodIssueCode.custom,
        message: "runtime layer names must be safe Martin identifiers",
        path: ["publication_units", unitName, "layers", layerName],
      });
    }
    if (sourceLayers.has(layer.source_layer)) {
      ctx.addIssue({
        code: z.ZodIssueCode.custom,
        message: "Martin source-layer ids must be unique within a publication unit",
        path: ["publication_units", unitName, "layers", layerName, "source_layer"],
      });
    } else {
      sourceLayers.add(layer.source_layer);
    }
  }
  if (unit.source.kind !== "static_pmtiles") return;

  const expectedFilename = `${unitName}-${unit.active_release_id}.pmtiles`;
  const objectFilename = unit.source.pmtiles_object_key.split("/").at(-1);
  if (
    objectFilename !== expectedFilename ||
    unit.source.martin_source_id !== expectedFilename.slice(0, -".pmtiles".length)
  ) {
    ctx.addIssue({
      code: z.ZodIssueCode.custom,
      message: "static PMTiles source id must equal its release-addressed filename stem",
      path: ["publication_units", unitName, "source"],
    });
  }
}

export type VectorTileRuntimeManifest = z.infer<typeof VectorTileRuntimeManifestSchema>;
export type VectorTileRuntimePublicationUnit = z.infer<typeof RuntimePublicationUnitSchema>;
export type VectorTileRuntimeLayer = z.infer<typeof RuntimeTileLayerSchema>;
export type VectorTileRuntimeFetchResult = {
  manifest: VectorTileRuntimeManifest;
  etag: string | undefined;
  notModified: boolean;
};

export function parseVectorTileManifest(input: unknown): VectorTileManifest {
  return VectorTileManifestSchema.parse(input);
}

export function resolveVectorTileRuntimeEnv(): EnvLike {
  return {
    NEXT_PUBLIC_FOUNDATION_PLATFORM_BASE_URL: process.env.NEXT_PUBLIC_FOUNDATION_PLATFORM_BASE_URL,
    NEXT_PUBLIC_TILES_MANIFEST_URL: process.env.NEXT_PUBLIC_TILES_MANIFEST_URL,
  };
}

export function resolveVectorTileManifestUrl(
  env: EnvLike = resolveVectorTileRuntimeEnv(),
): string | undefined {
  const explicit = nonempty(env.NEXT_PUBLIC_TILES_MANIFEST_URL);
  if (explicit) return explicit;

  const foundationPlatformBase = nonempty(env.NEXT_PUBLIC_FOUNDATION_PLATFORM_BASE_URL);
  if (foundationPlatformBase) {
    const base = foundationPlatformBase.endsWith("/")
      ? foundationPlatformBase.slice(0, -1)
      : foundationPlatformBase;
    return `${base}/catalog/v1/vector-tiles/manifest`;
  }

  return undefined;
}

export function resolveVectorTileAllowedOrigins(
  env: EnvLike = resolveVectorTileRuntimeEnv(),
): string[] {
  const urls = [
    resolveVectorTileManifestUrl(env),
    nonempty(env.NEXT_PUBLIC_TILES_MANIFEST_URL),
    nonempty(env.NEXT_PUBLIC_FOUNDATION_PLATFORM_BASE_URL),
  ];
  const origins = new Set<string>();
  for (const url of urls) {
    if (!url) continue;
    try {
      origins.add(new URL(url).origin);
    } catch {
      // Environment validation reports bad URLs elsewhere; CSP assembly should stay resilient.
    }
  }
  return [...origins].sort();
}

export async function fetchVectorTileManifest(
  fetcher: typeof fetch = fetch,
  env: EnvLike = resolveVectorTileRuntimeEnv(),
): Promise<VectorTileManifest> {
  const manifestUrl = resolveVectorTileManifestUrl(env);
  if (!manifestUrl) {
    throw new Error(
      "NEXT_PUBLIC_TILES_MANIFEST_URL or NEXT_PUBLIC_FOUNDATION_PLATFORM_BASE_URL is required for vector tiles",
    );
  }
  const response = await fetcher(manifestUrl, {
    headers: { accept: "application/json" },
    cache: "no-store",
  });
  if (!response.ok) {
    throw new Error(`vector tile manifest fetch failed: ${response.status}`);
  }
  return attachManifestUrl(parseVectorTileManifest(await response.json()), manifestUrl);
}

/** Resolves the strict v2 Catalog runtime endpoint; v1's configurable URL is never reused. */
export function resolveVectorTileRuntimeManifestUrl(
  env: EnvLike = resolveVectorTileRuntimeEnv(),
): string | undefined {
  const foundationPlatformBase = nonempty(env.NEXT_PUBLIC_FOUNDATION_PLATFORM_BASE_URL);
  if (!foundationPlatformBase) return undefined;
  const base = foundationPlatformBase.endsWith("/")
    ? foundationPlatformBase.slice(0, -1)
    : foundationPlatformBase;
  return `${base}/catalog/v1/vector-tiles/runtime-manifest`;
}

/** Fetches v2 with a standards-compliant conditional ETag request. */
export async function fetchVectorTileRuntimeManifest(
  fetcher: typeof fetch = fetch,
  env: EnvLike = resolveVectorTileRuntimeEnv(),
  options: { etag?: string; previous?: VectorTileRuntimeManifest; signal?: AbortSignal } = {},
): Promise<VectorTileRuntimeFetchResult> {
  const manifestUrl = resolveVectorTileRuntimeManifestUrl(env);
  if (!manifestUrl) {
    throw new Error(
      "NEXT_PUBLIC_FOUNDATION_PLATFORM_BASE_URL is required for the v2 vector manifest",
    );
  }
  const headers: Record<string, string> = { accept: "application/json" };
  if (options.etag) headers["if-none-match"] = options.etag;
  const response = await fetcher(manifestUrl, {
    headers,
    cache: "no-store",
    signal: options.signal,
  });
  if (response.status === 304) {
    if (!options.previous) throw new Error("v2 manifest returned 304 without a previous manifest");
    return { manifest: options.previous, etag: options.etag, notModified: true };
  }
  if (!response.ok)
    throw new Error(`vector tile runtime manifest fetch failed: ${response.status}`);
  const manifest = parseVectorTileRuntimeManifest(await response.json());
  return { manifest, etag: response.headers.get("etag") ?? undefined, notModified: false };
}

/** Parses v2 only; unknown schema versions fail closed. */
export function parseVectorTileRuntimeManifest(input: unknown): VectorTileRuntimeManifest {
  return VectorTileRuntimeManifestSchema.parse(input);
}

/** Builds a Mapbox vector source from a complete v2 unit without object-key substitution. */
export function buildVectorTileRuntimeSource(
  unit: VectorTileRuntimePublicationUnit,
  layer: string,
): VectorTileSource {
  const layerMetadata = unit.layers[layer];
  if (!layerMetadata) throw new Error(`vector tile runtime layer is missing: ${layer}`);
  const source: VectorTileSource = {
    type: "vector",
    tiles: [unit.source.tiles_url_template],
    minzoom: layerMetadata.tile_min_zoom,
    maxzoom: layerMetadata.tile_max_zoom,
    promoteId: layerMetadata.feature_id_property,
  };
  return source;
}

export function buildVectorTileSource(
  manifest: VectorTileManifest,
  layer: VectorTileLayerId,
  options: { promoteId?: string; tileUrlBaseUrl?: string } = {},
): VectorTileSource {
  const artifact = manifest.artifacts[layer];
  if (!artifact) {
    throw new Error(`vector tile artifact is missing: ${layer}`);
  }
  const source: VectorTileSource = {
    type: "vector",
    tiles: [
      materializeTilesUrl(manifest.tiles_url_template, artifact, {
        manifestUrl: manifest[manifestUrlSymbol],
        tileUrlBaseUrl: options.tileUrlBaseUrl,
      }),
    ],
    minzoom: artifact.tile_min_zoom,
    maxzoom: artifact.tile_max_zoom,
  };
  if (options.promoteId) source.promoteId = options.promoteId;
  return source;
}

export function getVectorTileArtifact(
  manifest: VectorTileManifest,
  layer: VectorTileLayerId,
): VectorTileArtifact | undefined {
  return manifest.artifacts[layer];
}

function attachManifestUrl(manifest: VectorTileManifest, manifestUrl: string): VectorTileManifest {
  return Object.defineProperty(manifest, manifestUrlSymbol, {
    value: manifestUrl,
    enumerable: false,
  });
}

function materializeTilesUrl(
  template: string,
  artifact: VectorTileArtifact,
  runtime: { manifestUrl?: string; tileUrlBaseUrl?: string },
): string {
  const placeholder = "{object_key_prefix}";
  const materialized = template
    .replaceAll(`${placeholder}/`, `${artifact.object_key_prefix.replace(/\/+$/, "")}/`)
    .replaceAll(placeholder, artifact.object_key_prefix);
  if (isAbsoluteHttpUrl(materialized)) return materialized;

  const origin = resolveTileUrlOrigin(runtime);
  if (materialized.startsWith("/")) return `${origin}${materialized}`;
  return `${origin}/${materialized}`;
}

function resolveTileUrlOrigin(runtime: { manifestUrl?: string; tileUrlBaseUrl?: string }): string {
  const baseUrl = runtime.tileUrlBaseUrl ?? runtime.manifestUrl;
  if (!baseUrl) {
    throw new Error("relative tiles_url_template requires a manifest URL or tileUrlBaseUrl");
  }
  return new URL(baseUrl).origin;
}

function isAbsoluteHttpUrl(value: string): boolean {
  try {
    const url = new URL(value);
    return url.protocol === "http:" || url.protocol === "https:";
  } catch {
    return false;
  }
}

function nonempty(value: string | undefined): string | undefined {
  const trimmed = value?.trim();
  return trimmed ? trimmed : undefined;
}
