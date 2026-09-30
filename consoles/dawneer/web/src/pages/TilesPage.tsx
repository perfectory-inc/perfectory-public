import { useQuery } from "@tanstack/react-query";
import type { ReactNode } from "react";

import type { DawneerClient, TileUnit } from "../api/client";
import { ago, formatBytes, label, TILE_UNIT_LABEL } from "../lib/catalogLabels";

function Field({ name, children }: { name: string; children: ReactNode }) {
  return (
    <div className="flex gap-3 py-1 text-sm">
      <dt className="w-32 shrink-0 text-slate-500">{name}</dt>
      <dd className="min-w-0 break-all font-mono text-xs leading-5 text-slate-800">{children}</dd>
    </div>
  );
}

function UnitCard({ name, unit }: { name: string; unit: TileUnit }) {
  const source = unit.source;
  const isStatic = source.kind === "static_pmtiles";
  return (
    <article className="rounded-lg border border-slate-200 bg-white p-5">
      <header className="flex items-baseline justify-between gap-3">
        <h2 className="text-lg font-semibold">{label(TILE_UNIT_LABEL, name)}</h2>
        <span className="rounded bg-slate-100 px-2 py-0.5 text-xs text-slate-600">
          {isStatic ? `정적 파일 · ${formatBytes(source.pmtiles_bytes)}` : "실시간 DB"}
        </span>
      </header>
      <p className="mt-1 text-sm text-slate-500">
        발행 {unit.serving_generation}세대 · 레이어 {Object.keys(unit.layers).length}개
      </p>
      <dl className="mt-4 border-t border-slate-100 pt-3">
        {Object.entries(unit.layers).map(([layerName, layer]) => (
          <Field key={layerName} name={`레이어 ${layerName}`}>
            확대 {layer.render_min_zoom}–{layer.render_max_zoom} · 식별자 {layer.feature_id_property}
          </Field>
        ))}
        <Field name="데이터 판">{unit.data_revision}</Field>
        <Field name="발행본">{unit.active_release_id}</Field>
        <Field name="레이크하우스 스냅숏">{unit.canonical_iceberg_snapshot_id}</Field>
        {isStatic ? (
          <>
            <Field name="파일 위치">{source.pmtiles_object_key}</Field>
            <Field name="SHA-256">{source.pmtiles_sha256}</Field>
          </>
        ) : null}
        <Field name="타일 주소">{source.tiles_url_template}</Field>
        <Field name="원천 기록">{unit.lineage.source_record_id}</Field>
      </dl>
    </article>
  );
}

/** What the map serves right now: one card per publication unit, straight from the live manifest. */
export function TilesPage({ client }: { client: DawneerClient }) {
  const manifest = useQuery({ queryKey: ["tile-manifest"], queryFn: () => client.tileManifest(), refetchInterval: 60_000 });

  if (manifest.isPending) return <p className="text-slate-500">불러오는 중…</p>;
  if (manifest.isError) return <p className="text-red-700">지도 발행 정보를 읽지 못했습니다: {manifest.error.message}</p>;

  const data = manifest.data;
  const units = Object.entries(data.publication_units).sort(([a], [b]) => a.localeCompare(b));
  return (
    <section>
      <div className="flex flex-wrap items-baseline gap-x-6 gap-y-1">
        <h1 className="text-xl font-semibold">지도 타일 발행</h1>
        <p className="text-sm text-slate-600">
          지금 지도가 보여 주는 판 · {data.manifest_generation}세대 · {ago(data.published_at)} 발행 (
          {new Date(data.published_at).toLocaleString("ko-KR")})
        </p>
      </div>
      <p className="mt-1 text-sm text-slate-500">
        발행 단위마다 어떤 데이터 판이 어느 파일로 나가고 있는지 보여 줍니다. 1분마다 새로 읽습니다.
      </p>
      <div className="mt-6 grid gap-4 lg:grid-cols-2 2xl:grid-cols-3">
        {units.map(([name, unit]) => (
          <UnitCard key={name} name={name} unit={unit} />
        ))}
      </div>
      <p className="mt-6 font-mono text-xs text-slate-400">manifest {data.current_version}</p>
    </section>
  );
}
