// apps/web/app/(public)/preview-floors/page.tsx
// DEV-only LIVE preview of the parcel 층 구성 + 호실 inspector.
//
// 100% server-rendered + CSS-only interactivity (radio tabs / :target modal) so
// it works WITHOUT client hydration. All data is real, reconciled from
// 표제부 (층) + 전유부 (호) + 전유공용면적 (전용면적, matched by PNU+동+호명).
// PNU는 표준 사투리(11번째 자리 1=일반/2=산) — ADR-0023 canonical.
import type { ReactNode } from "react";
import { type FloorsResponse, FloorsResponseSchema } from "@/lib/api/floors";
import koMessages from "@/lib/i18n/ko.json";
import { PREVIEW_PARCELS } from "./preview-data";

const GONGZZANG_API = "http://127.0.0.1:18091";
const FOUNDATION_PLATFORM_API = "http://127.0.0.1:18090";

const COPY = koMessages.previewFloors;

type Unit = {
  id: string;
  building_name: string;
  dong_name: string;
  ho_name: string;
  floor_label: string;
  exclusive_area_m2?: number;
  usage_name?: string;
  structure_name?: string;
};

type ParcelData = {
  pnu: string;
  addr: string;
  floors: FloorsResponse;
  units: Unit[];
};

async function fetchFloorsLive(pnu: string): Promise<FloorsResponse> {
  try {
    const res = await fetch(`${GONGZZANG_API}/api/floors?parcel_pnu=${pnu}`, {
      headers: { authorization: "Bearer DEV.devuser" },
      cache: "no-store",
    });
    if (!res.ok) return { buildings: [] };
    return FloorsResponseSchema.parse(await res.json());
  } catch {
    return { buildings: [] };
  }
}

async function fetchUnitsLive(pnu: string): Promise<Unit[]> {
  try {
    const res = await fetch(`${FOUNDATION_PLATFORM_API}/catalog/v1/parcels/by-pnu/${pnu}/units`, {
      cache: "no-store",
    });
    if (!res.ok) return [];
    return (await res.json()) as Unit[];
  } catch {
    return [];
  }
}

const hoLabel = (ho: string) => `${ho.replace(/호+$/u, "")}${COPY.unitSuffix}`;
const fmtArea = (a?: number) =>
  a == null ? null : `${(Math.round(a * 100) / 100).toLocaleString("ko-KR")}㎡`;

function lastNum(s: string): number {
  const m = String(s).match(/\d+/g);
  const last = m?.at(-1);
  return last ? Number.parseInt(last, 10) : 0;
}

function floorRank(label: string): number {
  if (!label) return 9999;
  const b = label.match(/^지하\s*(\d+)/u);
  if (b) return -Number.parseInt(b[1] ?? "0", 10);
  const a = label.match(/(\d+)/u);
  return a ? Number.parseInt(a[1] ?? "9999", 10) : 9999;
}

function dongList(units: Unit[]): string[] {
  const seen: string[] = [];
  for (const u of units) {
    const d = u.dong_name.trim();
    if (!seen.includes(d)) seen.push(d);
  }
  return seen.sort();
}

function floorGroups(units: Unit[]): { floor: string; units: Unit[] }[] {
  const map = new Map<string, Unit[]>();
  for (const u of units) {
    const key = u.floor_label.trim() || COPY.unknown;
    const list = map.get(key);
    if (list) list.push(u);
    else map.set(key, [u]);
  }
  return [...map.entries()]
    .sort((a, b) => floorRank(a[0]) - floorRank(b[0]))
    .map(([floor, us]) => ({
      floor,
      units: us.sort((a, b) => lastNum(a.ho_name) - lastNum(b.ho_name)),
    }));
}

function rooftopFact(b: FloorsResponse["buildings"][number]): { dd: string; note: string | null } {
  if (!b.has_rooftop) return { dd: COPY.none, note: null };
  const dd =
    b.rooftop_area_m2 != null
      ? `${COPY.present} · ${b.rooftop_area_m2.toLocaleString("ko-KR")}㎡`
      : COPY.present;
  const note = b.rooftop_usage ? COPY.rooftopUse.replace("{use}", b.rooftop_usage) : null;
  return { dd, note };
}

function BuildingXsec({ b }: { b: FloorsResponse["buildings"][number] }) {
  const roof = rooftopFact(b);
  const rows: ReactNode[] = [];
  if (b.has_rooftop) {
    rows.push(
      <div key="rt" className="pf-flr pf-rt">
        {COPY.rooftop}
      </div>,
    );
  }
  for (let f = b.above_ground; f >= 1; f--) {
    rows.push(
      <div key={`a${f}`} className={`pf-flr pf-above${f === b.above_ground ? " pf-top" : ""}`}>
        {f === b.above_ground || f === 1 ? `${f}F` : ""}
      </div>,
    );
  }
  rows.push(
    <div key="gl" className="pf-gl">
      <span>GL</span>
    </div>,
  );
  for (let f = 1; f <= b.below_ground; f++) {
    rows.push(
      <div key={`b${f}`} className={`pf-flr pf-below${f === b.below_ground ? " pf-bottom" : ""}`}>
        {f === b.below_ground ? `B${f}` : ""}
      </div>,
    );
  }
  return (
    <div>
      <h3 className="pf-bh">{COPY.floorComposition}</h3>
      <dl className="pf-facts">
        <div>
          <dt>{COPY.aboveGround}</dt>
          <dd>
            {b.above_ground}
            {COPY.floorSuffix}
          </dd>
        </div>
        <div>
          <dt>{COPY.belowGround}</dt>
          <dd>
            {b.below_ground}
            {COPY.floorSuffix}
          </dd>
        </div>
        <div>
          <dt>{COPY.rooftop}</dt>
          <dd>{roof.dd}</dd>
        </div>
      </dl>
      {roof.note ? <p className="pf-roof-note">{roof.note}</p> : null}
      <div className="pf-xsec">{rows}</div>
    </div>
  );
}

function DongPanel({ parcel, dong }: { parcel: ParcelData; dong: string }) {
  const groups = floorGroups(parcel.units.filter((u) => u.dong_name.trim() === dong));
  return (
    <div className="pf-scroller">
      {groups.map((g) => (
        <div key={g.floor} className="pf-fg">
          <div className="pf-fg-h">
            <span>{g.floor}</span>
            <span className="pf-fg-c">
              {g.units.length}
              {COPY.unitSuffix}
            </span>
          </div>
          <div className="pf-ho-grid">
            {g.units.map((u) => (
              <a key={u.id} id={`u-${u.id}`} href={`#u-${u.id}`} className="pf-ho">
                <span className="pf-ho-n">{hoLabel(u.ho_name)}</span>
                <span className={`pf-ho-a${u.exclusive_area_m2 == null ? " pf-noarea" : ""}`}>
                  {u.exclusive_area_m2 != null
                    ? COPY.exclusiveArea.replace("{area}", fmtArea(u.exclusive_area_m2) ?? "")
                    : COPY.areaMismatch}
                </span>
                {(u.usage_name?.trim() || u.structure_name?.trim()) && (
                  <span className="pf-ho-meta">
                    {[u.usage_name?.trim(), u.structure_name?.trim()].filter(Boolean).join(" · ")}
                  </span>
                )}
              </a>
            ))}
          </div>
        </div>
      ))}
    </div>
  );
}

function ParcelPanel({ parcel, pi }: { parcel: ParcelData; pi: number }) {
  const b = parcel.floors.buildings[0];
  const dongs = dongList(parcel.units);
  const buildingName = parcel.units.find((u) => u.building_name.trim())?.building_name.trim();
  const multiDong = dongs.length > 1;

  return (
    <section className="pf-panel" data-pi={pi}>
      <div className="pf-parcel-head">
        <div>
          <div className="pf-addr">{parcel.addr}</div>
          <div className="pf-pnu">PNU {parcel.pnu}</div>
        </div>
        {buildingName && <div className="pf-bname">{buildingName}</div>}
      </div>
      <div className="pf-split">
        <div className="pf-bcol">
          {b ? <BuildingXsec b={b} /> : <p className="pf-empty">{COPY.titleUnavailable}</p>}
        </div>
        <div className="pf-ucol">
          {multiDong ? (
            dongs.map((d, di) => (
              <div key={d || "_"}>
                <input
                  type="radio"
                  name={`dong-${pi}`}
                  id={`d-${pi}-${di}`}
                  className="pf-dongradio"
                  defaultChecked={di === 0}
                />
                <div className="pf-dong-content" data-di={di}>
                  <DongPanel parcel={parcel} dong={d} />
                </div>
              </div>
            ))
          ) : (
            <DongPanel parcel={parcel} dong={dongs[0] ?? ""} />
          )}
          {multiDong && (
            <div className="pf-dongbar">
              {dongs.map((d, di) => {
                const cnt = parcel.units.filter((u) => u.dong_name.trim() === d).length;
                return (
                  <label
                    key={d || "_"}
                    htmlFor={`d-${pi}-${di}`}
                    className="pf-dongchip"
                    data-di={di}
                  >
                    {d || COPY.singleBuilding}
                    <span className="pf-dongchip-c">
                      {cnt}
                      {COPY.unitSuffix}
                    </span>
                  </label>
                );
              })}
            </div>
          )}
        </div>
      </div>
    </section>
  );
}

function interactivityCss(parcels: ParcelData[]): string {
  const tabRules = parcels
    .map(
      (_, i) =>
        `#pf-tab-${i}:checked~.pf-tabs label[for="pf-tab-${i}"]{background:var(--color-canvas);color:var(--color-ink);border-color:var(--color-hairline-strong)}` +
        `#pf-tab-${i}:checked~.pf-tabs label[for="pf-tab-${i}"]::after{opacity:1}` +
        `#pf-tab-${i}:checked~.pf-panels .pf-panel[data-pi="${i}"]{display:block}`,
    )
    .join("");
  const dongRules = parcels
    .map((p, pi) => {
      const n = dongList(p.units).length;
      if (n <= 1) return "";
      return Array.from(
        { length: n },
        (_, di) =>
          `#d-${pi}-${di}:checked~.pf-dong-content[data-di="${di}"]{display:block}` +
          `#d-${pi}-${di}:checked~.pf-dongbar .pf-dongchip[data-di="${di}"]{background:var(--color-ink);color:var(--color-canvas);border-color:var(--color-ink);font-weight:600}`,
      ).join("");
    })
    .join("");
  return `${BASE_CSS}${tabRules}${dongRules}`;
}

export default async function PreviewFloorsPage() {
  const parcels: ParcelData[] = await Promise.all(
    PREVIEW_PARCELS.map(async (p) => ({
      pnu: p.pnu,
      addr: p.address,
      floors: await fetchFloorsLive(p.pnu),
      units: await fetchUnitsLive(p.pnu),
    })),
  );

  const totalUnits = parcels.reduce((n, p) => n + p.units.length, 0);
  const withArea = parcels.reduce(
    (n, p) => n + p.units.filter((u) => u.exclusive_area_m2 != null).length,
    0,
  );
  const stats: { n: string; k: string }[] = [
    { n: String(parcels.length), k: COPY.stats.parcel },
    { n: String(parcels.length), k: COPY.stats.building },
    { n: totalUnits.toLocaleString("ko-KR"), k: COPY.stats.unit },
    { n: withArea.toLocaleString("ko-KR"), k: COPY.stats.area },
  ];

  return (
    <main className="pf-wrap" id="pf-top">
      {/* CSS-only 탭/동/모달 — React 는 문자열 자식을 <style> 텍스트로 렌더한다(안전). */}
      <style>{interactivityCss(parcels)}</style>

      <header className="pf-head">
        <p className="pf-eyebrow">{COPY.header.eyebrow}</p>
        <h1 className="pf-title">{COPY.header.title}</h1>
        <p className="pf-lede">{COPY.header.description}</p>
      </header>

      <div className="pf-stats">
        {stats.map((s) => (
          <div key={s.k} className="pf-stat">
            <div className="pf-stat-n">{s.n}</div>
            <div className="pf-stat-k">{s.k}</div>
          </div>
        ))}
      </div>

      <section className="pf-section">
        <div className="pf-sech">
          <h2>{COPY.connection.title}</h2>
          <span>{COPY.connection.subtitle}</span>
        </div>
        <div className="pf-chain">
          {[
            {
              nm: COPY.connection.titleRegister.name,
              sub: COPY.connection.titleRegister.summary,
              rows: COPY.connection.titleRegister.fields,
              c: "title",
            },
            {
              nm: COPY.connection.exclusiveUnit.name,
              sub: COPY.connection.exclusiveUnit.summary,
              rows: COPY.connection.exclusiveUnit.fields,
              c: "unit",
            },
            {
              nm: COPY.connection.exclusiveArea.name,
              sub: COPY.connection.exclusiveArea.summary,
              rows: COPY.connection.exclusiveArea.fields,
              c: "area",
            },
          ].map((s) => (
            <div key={s.nm} className={`pf-src pf-src-${s.c}`}>
              <div className="pf-src-nm">{s.nm}</div>
              <div className="pf-src-sub">{s.sub}</div>
              <div className="pf-src-rows">{s.rows}</div>
            </div>
          ))}
        </div>
        <div className="pf-keys">
          <span>{COPY.connection.unitAreaKey}</span>
          <span>{COPY.connection.parcelKey}</span>
          <span>{COPY.connection.unitNameKey}</span>
        </div>
      </section>

      <section className="pf-section">
        <div className="pf-sech">
          <h2>{COPY.inspector.title}</h2>
          <span>{COPY.inspector.subtitle}</span>
        </div>
        {parcels.map((p, i) => (
          <input
            key={`r-${p.pnu}`}
            type="radio"
            name="pf-parcel"
            id={`pf-tab-${i}`}
            className="pf-tabradio"
            defaultChecked={i === 0}
          />
        ))}
        <div className="pf-tabs">
          {parcels.map((p, i) => {
            const ndong = dongList(p.units).length;
            return (
              <label key={p.pnu} htmlFor={`pf-tab-${i}`} className="pf-tab">
                <span className="pf-tab-addr">{p.addr}</span>
                <span className="pf-tab-cnt">
                  {COPY.inspector.unitCount.replace("{count}", String(p.units.length))}
                  {ndong > 1 ? COPY.inspector.buildingCount.replace("{count}", String(ndong)) : ""}
                </span>
              </label>
            );
          })}
        </div>
        <div className="pf-panels">
          {parcels.map((p, i) => (
            <ParcelPanel key={p.pnu} parcel={p} pi={i} />
          ))}
        </div>
      </section>

      <p className="pf-foot">
        {COPY.footnote} <code>catalog.building / building_unit</code> {COPY.footnoteDetail}
      </p>
    </main>
  );
}

const BASE_CSS = `
.pf-wrap{--pf-title-src:#7a6a3f;--pf-title-bg:#f4efe2;--pf-unit-src:#3f5a6b;--pf-unit-bg:#e8eef2;--pf-area-src:var(--color-primary);--pf-area-bg:#f6e7df;max-width:1080px;margin:32px auto 96px;padding:0 24px;font-variant-numeric:tabular-nums}
.pf-sr{position:absolute;width:1px;height:1px;overflow:hidden;clip:rect(0 0 0 0)}
.pf-head{display:flex;flex-direction:column;gap:10px;margin-bottom:28px}
.pf-eyebrow{font-size:12px;letter-spacing:.14em;text-transform:uppercase;color:var(--color-muted-soft);font-weight:600;margin:0}
.pf-title{font-size:clamp(24px,4vw,34px);line-height:1.1;letter-spacing:-.02em;font-weight:700;color:var(--color-ink);margin:0}
.pf-lede{color:var(--color-muted);font-size:15px;max-width:62ch;margin:0}
.pf-stats{display:flex;flex-wrap:wrap;border:1px solid var(--color-hairline);border-radius:12px;background:var(--color-canvas);overflow:hidden;margin-bottom:40px}
.pf-stat{flex:1 1 0;min-width:120px;padding:16px 20px;border-right:1px solid var(--color-hairline)}
.pf-stat:last-child{border-right:none}
.pf-stat-n{font-size:26px;font-weight:700;letter-spacing:-.02em}
.pf-stat-k{font-size:12.5px;color:var(--color-muted);margin-top:2px}
.pf-section{margin-bottom:40px}
.pf-sech{display:flex;align-items:baseline;gap:12px;margin-bottom:16px;padding-bottom:11px;border-bottom:2px solid var(--color-ink)}
.pf-sech h2{font-size:18px;margin:0;font-weight:700;letter-spacing:-.01em}
.pf-sech span{font-size:13px;color:var(--color-muted)}
.pf-chain{display:grid;grid-template-columns:1fr 1fr 1fr;gap:12px}
@media(max-width:720px){.pf-chain{grid-template-columns:1fr}}
.pf-src{border:1px solid var(--color-hairline);border-radius:10px;background:var(--color-canvas);padding:14px 16px;display:flex;flex-direction:column;gap:6px}
.pf-src-title{border-top:3px solid var(--pf-title-src)}
.pf-src-unit{border-top:3px solid var(--pf-unit-src)}
.pf-src-area{border-top:3px solid var(--pf-area-src)}
.pf-src-nm{font-weight:700;font-size:14.5px}
.pf-src-sub{font-size:12.5px;color:var(--color-muted)}
.pf-src-rows{font-size:12px;color:var(--color-muted)}
.pf-keys{display:flex;flex-wrap:wrap;gap:8px;margin-top:12px}
.pf-keys span{font-size:12px;padding:5px 11px;border-radius:999px;background:var(--pf-area-bg);color:var(--color-primary);font-weight:600}
.pf-tabradio,.pf-dongradio{position:absolute;opacity:0;pointer-events:none;width:0;height:0}
.pf-tabs{display:flex;flex-wrap:wrap;gap:6px}
.pf-tab{border:1px solid var(--color-hairline);background:var(--color-surface-card);color:var(--color-muted);padding:10px 15px;border-radius:10px 10px 0 0;cursor:pointer;display:flex;flex-direction:column;gap:2px;border-bottom:none;position:relative;top:1px}
.pf-tab::after{content:"";position:absolute;left:0;right:0;bottom:-1px;height:2px;background:var(--color-primary);opacity:0}
.pf-tab-addr{font-weight:700;color:var(--color-ink)}
.pf-tab-cnt{font-size:11.5px;color:var(--color-muted-soft)}
.pf-panels{border:1px solid var(--color-hairline-strong);border-radius:0 12px 12px 12px;background:var(--color-canvas);overflow:hidden}
.pf-panel{display:none}
.pf-parcel-head{padding:18px 22px;border-bottom:1px solid var(--color-hairline);display:flex;flex-wrap:wrap;justify-content:space-between;gap:12px;align-items:baseline}
.pf-addr{font-size:18px;font-weight:700}
.pf-pnu{font-size:12.5px;color:var(--color-muted);font-family:ui-monospace,monospace}
.pf-bname{font-weight:700}
.pf-split{display:grid;grid-template-columns:260px 1fr}
@media(max-width:780px){.pf-split{grid-template-columns:1fr}}
.pf-bcol{border-right:1px solid var(--color-hairline);padding:22px}
@media(max-width:780px){.pf-bcol{border-right:none;border-bottom:1px solid var(--color-hairline)}}
.pf-bh{margin:0 0 10px;font-size:13px;letter-spacing:.04em;text-transform:uppercase;color:var(--color-muted)}
.pf-facts{display:flex;flex-direction:column;gap:4px;margin:0 0 16px;font-size:13px}
.pf-facts div{display:flex;justify-content:space-between}
.pf-facts dt{color:var(--color-muted);margin:0}
.pf-facts dd{font-weight:600;margin:0}
.pf-roof-note{margin:-10px 0 14px;font-size:11.5px;line-height:1.4;color:var(--color-muted-soft)}
.pf-xsec{display:flex;flex-direction:column;gap:3px}
.pf-flr{height:13px;border-radius:2px;display:flex;align-items:center;justify-content:flex-end;padding-right:6px;font-size:8px;color:var(--color-muted-soft)}
.pf-rt{background:var(--pf-area-src);color:#fff;height:10px;border-radius:2px 2px 0 0}
.pf-above{background:color-mix(in srgb,var(--pf-title-src) 26%,var(--color-canvas))}
.pf-top{border-radius:3px 3px 0 0}
.pf-gl{height:2px;background:var(--color-ink);margin:2px 0;position:relative}
.pf-gl span{position:absolute;right:0;top:-13px;font-size:8px;color:var(--color-muted);letter-spacing:.1em}
.pf-below{background:color-mix(in srgb,var(--pf-unit-src) 20%,var(--color-canvas));opacity:.85}
.pf-bottom{border-radius:0 0 3px 3px}
.pf-ucol{min-width:0}
.pf-dong-content{display:none}
.pf-dongbar{display:flex;flex-wrap:wrap;gap:5px;padding:12px 18px;border-top:1px solid var(--color-hairline)}
.pf-dongchip{border:1px solid var(--color-hairline);background:var(--color-canvas);color:var(--color-muted);border-radius:999px;padding:5px 12px;font-size:12.5px;cursor:pointer}
.pf-dongchip-c{opacity:.6;margin-left:4px}
.pf-scroller{max-height:560px;overflow-y:auto}
.pf-fg{border-bottom:1px solid var(--color-hairline)}
.pf-fg:last-child{border-bottom:none}
.pf-fg-h{position:sticky;top:0;background:color-mix(in srgb,var(--color-canvas) 92%,var(--color-surface-card));padding:8px 18px;font-size:12px;font-weight:700;color:var(--color-muted);border-bottom:1px solid var(--color-hairline);display:flex;justify-content:space-between;z-index:1}
.pf-fg-c{font-weight:500;color:var(--color-muted-soft)}
.pf-ho-grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(148px,1fr));gap:1px;background:var(--color-hairline)}
.pf-ho{background:var(--color-canvas);padding:10px 14px;display:flex;flex-direction:column;gap:2px;color:var(--color-ink);text-decoration:none;scroll-margin-top:60px}
.pf-ho:hover{background:var(--pf-area-bg)}
.pf-ho:target{background:var(--pf-area-bg);box-shadow:inset 0 0 0 2px var(--color-primary)}
.pf-ho:target .pf-ho-n{color:var(--color-primary)}
.pf-ho-n{font-weight:700;font-size:13.5px}
.pf-ho-a{font-size:11.5px;color:var(--color-muted)}
.pf-ho-meta{font-size:11px;color:var(--color-muted-soft)}
.pf-noarea{color:var(--color-muted-soft);font-style:italic}
.pf-empty{font-size:13px;color:var(--color-muted)}
.pf-foot{margin-top:32px;border-top:1px solid var(--color-hairline);padding-top:16px;font-size:12.5px;color:var(--color-muted-soft)}
.pf-foot code{font-family:ui-monospace,monospace;font-size:12px}
`;
