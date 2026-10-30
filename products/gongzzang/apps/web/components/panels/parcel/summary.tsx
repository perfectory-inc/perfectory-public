// apps/web/components/panels/parcel/summary.tsx
"use client";
import { useQuery } from "@tanstack/react-query";
import { useTranslations } from "next-intl";
import { type ReactNode, useCallback, useEffect, useRef, useState } from "react";
import type { Building } from "@/lib/api/buildings";
import { jibunOf, type ParcelDocument } from "@/lib/api/parcels";
import { formatAreaM2, formatAreaPyeong, formatPriceKrw, UNITS } from "@/lib/listings/format";
import type { PanelStackEntry } from "@/lib/panel/types";
import { usePanelStack } from "@/lib/panel/use-panel-stack";
import { ParcelCourtAuctionCard } from "./court-auction";
import { ParcelListingsCard } from "./listings";
import { ParcelPublicAuctionCard } from "./public-auction";
import {
  parcelBuildingsQuery,
  parcelCourtAuctionQuery,
  parcelListingsQuery,
  parcelPublicAuctionQuery,
} from "./queries";

type Entry = Extract<PanelStackEntry, { kind: "parcel" }>;

/** A section of a category's page: its anchor tab and its content, shown only when it has any. */
type Section = { id: string; label: string; content: ReactNode };

/**
 * The parcel panel (owner decision 2026-10-19): a bottom bar of categories (필지정보 always, 매물
 * when the parcel has listings; 경매·공매·공장 join when Foundation publishes their by-PNU lanes,
 * root ADR-0280) and, inside a category, one scrolling page whose sections a row of anchor tabs
 * follows and jumps to.
 */
export function ParcelSummaryCard({ entry, data }: { entry: Entry; data: ParcelDocument }) {
  const t = useTranslations("panels.parcel.summary");
  const [category, setCategory] = useState<
    "parcel" | "listings" | "publicAuction" | "courtAuction"
  >("parcel");
  const listings = useQuery(parcelListingsQuery(entry.id));
  const listingCount = listings.data?.total ?? 0;
  const publicAuction = useQuery(parcelPublicAuctionQuery(entry.id));
  const publicAuctionCount = publicAuction.data?.length ?? 0;
  const courtAuction = useQuery(parcelCourtAuctionQuery(entry.id));
  const courtAuctionCount = courtAuction.data?.rounds.length ?? 0;
  const counts = {
    listings: listingCount,
    publicAuction: publicAuctionCount,
    courtAuction: courtAuctionCount,
  };
  const categories = [
    { key: "parcel" as const, label: t("categories.parcel"), count: null },
    ...(["listings", "publicAuction", "courtAuction"] as const)
      .filter((key) => counts[key] > 0)
      .map((key) => ({ key, label: t(`categories.${key}`), count: counts[key] })),
  ];
  // Each non-parcel category is one page of its own data, once that data has loaded.
  const pages: Record<Exclude<typeof category, "parcel">, ReactNode> = {
    listings: listings.data && <ParcelListingsCard entry={entry} data={listings.data} />,
    publicAuction: publicAuction.data && <ParcelPublicAuctionCard rounds={publicAuction.data} />,
    courtAuction: courtAuction.data && <ParcelCourtAuctionCard data={courtAuction.data} />,
  };
  const page = category === "parcel" ? null : pages[category];
  const parcelSections = useParcelSections(entry, data);
  const sections: Section[] =
    category !== "parcel" && page
      ? [{ id: category, label: t(`categories.${category}`), content: page }]
      : parcelSections;

  const scrollRef = useRef<HTMLDivElement>(null);
  const active = useScrollSpy(scrollRef, sections.map((s) => s.id).join(","));
  const jump = useCallback((id: string) => {
    const scroller = scrollRef.current;
    const target = scroller?.querySelector<HTMLElement>(`[data-section="${id}"]`);
    if (scroller && target) scroller.scrollTo({ top: target.offsetTop });
  }, []);

  return (
    <div className="flex min-h-0 flex-1 flex-col" data-testid="parcel-panel">
      <ParcelHeader pnu={entry.id} data={data} />
      {sections.length > 1 && (
        <nav
          aria-label={t("anchorsLabel")}
          className="flex gap-1 overflow-x-auto border-b border-[var(--color-hairline)] px-3"
        >
          {sections.map((section) => (
            <button
              key={section.id}
              type="button"
              aria-current={active === section.id ? "true" : undefined}
              onClick={() => jump(section.id)}
              className={`whitespace-nowrap border-b-2 px-3 py-2 text-[length:var(--text-body-sm)] ${
                active === section.id
                  ? "border-[var(--color-ink)] font-semibold text-[var(--color-ink)]"
                  : "border-transparent text-[var(--color-muted)]"
              }`}
            >
              {section.label}
            </button>
          ))}
        </nav>
      )}
      <div ref={scrollRef} className="relative min-h-0 flex-1 overflow-y-auto">
        {sections.map((section) => (
          <section
            key={section.id}
            data-section={section.id}
            aria-label={section.label}
            className="border-b-8 border-[var(--color-surface-soft)]"
          >
            {section.content}
          </section>
        ))}
        {category === "parcel" && <ParcelFooter pnu={entry.id} />}
      </div>
      {categories.length > 1 && (
        <nav
          aria-label={t("categoriesLabel")}
          className="grid border-t border-[var(--color-hairline)] bg-[var(--color-canvas)]"
          style={{ gridTemplateColumns: `repeat(${categories.length}, minmax(0, 1fr))` }}
        >
          {categories.map((c) => (
            <button
              key={c.key}
              type="button"
              aria-pressed={category === c.key}
              onClick={() => {
                setCategory(c.key);
                scrollRef.current?.scrollTo({ top: 0 });
              }}
              className={`border-t-2 py-2.5 text-[length:var(--text-caption)] ${
                category === c.key
                  ? "border-[var(--color-ink)] font-semibold text-[var(--color-ink)]"
                  : "border-transparent text-[var(--color-muted)]"
              }`}
            >
              {c.label}
              {c.count != null && ` ${c.count.toLocaleString("ko-KR")}`}
            </button>
          ))}
        </nav>
      )}
    </div>
  );
}

/** The section the reader is in: the last one whose top has passed the scroller's top. */
function useScrollSpy(ref: React.RefObject<HTMLDivElement | null>, ids: string): string | null {
  const [active, setActive] = useState<string | null>(null);
  // biome-ignore lint/correctness/useExhaustiveDependencies: `ids` re-arms the spy when a category's sections change; the effect reads them from the DOM.
  useEffect(() => {
    const scroller = ref.current;
    if (!scroller) return;
    const update = () => {
      const nodes = [...scroller.querySelectorAll<HTMLElement>("[data-section]")];
      if (nodes.length === 0) return setActive(null);
      const top = scroller.scrollTop + 8;
      let current = nodes[0];
      // The scroller is the sections' offset parent (`relative`): offsetTop is measured from its top.
      for (const node of nodes) if (node.offsetTop <= top) current = node;
      const scrolls = scroller.scrollHeight > scroller.clientHeight;
      if (scrolls && scroller.scrollTop + scroller.clientHeight >= scroller.scrollHeight - 2) {
        current = nodes.at(-1);
      }
      setActive(current?.dataset.section ?? null);
    };
    update();
    scroller.addEventListener("scroll", update, { passive: true });
    return () => scroller.removeEventListener("scroll", update);
  }, [ref, ids]);
  return active;
}

function ParcelHeader({ pnu, data }: { pnu: string; data: ParcelDocument }) {
  const t = useTranslations("panels.parcel.summary");
  const jibun = jibunOf(pnu, t("mountain"));
  const dong = data.legal_dong;
  const area = data.characteristics?.area_m2;
  const zone = data.zonings?.find((z) => z.inclusion_code !== "2") ?? data.zonings?.[0];
  const chips = [
    zone?.zone_name,
    data.characteristics?.land_category,
    area != null ? `${formatAreaM2(area)} · ${formatAreaPyeong(area)}` : null,
    data.characteristics?.land_use_situation,
  ].filter((chip): chip is string => Boolean(chip));
  return (
    <header className="flex flex-col gap-1.5 border-b border-[var(--color-hairline)] px-5 pt-5 pb-3">
      <div className="text-[length:var(--text-caption)] text-[var(--color-muted)]">
        {dong ? dong.name : t("legalDongCode", { code: pnu.slice(0, 10) })}
      </div>
      <h2 className="text-[length:var(--text-title-lg)] font-semibold text-[var(--color-ink)]">
        {dong?.lowest_name ? `${dong.lowest_name} ${jibun}` : jibun}
      </h2>
      {chips.length > 0 && (
        <ul className="flex flex-wrap gap-1.5">
          {chips.map((chip, i) => (
            <li
              key={chip}
              className={`rounded-full border px-2.5 py-0.5 text-[length:var(--text-caption)] ${
                i === 0 && zone?.zone_name === chip
                  ? "border-[var(--color-primary)] text-[var(--color-primary-active)]"
                  : "border-[var(--color-hairline)] text-[var(--color-body)]"
              }`}
            >
              {chip}
            </li>
          ))}
        </ul>
      )}
      {data.price && (
        <div className="text-[length:var(--text-caption)] text-[var(--color-muted)]">
          {t("priceLine", {
            value: data.price.price_per_m2.toLocaleString("ko-KR"),
            year: data.price.base_year,
            month: data.price.base_month,
          })}
        </div>
      )}
    </header>
  );
}

/** 필지정보's sections, each only when the parcel has something to show in it. */
function useParcelSections(entry: Entry, data: ParcelDocument): Section[] {
  const t = useTranslations("panels.parcel.summary");
  const buildings = useQuery(parcelBuildingsQuery(entry.id));
  const sections: Section[] = [
    { id: "land", label: t("sections.land"), content: <LandSection data={data} /> },
  ];
  const buildingList = buildings.data?.buildings ?? [];
  const landRights = data.land_right_total ?? 0;
  if (buildingList.length > 0 || landRights > 0) {
    sections.push({
      id: "buildings",
      label: t("sections.buildings"),
      content: <BuildingsSection entry={entry} buildings={buildingList} landRights={landRights} />,
    });
  }
  if ((data.zonings?.length ?? 0) > 0) {
    sections.push({
      id: "rules",
      label: t("sections.rules"),
      content: <RulesSection data={data} />,
    });
  }
  return sections;
}

function Group({ title, note, children }: { title: string; note?: string; children: ReactNode }) {
  return (
    <div className="flex flex-col gap-2 px-5 py-4">
      <h3 className="flex items-baseline justify-between gap-2 text-[length:var(--text-body-sm)] font-semibold text-[var(--color-ink)]">
        {title}
        {note && (
          <span className="font-normal text-[length:var(--text-caption)] text-[var(--color-muted)]">
            {note}
          </span>
        )}
      </h3>
      {children}
    </div>
  );
}

function Rows({ rows }: { rows: [string, ReactNode][] }) {
  return (
    <dl className="grid grid-cols-[7.5em_1fr] gap-x-3 gap-y-1.5 text-[length:var(--text-body-sm)]">
      {rows.map(([label, value]) => (
        <div key={label} className="contents">
          <dt className="text-[var(--color-muted)]">{label}</dt>
          <dd className="text-[var(--color-ink)] tabular-nums">{value}</dd>
        </div>
      ))}
    </dl>
  );
}

type Translate = ReturnType<typeof useTranslations<"panels.parcel.summary">>;

/** 토지특성's six fields in the source's order, "-" where it says nothing. */
function characteristicRows(
  t: Translate,
  data: ParcelDocument,
  dash: string,
): [string, ReactNode][] {
  const c = data.characteristics;
  const area = c?.area_m2;
  return [
    [t("land.category"), c?.land_category ?? dash],
    [t("land.area"), area != null ? `${formatAreaM2(area)} (${formatAreaPyeong(area)})` : dash],
    [t("land.useSituation"), c?.land_use_situation ?? dash],
    [t("land.terrainHeight"), c?.terrain_height ?? dash],
    [t("land.terrainShape"), c?.terrain_shape ?? dash],
    [t("land.roadContact"), c?.road_contact ?? dash],
  ];
}

function LandSection({ data }: { data: ParcelDocument }) {
  const t = useTranslations("panels.parcel.summary");
  // 원천이 말하지 않은 값은 "-" — 지어내지 않는다 (root ADR-0070/0078).
  const dash = t("none");
  const area = data.characteristics?.area_m2;
  const price = data.price;
  const moves = [...(data.transfer_history ?? [])].sort((a, b) =>
    (b.moved_at ?? "").localeCompare(a.moved_at ?? ""),
  );
  return (
    <div>
      <Group title={t("land.characteristics")} note={t("land.characteristicsSource")}>
        <Rows rows={characteristicRows(t, data, dash)} />
      </Group>
      <Group
        title={t("land.price")}
        note={
          price
            ? t("land.priceBase", {
                year: price.base_year,
                month: price.base_month,
                announced: price.announced_date ?? dash,
              })
            : undefined
        }
      >
        {price ? (
          <Rows
            rows={[
              [t("land.pricePerM2"), `${price.price_per_m2.toLocaleString("ko-KR")}${UNITS.krw}`],
              ...(area != null
                ? ([[t("land.priceTotal"), formatPriceKrw(price.price_per_m2 * area)]] as [
                    string,
                    ReactNode,
                  ][])
                : []),
            ]}
          />
        ) : (
          <p className="text-[length:var(--text-body-sm)] text-[var(--color-muted)]">
            {t("land.noPrice")}
          </p>
        )}
      </Group>
      {data.forest_ledger && (
        <Group title={t("land.ownership")} note={t("land.ownershipSource")}>
          <Rows
            rows={[
              [t("land.ownershipKind"), data.forest_ledger.ownership_kind_name ?? dash],
              [
                t("land.coOwners"),
                data.forest_ledger.co_owner_count != null
                  ? t("land.coOwnerCount", { count: data.forest_ledger.co_owner_count })
                  : dash,
              ],
            ]}
          />
        </Group>
      )}
      <Group title={t("land.moves")} note={moves.length > 0 ? t("land.movesNote") : undefined}>
        {moves.length > 0 ? (
          <ol className="flex flex-col gap-1.5 text-[length:var(--text-body-sm)]">
            {moves.map((move, i) => (
              <li
                // biome-ignore lint/suspicious/noArrayIndexKey: two moves may share a date and a reason; their order is the identity.
                key={i}
                className="grid grid-cols-[6.5em_1fr] gap-3"
              >
                <time className="text-[var(--color-muted)] tabular-nums">
                  {move.moved_at ?? dash}
                </time>
                <span className="text-[var(--color-ink)]">{move.reason ?? dash}</span>
              </li>
            ))}
          </ol>
        ) : (
          <p className="text-[length:var(--text-body-sm)] text-[var(--color-muted)]">
            {t("land.noMoves")}
          </p>
        )}
      </Group>
    </div>
  );
}

function BuildingsSection({
  entry,
  buildings,
  landRights,
}: {
  entry: Entry;
  buildings: Building[];
  landRights: number;
}) {
  const t = useTranslations("panels.parcel.summary");
  const { push } = usePanelStack();
  return (
    <div>
      {buildings.length > 0 && (
        <Group
          title={t("buildings.title", { count: buildings.length })}
          note={t("buildings.source")}
        >
          <ul className="flex flex-col gap-2">
            {buildings.map((b) => (
              <li
                key={b.id}
                className="rounded-md border border-[var(--color-hairline)] px-3 py-2 text-[length:var(--text-body-sm)]"
              >
                <div className="font-semibold text-[var(--color-ink)]">
                  {[b.name, b.purpose].filter(Boolean).join(" · ") || t("buildings.fallbackName")}
                </div>
                <div className="text-[var(--color-muted)]">
                  {[
                    b.structure,
                    b.total_area_m2 != null
                      ? t("buildings.floorArea", { area: formatAreaM2(b.total_area_m2) })
                      : null,
                    b.stories != null
                      ? t("buildings.stories", {
                          above: b.stories,
                          below: b.below_ground_floors ?? 0,
                        })
                      : null,
                    b.built_year != null ? t("buildings.builtYear", { year: b.built_year }) : null,
                  ]
                    .filter(Boolean)
                    .join(" · ")}
                </div>
              </li>
            ))}
          </ul>
          <div className="flex gap-2">
            <button
              type="button"
              onClick={() => push({ kind: "parcel", id: entry.id, view: "buildings" })}
              className="rounded-md border border-[var(--color-hairline)] px-3 py-1.5 text-[length:var(--text-caption)] hover:bg-[var(--color-surface-cream-strong)]"
            >
              {t("buildings.units")} ›
            </button>
            <button
              type="button"
              onClick={() => push({ kind: "parcel", id: entry.id, view: "floors" })}
              className="rounded-md border border-[var(--color-hairline)] px-3 py-1.5 text-[length:var(--text-caption)] hover:bg-[var(--color-surface-cream-strong)]"
            >
              {t("buildings.floors")} ›
            </button>
          </div>
        </Group>
      )}
      {landRights > 0 && (
        <Group title={t("buildings.landRights")} note={t("buildings.landRightsSource")}>
          <p className="text-[length:var(--text-body-sm)] text-[var(--color-ink)]">
            {t("buildings.landRightCount", { count: landRights })}
          </p>
        </Group>
      )}
    </div>
  );
}

function RulesSection({ data }: { data: ParcelDocument }) {
  const t = useTranslations("panels.parcel.summary");
  return (
    <Group title={t("rules.zoning")} note={t("rules.zoningSource")}>
      <ul className="flex flex-col gap-1.5 text-[length:var(--text-body-sm)]">
        {(data.zonings ?? []).map((zone) => (
          <li key={zone.zone_code} className="flex items-center justify-between gap-2">
            <span className="text-[var(--color-ink)]">{zone.zone_name}</span>
            {(zone.inclusion_code === "1" || zone.inclusion_code === "2") && (
              <span className="rounded border border-[var(--color-hairline)] px-1.5 text-[length:var(--text-caption)] text-[var(--color-muted)]">
                {zone.inclusion_code === "1" ? t("rules.included") : t("rules.conflicting")}
              </span>
            )}
          </li>
        ))}
      </ul>
    </Group>
  );
}

function ParcelFooter({ pnu }: { pnu: string }) {
  const t = useTranslations("panels.parcel.summary");
  const [copied, setCopied] = useState(false);
  return (
    <footer className="flex flex-col gap-1 bg-[var(--color-surface-soft)] px-5 py-4 text-[length:var(--text-caption)] text-[var(--color-muted)]">
      <div className="flex items-center gap-2">
        <span className="font-mono">PNU {pnu}</span>
        <button
          type="button"
          onClick={() => {
            navigator.clipboard
              ?.writeText(pnu)
              .then(() => setCopied(true))
              .catch(() => undefined);
          }}
          className="rounded border border-[var(--color-hairline)] px-1.5 hover:bg-[var(--color-surface-cream-strong)]"
        >
          {copied ? t("copied") : t("copy")}
        </button>
      </div>
      <div>{t("sources")}</div>
      <div>{t("notice")}</div>
    </footer>
  );
}
