// apps/web/components/panels/parcel/summary.tsx
"use client";
import { useQuery } from "@tanstack/react-query";
import { groupBy } from "es-toolkit";
import { useTranslations } from "next-intl";
import { type ReactNode, useCallback, useEffect, useRef, useState } from "react";
import {
  type BuildingPermits,
  hasPermitContent,
  PERMIT_DETAIL_DATES,
  PERMIT_DETAILS,
  type PermitDetailKey,
  type PermitDetailRecord,
  permitDetail,
} from "@/lib/api/building-permits";
import type { Building } from "@/lib/api/buildings";
import { courtAuctionTotal } from "@/lib/api/court-auction";
import { type Demolitions, dayLabel, reportDates } from "@/lib/api/demolitions";
import { chronological, type Energy, electricityPeak, monthLabel } from "@/lib/api/energy";
import type { Factories } from "@/lib/api/factories";
import { latestChange, type Ownership, ownerKinds } from "@/lib/api/ownership";
import { jibunOf, type ParcelDocument } from "@/lib/api/parcels";
import type { RealTransaction } from "@/lib/api/real-transaction";
import { filled, type RegisterMasters } from "@/lib/api/register-masters";
import type { RegulationZone, RegulationZones } from "@/lib/api/regulation-zones";
import { jibunLabel, type SubParcelLink, type SubParcels } from "@/lib/api/sub-parcels";
import { formatAreaM2, formatAreaPyeong, formatPriceKrw, UNITS } from "@/lib/listings/format";
import {
  ZONING_FAMILIES,
  ZONING_PENDING_LAYER_CODES,
  zoningFamilyOf,
} from "@/lib/map/zoning-families";
import type { PanelStackEntry } from "@/lib/panel/types";
import { usePanelStack } from "@/lib/panel/use-panel-stack";
import { ParcelCourtAuctionCard } from "./court-auction";
import { ParcelListingsCard } from "./listings";
import { ParcelPublicAuctionCard } from "./public-auction";
import {
  parcelBuildingPermitsQuery,
  parcelBuildingsQuery,
  parcelCourtAuctionQuery,
  parcelDemolitionsQuery,
  parcelEnergyQuery,
  parcelFactoriesQuery,
  parcelListingsQuery,
  parcelOwnershipQuery,
  parcelPublicAuctionQuery,
  parcelRealTransactionQuery,
  parcelRegisterMastersQuery,
  parcelRegulationZonesQuery,
  parcelSubParcelsQuery,
} from "./queries";
import { ParcelComparablesCard, ParcelRealTransactionsCard } from "./real-transaction";

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
  const realTransactionText = useTranslations("panels.parcel.realTransaction");
  const [category, setCategory] = useState<
    "parcel" | "listings" | "publicAuction" | "courtAuction" | "realTransaction"
  >("parcel");
  const listings = useQuery(parcelListingsQuery(entry.id));
  const listingCount = listings.data?.total ?? 0;
  const publicAuction = useQuery(parcelPublicAuctionQuery(entry.id));
  const publicAuctionCount = publicAuction.data?.length ?? 0;
  // A parcel the 실거래 lane has no document for has no 실거래 category (root ADR-0350 §5).
  const realTransaction = useQuery(parcelRealTransactionQuery(entry.id));
  const courtAuction = useQuery(parcelCourtAuctionQuery(entry.id));
  const courtAuctionCount = courtAuction.data ? courtAuctionTotal(courtAuction.data) : 0;
  const categories = [
    { key: "parcel" as const, label: t("categories.parcel"), count: null },
    ...(listingCount > 0
      ? [{ key: "listings" as const, label: t("categories.listings"), count: listingCount }]
      : []),
    ...(publicAuctionCount > 0
      ? [
          {
            key: "publicAuction" as const,
            label: t("categories.publicAuction"),
            count: publicAuctionCount,
          },
        ]
      : []),
    ...(courtAuctionCount > 0
      ? [
          {
            key: "courtAuction" as const,
            label: t("categories.courtAuction"),
            count: courtAuctionCount,
          },
        ]
      : []),
    ...(realTransaction.data
      ? [
          {
            key: "realTransaction" as const,
            label: t("categories.realTransaction"),
            count: realTransaction.data.transactionTotal,
          },
        ]
      : []),
  ];
  const parcelSections = useParcelSections(entry, data);
  // Each category's page once its read has data; 필지정보 otherwise.
  const pages: Record<typeof category, Section[] | null | undefined> = {
    parcel: parcelSections,
    listings: listings.data && [
      {
        id: "listings",
        label: t("categories.listings"),
        content: <ParcelListingsCard entry={entry} data={listings.data} />,
      },
    ],
    publicAuction: publicAuction.data && [
      {
        id: "publicAuction",
        label: t("categories.publicAuction"),
        content: <ParcelPublicAuctionCard rounds={publicAuction.data} />,
      },
    ],
    courtAuction: courtAuction.data && [
      {
        id: "courtAuction",
        label: t("categories.courtAuction"),
        content: <ParcelCourtAuctionCard data={courtAuction.data} />,
      },
    ],
    realTransaction:
      realTransaction.data && realTransactionSections(realTransactionText, realTransaction.data),
  };
  const sections: Section[] = pages[category] ?? parcelSections;

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

/** The 실거래 category's page: the parcel's own deals, then its 법정동's 공장·창고 deals when it carries them. */
function realTransactionSections(
  t: ReturnType<typeof useTranslations>,
  data: RealTransaction,
): Section[] {
  return [
    {
      id: "realTransactionOwn",
      label: t("ownSection"),
      content: <ParcelRealTransactionsCard data={data} />,
    },
    ...(data.comparables.length > 0
      ? [
          {
            id: "realTransactionComparables",
            label: t("comparablesSection"),
            content: <ParcelComparablesCard data={data} />,
          },
        ]
      : []),
  ];
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
  const ownership = useQuery(parcelOwnershipQuery(entry.id));
  const subParcels = useQuery(parcelSubParcelsQuery(entry.id));
  const energy = useQuery(parcelEnergyQuery(entry.id));
  const demolitions = useQuery(parcelDemolitionsQuery(entry.id));
  const factories = useQuery(parcelFactoriesQuery(entry.id));
  const registerMasters = useQuery(parcelRegisterMastersQuery(entry.id));
  const buildingPermits = useQuery(parcelBuildingPermitsQuery(entry.id));
  const sections: Section[] = [
    {
      id: "land",
      label: t("sections.land"),
      content: <LandSection data={data} ownership={ownership.data ?? null} />,
    },
  ];
  sections.push(...factoriesSections(factories.data, t("sections.factories")));
  const buildingList = buildings.data?.buildings ?? [];
  const landRights = data.land_right_total ?? 0;
  if (buildingList.length > 0 || landRights > 0) {
    sections.push({
      id: "buildings",
      label: t("sections.buildings"),
      content: <BuildingsSection entry={entry} buildings={buildingList} landRights={landRights} />,
    });
  }
  sections.push(...registerMastersSections(registerMasters.data, t("sections.registerMasters")));
  sections.push(...buildingPermitsSections(buildingPermits.data, t("sections.buildingPermits")));
  const links = subParcels.data;
  if (links && links.on_other_total + links.from_other_total > 0) {
    sections.push({
      id: "subParcels",
      label: t("sections.subParcels"),
      content: <SubParcelsSection links={links} />,
    });
  }
  const mapZones = useQuery(parcelRegulationZonesQuery(entry.id)).data ?? null;
  if (energy.data && energy.data.months.length > 0) {
    sections.push({
      id: "energy",
      label: t("sections.energy"),
      content: <EnergySection energy={energy.data} />,
    });
  }
  if (demolitions.data && demolitions.data.records.length > 0) {
    sections.push({
      id: "demolitions",
      label: t("sections.demolitions"),
      content: <DemolitionsSection demolitions={demolitions.data} />,
    });
  }
  if ((data.zonings?.length ?? 0) > 0 || mapZones !== null) {
    sections.push({
      id: "rules",
      label: t("sections.rules"),
      content: <RulesSection data={data} mapZones={mapZones} />,
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

/**
 * 소유 from the 토지소유 lane (root ADR-0361): the parcel's own owners counted by 소유구분, how many
 * co-owners, the newest change, where the owners live relative to the land, and for a collective
 * building on it how many unit owners of each 소유구분. Every value is the provider's word; a field
 * the source leaves out is "-" or not shown.
 */
function OwnershipGroup({ ownership }: { ownership: Ownership }) {
  const t = useTranslations("panels.parcel.summary");
  const dash = t("none");
  const kinds = ownerKinds(ownership)
    .map(([kind, count]) => (count > 1 ? `${kind ?? dash} ${count}` : (kind ?? dash)))
    .join(" · ");
  const latest = latestChange(ownership);
  const residences = [
    ...new Set(ownership.owners.map((o) => o.residence_kind_name).filter((r): r is string => !!r)),
  ];
  const units = ownership.unit_owner_kinds
    .map((k) => `${k.ownership_kind_name ?? dash} ${k.count.toLocaleString("ko-KR")}`)
    .join(" · ");
  const rows: [string, ReactNode][] = [];
  if (ownership.owners.length > 0) {
    rows.push([t("land.ownerKinds"), kinds]);
    rows.push([t("land.ownerCount"), t("land.coOwnerCount", { count: ownership.ownerTotal })]);
    rows.push([
      t("land.latestChange"),
      latest
        ? `${latest.ownership_changed_on} ${latest.ownership_change_reason ?? ""}`.trim()
        : dash,
    ]);
    rows.push([t("land.residence"), residences.length > 0 ? residences.join(" · ") : dash]);
  }
  if (ownership.unit_owner_total > 0) {
    rows.push([
      t("land.unitOwners"),
      `${units} (${t("land.unitOwnerCount", { count: ownership.unit_owner_total.toLocaleString("ko-KR") })})`,
    ]);
  }
  if (rows.length === 0) return null;
  return (
    <Group
      title={t("land.ownership")}
      note={
        ownership.base_year_month
          ? t("land.ownershipLaneSource", { month: ownership.base_year_month })
          : t("land.ownershipLaneSourceUndated")
      }
    >
      <Rows rows={rows} />
    </Group>
  );
}

function LandSection({ data, ownership }: { data: ParcelDocument; ownership: Ownership | null }) {
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
      {ownership ? (
        <OwnershipGroup ownership={ownership} />
      ) : (
        data.forest_ledger && (
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
        )
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

/**
 * 부속지번 from the 부속지번 lane (root ADR-0399), both ways: the other parcels a register on this 대지
 * also stands on, and the registers on another 대지 that also stand here. Each names the register's
 * 대장종류 and the other parcel's 지번, and opens that parcel when Foundation knows its PNU.
 */
function SubParcelsSection({ links }: { links: SubParcels }) {
  const t = useTranslations("panels.parcel.summary");
  const { push } = usePanelStack();
  const list = (entries: SubParcelLink[], total: number) => (
    <ul className="flex flex-col gap-1.5">
      {entries.map((link) => (
        <li
          key={[link.mgmt_key, link.other_pnu, link.other_jibun].join("|")}
          className="flex items-center justify-between gap-2 text-[length:var(--text-body-sm)]"
        >
          <span className="text-[var(--color-ink)]">
            {[jibunLabel(link.other_jibun), link.register_kind_name].filter(Boolean).join(" · ")}
          </span>
          {link.other_pnu && (
            <button
              type="button"
              onClick={() =>
                push({ kind: "parcel", id: link.other_pnu as string, view: "summary" })
              }
              className="rounded-md border border-[var(--color-hairline)] px-2 py-0.5 text-[length:var(--text-caption)] hover:bg-[var(--color-surface-cream-strong)]"
            >
              {t("subParcels.open")} ›
            </button>
          )}
        </li>
      ))}
      {total > entries.length && (
        <li className="text-[length:var(--text-caption)] text-[var(--color-muted)]">
          {t("subParcels.more", { shown: entries.length, total })}
        </li>
      )}
    </ul>
  );
  return (
    <div>
      {links.on_other_total > 0 && (
        <Group
          title={t("subParcels.onOther", { count: links.on_other_total })}
          note={t("subParcels.source")}
        >
          {list(links.on_other_parcels, links.on_other_total)}
        </Group>
      )}
      {links.from_other_total > 0 && (
        <Group
          title={t("subParcels.fromOther", { count: links.from_other_total })}
          note={t("subParcels.source")}
        >
          {list(links.from_other_parcels, links.from_other_total)}
        </Group>
      )}
    </div>
  );
}

/**
 * 건물 에너지 from the 월별 전기·가스 lane (root ADR-0398): the 지번's newest twelve months, oldest
 * first, 전기 in the provider's kWh with a bar against the window's largest month and 가스 as the
 * source's 사용량 (no unit is stated). A month without a row of an energy is "-", never 0. The
 * provider's own exclusions (산업·수송·발전 uses, houses) are said beside the numbers: a factory's
 * 산업용 meter is not in these files.
 */
function EnergySection({ energy }: { energy: Energy }) {
  const t = useTranslations("panels.parcel.summary");
  const dash = t("none");
  const peak = electricityPeak(energy);
  const number = (value: number | null | undefined) =>
    value == null ? dash : value.toLocaleString("ko-KR", { maximumFractionDigits: 3 });
  return (
    <Group
      title={t("energy.title")}
      note={
        energy.latest_use_ym
          ? t("energy.source", { month: monthLabel(energy.latest_use_ym) })
          : t("energy.sourceUndated")
      }
    >
      <table className="w-full text-[length:var(--text-body-sm)] tabular-nums">
        <thead className="text-[var(--color-muted)]">
          <tr>
            <th scope="col" className="text-left font-normal">
              {t("energy.month")}
            </th>
            <th scope="col" className="text-right font-normal">
              {t("energy.electricity", { unit: energy.electricity_unit })}
            </th>
            <th scope="col" className="text-right font-normal">
              {t("energy.gas")}
            </th>
          </tr>
        </thead>
        <tbody>
          {chronological(energy).map((month) => (
            <tr key={month.use_ym}>
              <th scope="row" className="text-left font-normal text-[var(--color-muted)]">
                {monthLabel(month.use_ym)}
              </th>
              <td className="text-right text-[var(--color-ink)]">
                <span className="inline-flex items-center justify-end gap-2">
                  {peak > 0 && month.electricity_usage != null && (
                    <span
                      aria-hidden="true"
                      className="inline-block h-1.5 rounded-sm bg-[var(--color-ink)] opacity-40"
                      style={{ width: `${Math.max(2, (month.electricity_usage / peak) * 48)}px` }}
                    />
                  )}
                  {number(month.electricity_usage)}
                </span>
              </td>
              <td className="text-right text-[var(--color-ink)]">{number(month.gas_usage)}</td>
            </tr>
          ))}
        </tbody>
      </table>
      <p className="text-[length:var(--text-caption)] text-[var(--color-muted)]">
        {t("energy.exclusions")}
      </p>
    </Group>
  );
}

/** The 등록공장 section, only for a parcel with a registered factory. */
function factoriesSections(factories: Factories | null | undefined, label: string): Section[] {
  if (!factories || factories.records.length === 0) return [];
  return [{ id: "factories", label, content: <FactoriesSection factories={factories} /> }];
}

/**
 * 등록공장 from the registered-factory lane (root ADR-0449): the factories whose 공장주소 is this parcel,
 * each in the provider's words (회사명, 단지명, 생산품). A value the source leaves out is not shown.
 */
function FactoriesSection({ factories }: { factories: Factories }) {
  const t = useTranslations("panels.parcel.summary");
  return (
    <Group title={t("factories.title", { count: factories.total })}>
      <ul className="flex flex-col gap-2">
        {factories.records.map((record) => (
          <li
            key={[record.company_name, record.complex_name, record.products].join("|")}
            className="rounded-md border border-[var(--color-hairline)] px-3 py-2 text-[length:var(--text-body-sm)]"
          >
            <div className="font-semibold text-[var(--color-ink)]">
              {record.company_name ?? t("none")}
            </div>
            <div className="text-[var(--color-muted)]">
              {[
                record.complex_name ? t("factories.complex", { name: record.complex_name }) : null,
                record.products ? t("factories.products", { products: record.products }) : null,
              ]
                .filter(Boolean)
                .join(" · ")}
            </div>
          </li>
        ))}
      </ul>
      {factories.total > factories.records.length && (
        <p className="text-[length:var(--text-caption)] text-[var(--color-muted)]">
          {t("factories.more", { shown: factories.records.length, total: factories.total })}
        </p>
      )}
    </Group>
  );
}

/** The 총괄표제부 section, only for a parcel with a site of several buildings. */
function registerMastersSections(
  masters: RegisterMasters | null | undefined,
  label: string,
): Section[] {
  if (!masters || masters.records.length === 0) return [];
  return [{ id: "registerMasters", label, content: <RegisterMastersSection masters={masters} /> }];
}

/**
 * 총괄표제부 from the register-master lane (root ADR-0450): each site's totals in the register's own
 * words (주용도, 대지·건축·연면적, 주·부속건축물수, 총주차수, 허가·착공·사용승인일), newest first.
 * A figure the register leaves empty or 0 is not shown.
 */
function RegisterMastersSection({ masters }: { masters: RegisterMasters }) {
  const t = useTranslations("panels.parcel.summary");
  // Each line is its labelled fields in the register's order; a field left out is not shown.
  const line = (fields: [string, string | null | undefined][], keep: (v: string) => boolean) =>
    fields
      .filter((field): field is [string, string] => !!field[1] && keep(field[1]))
      .map(([key, value]) => t(`registerMasters.${key}`, { value }))
      .join(" · ");
  return (
    <Group title={t("registerMasters.title", { count: masters.total })}>
      <ul className="flex flex-col gap-2">
        {masters.records.map((record) => (
          <li
            key={[
              record.main_purpose_name,
              record.total_floor_area_m2,
              record.permit_date,
              record.start_date,
              record.use_approval_date,
              record.plot_area_m2,
            ].join("|")}
            className="rounded-md border border-[var(--color-hairline)] px-3 py-2 text-[length:var(--text-body-sm)]"
          >
            <div className="font-semibold text-[var(--color-ink)]">
              {record.main_purpose_name ?? t("none")}
            </div>
            <div className="text-[var(--color-muted)]">
              {line(
                [
                  ["plotArea", record.plot_area_m2],
                  ["buildingArea", record.building_area_m2],
                  ["floorArea", record.total_floor_area_m2],
                ],
                filled,
              )}
            </div>
            <div className="text-[var(--color-muted)]">
              {line(
                [
                  ["mainBuildings", record.main_building_count],
                  ["annexBuildings", record.annex_building_count],
                  ["parking", record.parking_count],
                ],
                filled,
              )}
            </div>
            <div className="text-[var(--color-muted)]">
              {line(
                [
                  ["permitDate", record.permit_date && dayLabel(record.permit_date)],
                  ["startDate", record.start_date && dayLabel(record.start_date)],
                  [
                    "useApprovalDate",
                    record.use_approval_date && dayLabel(record.use_approval_date),
                  ],
                ],
                () => true,
              )}
            </div>
          </li>
        ))}
      </ul>
      {masters.total > masters.records.length && (
        <p className="text-[length:var(--text-caption)] text-[var(--color-muted)]">
          {t("registerMasters.more", { shown: masters.records.length, total: masters.total })}
        </p>
      )}
    </Group>
  );
}

/** The 건축인허가 section, only for a parcel with a building permit or a row of another permit file. */
function buildingPermitsSections(
  permits: BuildingPermits | null | undefined,
  label: string,
): Section[] {
  if (!permits || !hasPermitContent(permits)) return [];
  return [{ id: "buildingPermits", label, content: <BuildingPermitsSection permits={permits} /> }];
}

/**
 * 건축인허가 from the building-permit lane (root ADR-0451): each permit in the register's own words
 * (건축구분·주용도, 대지·건축·연면적, 건축허가·착공예정·착공연기·실제착공·사용승인일, and its 주차장
 * 옥내자주식·옥외자주식·옥내기계식대수, ADR-0452), newest first.
 * A figure the register leaves empty or 0 is not shown.
 */
function BuildingPermitsSection({ permits }: { permits: BuildingPermits }) {
  return (
    <>
      {permits.records.length > 0 && <PermitsGroup permits={permits} />}
      {PERMIT_DETAILS.map(({ key, head, fields }) => (
        <PermitDetailGroup key={key} permits={permits} detail={key} head={head} fields={fields} />
      ))}
    </>
  );
}

/**
 * One other file of the permit family on the parcel (root ADR-0464): its rows in the register's own
 * words, the heading fields unlabelled and the rest labelled. A figure left empty or 0 is not shown.
 */
function PermitDetailGroup({
  permits,
  detail,
  head,
  fields,
}: {
  permits: BuildingPermits;
  detail: PermitDetailKey;
  head: readonly string[];
  fields: readonly string[];
}) {
  const t = useTranslations("panels.parcel.summary");
  const listed = permitDetail(permits, detail);
  if (!listed) return null;
  const value = (record: PermitDetailRecord, field: string) => {
    const raw = record[field];
    return raw && PERMIT_DETAIL_DATES.has(field) ? dayLabel(raw) : raw;
  };
  const key = (record: PermitDetailRecord, index: number) =>
    [index, ...head.map((f) => record[f]), ...fields.map((f) => record[f])].join("|");
  return (
    <Group title={t(`buildingPermits.${detail}.title`, { count: listed.total })}>
      <ul className="flex flex-col gap-2">
        {listed.records.map((record, index) => {
          const labelled = fields
            .map((field) => [field, value(record, field)] as const)
            .filter((pair): pair is readonly [string, string] => filled(pair[1]))
            .map(([field, v]) => t(`buildingPermits.${detail}.${field}`, { value: v }))
            .join(" · ");
          return (
            <li
              key={key(record, index)}
              className="rounded-md border border-[var(--color-hairline)] px-3 py-2 text-[length:var(--text-body-sm)]"
            >
              <div className="font-semibold text-[var(--color-ink)]">
                {head
                  .map((f) => record[f])
                  .filter(Boolean)
                  .join(" · ") || t("none")}
              </div>
              {labelled && <div className="text-[var(--color-muted)]">{labelled}</div>}
            </li>
          );
        })}
      </ul>
      {listed.total > listed.records.length && (
        <p className="text-[length:var(--text-caption)] text-[var(--color-muted)]">
          {t("buildingPermits.more", { shown: listed.records.length, total: listed.total })}
        </p>
      )}
    </Group>
  );
}

/** The parcel's permits (기본개요), newest first. */
function PermitsGroup({ permits }: { permits: BuildingPermits }) {
  const t = useTranslations("panels.parcel.summary");
  // Each line is its labelled fields in the register's order; a field left out is not shown.
  const line = (fields: [string, string | null | undefined][], keep: (v: string) => boolean) =>
    fields
      .filter((field): field is [string, string] => !!field[1] && keep(field[1]))
      .map(([key, value]) => t(`buildingPermits.${key}`, { value }))
      .join(" · ");
  const day = (value: string | null | undefined) => value && dayLabel(value);
  return (
    <Group title={t("buildingPermits.title", { count: permits.total })}>
      <ul className="flex flex-col gap-2">
        {permits.records.map((record) => (
          <li
            key={[
              record.arch_kind_name,
              record.main_purpose_name,
              record.total_floor_area_m2,
              record.permit_date,
              record.start_actual_date,
              record.use_approval_date,
              record.plot_area_m2,
            ].join("|")}
            className="rounded-md border border-[var(--color-hairline)] px-3 py-2 text-[length:var(--text-body-sm)]"
          >
            <div className="font-semibold text-[var(--color-ink)]">
              {[record.arch_kind_name, record.main_purpose_name].filter(Boolean).join(" · ") ||
                t("none")}
            </div>
            <div className="text-[var(--color-muted)]">
              {line(
                [
                  ["plotArea", record.plot_area_m2],
                  ["buildingArea", record.building_area_m2],
                  ["floorArea", record.total_floor_area_m2],
                ],
                filled,
              )}
            </div>
            <div className="text-[var(--color-muted)]">
              {line(
                [
                  ["permitDate", day(record.permit_date)],
                  ["startScheduledDate", day(record.start_scheduled_date)],
                  ["startDelayedDate", day(record.start_delayed_date)],
                  ["startActualDate", day(record.start_actual_date)],
                  ["useApprovalDate", day(record.use_approval_date)],
                ],
                () => true,
              )}
            </div>
            <div className="text-[var(--color-muted)]">
              {line(
                [
                  ["indoorSelfParking", record.indoor_self_parking_count],
                  ["outdoorSelfParking", record.outdoor_self_parking_count],
                  ["indoorMechanicalParking", record.indoor_mechanical_parking_count],
                ],
                filled,
              )}
            </div>
          </li>
        ))}
      </ul>
      {permits.total > permits.records.length && (
        <p className="text-[length:var(--text-caption)] text-[var(--color-muted)]">
          {t("buildingPermits.more", { shown: permits.records.length, total: permits.total })}
        </p>
      )}
    </Group>
  );
}

/**
 * 멸실된 건물 from the 철거멸실 lane (root ADR-0399): the parcel's 철거·멸실 reports newest first, each
 * in the provider's words: 철거멸실구분, the date, the building's 주용도·구조·연면적(㎡)·건축물수.
 * A value the source leaves out is not shown.
 */
function DemolitionsSection({ demolitions }: { demolitions: Demolitions }) {
  const t = useTranslations("panels.parcel.summary");
  return (
    <Group
      title={t("demolitions.title", { count: demolitions.total })}
      note={t("demolitions.source")}
    >
      <ul className="flex flex-col gap-2">
        {demolitions.records.map((record) => (
          <li
            key={[
              record.demolition_kind_name,
              record.demolition_start_date,
              record.demolition_end_date,
              record.demolition_loss_date,
              record.main_purpose_name,
              record.total_floor_area_m2,
            ].join("|")}
            className="rounded-md border border-[var(--color-hairline)] px-3 py-2 text-[length:var(--text-body-sm)]"
          >
            <div className="font-semibold text-[var(--color-ink)]">
              {[record.demolition_kind_name, reportDates(record)].filter(Boolean).join(" · ")}
            </div>
            <div className="text-[var(--color-muted)]">
              {[
                record.main_purpose_name,
                record.structure_name,
                record.total_floor_area_m2
                  ? t("demolitions.floorArea", { area: record.total_floor_area_m2 })
                  : null,
                record.building_count
                  ? t("demolitions.buildingCount", { count: record.building_count })
                  : null,
              ]
                .filter(Boolean)
                .join(" · ")}
            </div>
          </li>
        ))}
      </ul>
      {demolitions.total > demolitions.records.length && (
        <p className="text-[length:var(--text-caption)] text-[var(--color-muted)]">
          {t("demolitions.more", { shown: demolitions.records.length, total: demolitions.total })}
        </p>
      )}
    </Group>
  );
}

function RulesSection({
  data,
  mapZones,
}: {
  data: ParcelDocument;
  mapZones: RegulationZones | null;
}) {
  const t = useTranslations("panels.parcel.summary");
  return (
    <div>
      {(data.zonings?.length ?? 0) > 0 && (
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
      )}
      {mapZones !== null && <MapZonesGroup zones={mapZones} />}
    </div>
  );
}

/**
 * 지도 규제 구역: the zones the Foundation found covering part of the parcel (root ADR-0375), by
 * family in the map toggles' order, each in the provider's layer and zone names, with the newest
 * release date among them; and which layers are still not collected.
 */
function MapZonesGroup({ zones }: { zones: RegulationZones }) {
  const t = useTranslations("panels.parcel.summary.rules");
  const tMap = useTranslations("map.zoning");
  const byFamily = groupBy(zones.zones, (zone) => zoningFamilyOf(zone.layer_code) ?? "other");
  const groups = [
    ...ZONING_FAMILIES.map(({ key }) => [tMap(`families.${key}`), byFamily[key]] as const),
    [t("otherFamily"), byFamily.other] as const,
  ].filter((group): group is readonly [string, RegulationZone[]] => (group[1]?.length ?? 0) > 0);
  const pending = ZONING_PENDING_LAYER_CODES.map((code) => tMap(`pendingLayers.${code}`)).join("·");
  const asOf = zones.zones
    .map((zone) => zone.release_date)
    .filter((date): date is string => !!date)
    .sort()
    .at(-1);
  const more = zones.zoneTotal - zones.zones.length;
  return (
    <Group title={t("mapZones")} note={asOf ? t("mapZonesAsOf", { date: asOf }) : undefined}>
      {groups.map(([label, list]) => (
        <div key={label} className="flex flex-col gap-1">
          <h4 className="text-[length:var(--text-caption)] text-[var(--color-muted)]">{label}</h4>
          <ul className="flex flex-col gap-1 text-[length:var(--text-body-sm)]">
            {list.map((zone) => (
              <li key={zone.zone_id} className="flex items-baseline justify-between gap-2">
                <span className="text-[var(--color-ink)]">
                  {zone.zone_name || zone.layer_name || zone.layer_code}
                </span>
                <span className="text-[length:var(--text-caption)] text-[var(--color-muted)]">
                  {zone.layer_name ?? zone.layer_code}
                </span>
              </li>
            ))}
          </ul>
        </div>
      ))}
      {more > 0 && (
        <p className="text-[length:var(--text-caption)] text-[var(--color-muted)]">
          {t("mapZonesMore", { count: more })}
        </p>
      )}
      <p className="text-[length:var(--text-caption)] text-[var(--color-muted)]">
        {t("pendingNote", { layers: pending })}
      </p>
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
      <div>{t("notice")}</div>
    </footer>
  );
}
