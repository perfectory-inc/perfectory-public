// apps/web/components/panels/complex/summary.tsx
"use client";
import { Badge } from "@gongzzang/ui";
import { useTranslations } from "next-intl";
import type { ComplexInfo } from "@/lib/api/complexes";
import type { PanelStackEntry } from "@/lib/panel/types";

/**
 * Wire values this card knows how to say in Korean.
 *
 * Explicit sets rather than `t(\`kind.${value}\`)`, because an unmapped wire value would otherwise
 * render as a message-key path. A value outside the set falls through to the source's own string:
 * showing what the source said is honest, inventing a label for it is not.
 */
const COMPLEX_KINDS = ["national", "general", "agricultural", "urban_high_tech"] as const;
const COMPLEX_STATUSES = [
  "planned",
  "developing",
  "operating",
  "changed",
  "abolished",
  "unknown",
] as const;
const LOT_SALES_STATUSES = ["planned", "in_progress", "completed"] as const;

function isKnown<T extends readonly string[]>(
  known: T,
  value: string,
): value is T[number] & string {
  return (known as readonly string[]).includes(value);
}

/** A value the source actually stated. Blank-but-present text is not a statement. */
function stated(value: string | null | undefined): string | undefined {
  if (value == null) return undefined;
  const trimmed = value.trim();
  return trimmed === "" ? undefined : trimmed;
}

function Row({ label, children }: { label: string; children: React.ReactNode }) {
  return (
    <>
      <dt className="text-[var(--color-muted)]">{label}</dt>
      <dd className="text-[var(--color-ink)]">{children}</dd>
    </>
  );
}

/** Renders a `dt`/`dd` pair only when the source stated the value. */
function TextRow({ label, value }: { label: string; value: string | null | undefined }) {
  const text = stated(value);
  if (text === undefined) return null;
  return <Row label={label}>{text}</Row>;
}

/**
 * A group of short facts, which disappears entirely when the source stated none of them.
 *
 * Without this an all-absent group leaves an empty `dl` behind, and the column gap around it reads
 * as a heading whose contents failed to load.
 */
function FactGroup({
  rows,
}: {
  rows: ReadonlyArray<{ label: string; value: string | null | undefined }>;
}) {
  if (!rows.some((row) => stated(row.value) !== undefined)) return null;
  return (
    <dl className="grid grid-cols-[auto_1fr] gap-x-4 gap-y-2 text-[length:var(--text-body-sm)]">
      {rows.map((row) => (
        <TextRow key={row.label} label={row.label} value={row.value} />
      ))}
    </dl>
  );
}

/**
 * A long free-text block, rendered as its own section rather than a table row.
 *
 * These are sentences, sometimes several, in the source's own words. Putting them in the two-column
 * `dl` above would push every short fact off the first screen.
 */
function ProseBlock({ label, value }: { label: string; value: string | null | undefined }) {
  const text = stated(value);
  if (text === undefined) return null;
  return (
    <section className="flex flex-col gap-1">
      <h3 className="text-[length:var(--text-caption)] text-[var(--color-muted)]">{label}</h3>
      <p className="whitespace-pre-wrap text-[length:var(--text-body-sm)] text-[var(--color-ink)]">
        {text}
      </p>
    </section>
  );
}

/**
 * Industrial-complex summary.
 *
 * Order answers what a user who just clicked a boundary asks, in the order they ask it: what is
 * this and where (name, code, address), what state is it in (조성 단계, 분양, 진행률, 면적), when
 * did that happen (지정·착공·준공·사업기간), who runs it (관리기관·시행자), and finally the
 * reference prose (근거법·개발방식·조성목적·유치업종), which is the longest and the least likely
 * to be the reason for the click.
 *
 * A column the source did not state has **no row at all**, rather than a row reading `—`: a dash
 * asserts that the field was considered and found empty, which is a claim the data does not make.
 * Two exceptions, both because absence and a value mean different things here:
 * - `development_progress_percent` renders whenever it is present, including `"0.00"` — site works
 *   that have not started is an answer, not a blank.
 * - `status: "unknown"` renders its own label. Per the Catalog contract, `unknown` means the source
 *   stated a lifecycle this contract does not recognize, while absent means it stated none; folding
 *   the first into the second would discard the fact that the source said *something*.
 */
export function ComplexSummaryCard({
  entry,
  data,
}: {
  entry: Extract<PanelStackEntry, { kind: "complex" }>;
  data: ComplexInfo;
}) {
  const t = useTranslations("panels.complex.summary");

  const kind = stated(data.kind);
  const kindLabel =
    kind !== undefined && isKnown(COMPLEX_KINDS, kind) ? t(`kindValue.${kind}`) : kind;

  const status = stated(data.status);
  const statusLabel =
    status !== undefined && isKnown(COMPLEX_STATUSES, status) ? t(`statusValue.${status}`) : status;

  const progress = stated(data.development_progress_percent);
  const lotSales = stated(data.lot_sales_status);
  const lotSalesLabel =
    lotSales !== undefined && isKnown(LOT_SALES_STATUSES, lotSales)
      ? t(`lotSalesValue.${lotSales}`)
      : lotSales;

  return (
    <div className="flex flex-col gap-4 p-6">
      <header className="flex flex-col gap-2">
        <div className="font-mono text-[length:var(--text-caption)] text-[var(--color-muted)]">
          {t("officialCode", { code: data.official_complex_code })}
        </div>
        <h2 className="text-[length:var(--text-title-lg)] font-semibold text-[var(--color-ink)]">
          {data.name}
        </h2>
        {kindLabel !== undefined && (
          <div>
            <Badge>{kindLabel}</Badge>
          </div>
        )}
      </header>

      <dl className="grid grid-cols-[auto_1fr] gap-x-4 gap-y-2 text-[length:var(--text-body-sm)]">
        <TextRow label={t("address")} value={data.address_text} />
        {statusLabel !== undefined && <Row label={t("status")}>{statusLabel}</Row>}
        {lotSalesLabel !== undefined && <Row label={t("lotSales")}>{lotSalesLabel}</Row>}
        {/* `stated`, not a truthiness check: `"0.00"` is a real answer and must survive. */}
        {progress !== undefined && (
          <Row label={t("progress")}>{t("progressPercent", { value: progress })}</Row>
        )}
        <Row label={t("area")}>{t("areaM2", { value: data.area_m2.toLocaleString("ko-KR") })}</Row>
      </dl>

      <FactGroup
        rows={[
          { label: t("designatedDate"), value: data.designated_date },
          { label: t("constructionStartDate"), value: data.construction_start_date },
          { label: t("completionDate"), value: data.completion_date },
          { label: t("businessPeriod"), value: data.business_period_raw },
        ]}
      />

      <FactGroup
        rows={[
          { label: t("managementAgency"), value: data.management_agency_name },
          { label: t("developer"), value: data.developer_name },
        ]}
      />

      <ProseBlock label={t("designationBasisLaw")} value={data.designation_basis_law_raw} />
      <ProseBlock label={t("developmentMethod")} value={data.development_method_raw} />
      <ProseBlock label={t("developmentPurpose")} value={data.development_purpose_raw} />
      <ProseBlock label={t("invitedIndustries")} value={data.invited_industries_raw} />

      <footer className="font-mono text-[length:var(--text-caption)] text-[var(--color-muted)]">
        {entry.id}
      </footer>
    </div>
  );
}
