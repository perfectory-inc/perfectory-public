// apps/web/components/panels/complex/document-facts.tsx
"use client";
import { Badge } from "@gongzzang/ui";
import { useTranslations } from "next-intl";
import type { ComplexDocumentFacts } from "@/lib/api/complexes";
import { parcelMediaStore } from "@/lib/api/public-auction";
import { mediaFileSrc } from "@/lib/media/photo-ladder";

type Kind = NonNullable<ComplexDocumentFacts["kinds"]["land_use_area"]>;
type Source = Kind["source"];

/**
 * Where the value was read: the notice and the page, opening the document at that page.
 *
 * Not a data-source credit (root ADR-0428 keeps those off the screens): it is the value's own
 * evidence, the way an appraisal report opens from the auction panel. Only a PDF is served by the
 * edge (`parcel_media.path_pattern`), so an HWP notice names its page without a link; its page
 * number is that of the printed PDF the text was read from.
 */
function SourceLine({ source, pages }: { source: Source; pages: number[] }) {
  const t = useTranslations("panels.complex.summary.documentFacts");
  const first = pages[0];
  const pageText =
    first === undefined
      ? undefined
      : pages.length === 1 || first === pages[pages.length - 1]
        ? t("page", { page: first })
        : t("pages", { first, last: pages[pages.length - 1] as number });
  const label = [source.notice_number, source.notice_date, pageText]
    .filter((part) => part != null && part !== "")
    .join(" · ");
  return (
    <p className="text-[length:var(--text-caption)] text-[var(--color-muted)]">
      {source.media_format === "pdf" && first !== undefined ? (
        <a
          className="underline"
          href={`${mediaFileSrc(parcelMediaStore(), source)}#page=${first}`}
          target="_blank"
          rel="noopener noreferrer"
        >
          {t("original", { label })}
        </a>
      ) : (
        t("original", { label })
      )}
    </p>
  );
}

function pagesOf(kind: { facts: ReadonlyArray<{ page_number: number }> }): number[] {
  return [...new Set(kind.facts.map((fact) => fact.page_number))].sort((a, b) => a - b);
}

function Heading({ label }: { label: string }) {
  const t = useTranslations("panels.complex.summary.documentFacts");
  return (
    <h3 className="flex items-center gap-2 text-[length:var(--text-caption)] text-[var(--color-muted)]">
      {label}
      <Badge>{t("aiExtracted")}</Badge>
    </h3>
  );
}

/**
 * What the complex's notice attachments state, read by the local model platform and admitted by
 * foundation only for the fact kinds whose hand-checked precision passed its gate (root ADR-0477
 * §5). Each kind is one notice — the newest that states it — so two plans never show side by side.
 * Labelled "AI 추출" because no person typed these values: a reader should know to check the page.
 */
export function ComplexDocumentFactsSection({
  facts,
}: {
  facts: ComplexDocumentFacts | null | undefined;
}) {
  const t = useTranslations("panels.complex.summary.documentFacts");
  const landUse = facts?.kinds.land_use_area;
  const industries = facts?.kinds.invited_industry;
  if (!landUse?.facts.length && !industries?.facts.length) return null;
  return (
    <>
      {landUse && landUse.facts.length > 0 && (
        <section className="flex flex-col gap-2">
          <Heading label={t("landUse")} />
          <table className="w-full text-[length:var(--text-body-sm)] text-[var(--color-ink)]">
            <thead className="text-[var(--color-muted)]">
              <tr>
                <th className="text-left font-normal">{t("landUseLabel")}</th>
                <th className="text-right font-normal">{t("landUseArea")}</th>
                <th className="text-right font-normal">{t("landUseRatio")}</th>
              </tr>
            </thead>
            <tbody>
              {landUse.facts.map((fact, index) => (
                // biome-ignore lint/suspicious/noArrayIndexKey: the facts are the document's rows in order
                <tr key={index}>
                  <td>
                    {fact.value.parent
                      ? `${fact.value.parent} · ${fact.value.label}`
                      : fact.value.label}
                  </td>
                  <td className="text-right tabular-nums">
                    {t("areaM2", { value: fact.value.area_m2.toLocaleString("ko-KR") })}
                  </td>
                  <td className="text-right tabular-nums">
                    {fact.value.ratio_pct == null
                      ? ""
                      : t("ratio", { value: fact.value.ratio_pct })}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
          <SourceLine source={landUse.source} pages={pagesOf(landUse)} />
        </section>
      )}
      {industries && industries.facts.length > 0 && (
        <section className="flex flex-col gap-2">
          <Heading label={t("invitedIndustries")} />
          <ul className="flex flex-col gap-1 text-[length:var(--text-body-sm)] text-[var(--color-ink)]">
            {industries.facts.map((fact) => (
              <li key={fact.value.ksic_division}>
                {t("industry", { name: fact.value.ksic_name, code: fact.value.ksic_division })}
              </li>
            ))}
          </ul>
          <SourceLine source={industries.source} pages={pagesOf(industries)} />
        </section>
      )}
    </>
  );
}
